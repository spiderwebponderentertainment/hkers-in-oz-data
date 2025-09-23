# workers/scrape_2cr.py
import json, hashlib, time, re
import datetime as dt
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
import feedparser
from bs4 import BeautifulSoup
try:
    # 伺服器端統一：簡→繁（香港用字）
    from opencc import OpenCC
    _CC_S2HK = OpenCC("s2hk")
    def to_trad(s: str | None) -> str | None:
        return _CC_S2HK.convert(s) if s else s
except Exception:
    # 無 opencc 時安全降級：直接返回原文
    def to_trad(s: str | None) -> str | None:
        return s

HEADERS = {"User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"}
TIMEOUT = 20
SLEEP = 0.3
MAX_ITEMS = 200
SYD = ZoneInfo("Australia/Sydney")

# 2CR（WordPress 常見 feed 路徑，可同時試幾個）
FEED_CANDIDATES = [
    "https://www.2cr.com.au/feed/",
    "https://www.2cr.com.au/category/news/feed/",
    "https://2cr.com.au/feed/",
    "https://www.2cr.com.au/category/%E8%B3%87%E8%A8%8A/%E6%9C%80%E6%96%B0%E6%B6%88%E6%81%AF/feed/",
    "https://www.2cr.com.au/category/%E8%B3%87%E8%A8%8A/%E7%A4%BE%E5%9C%98%E6%B6%88%E6%81%AF/feed/",
    "https://www.2cr.com.au/category/%E8%B3%87%E8%A8%8A/%E7%B6%9C%E5%90%88%E6%96%B0%E8%81%9E/feed/",
]

# WordPress 通常支援 ?paged=2、3…；有啲唔支援就會回 404 或返同一頁
MAX_PAGES = 12

OUT_PATH = "twocr.json"
SOURCE_NAME = "2CR 澳華之聲"

def to_iso(d: dt.datetime) -> str:
    s = d.isoformat()
    return s.replace("+00:00", "Z") if d.utcoffset() == dt.timedelta(0) else s

def ensure_utc(d: dt.datetime) -> dt.datetime:
    if d.tzinfo is None:
        return d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc)

def as_sydney(d_utc: dt.datetime) -> dt.datetime:
    return d_utc.astimezone(SYD)

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def now_iso_utc() -> str:
    return to_iso(now_utc())

def iso_now() -> str:
    # 後向相容
    return now_iso_utc()

def normalize_date(raw: str | None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    # 1) 先用 email/utils 解析（RSS 常見 RFC822）
    try:
        from email.utils import parsedate_to_datetime
        d = parsedate_to_datetime(raw)
        if not d:
             return None
        if not d.tzinfo:
              d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc).isoformat()
    except Exception:
        pass
    # 2) 再試 ISO8601
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"):
        try:
            return dt.datetime.strptime(raw, fmt).astimezone(dt.timezone.utc).isoformat()
        except Exception:
            pass
    return None

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r

def fetch_date_from_page(url: str) -> str | None:
    # 去文章頁補日期（article:published_time / JSON-LD）
    try:
        r = fetch(url)
        soup = BeautifulSoup(r.text, "html.parser")

        # meta
        for key in ("article:published_time", "og:article:published_time"):
            m = soup.find("meta", {"property": key})
            if m and m.get("content"):
                d = normalize_date(m["content"])
                if d: return d

        # time tag
        t = soup.find("time")
        if t and (t.get("datetime") or t.text.strip()):
            d = normalize_date(t.get("datetime") or t.text.strip())
            if d: return d

        # JSON-LD
        for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t):
            try:
                import json as _json
                data = _json.loads(tag.string or tag.text or "")
            except Exception:
                continue
            cands = data if isinstance(data, list) else [data]
            for obj in cands:
                if isinstance(obj, dict):
                    if "datePublished" in obj:
                        d = normalize_date(obj["datePublished"]); 
                        if d: return d
                    if "@graph" in obj and isinstance(obj["@graph"], list):
                        for g in obj["@graph"]:
                            if isinstance(g, dict) and "datePublished" in g:
                                d = normalize_date(g["datePublished"])
                                if d: return d
    except Exception:
        pass
    return None

def parse_one_feed(url: str) -> list[dict]:
    items: list[dict] = []
    for page in range(1, MAX_PAGES + 1):
        page_url = url if page == 1 else (url + ("&" if "?" in url else "?") + f"paged={page}")
        feed = feedparser.parse(page_url)
        if not getattr(feed, "entries", None):
            # 無條目／不支援分頁就停
            if page == 1:
                # 第一頁都無，可能此 feed 無效；換下一個 candidate
                return []
            break

        for entry in feed.entries:
            link = (getattr(entry, "link", "") or "").strip()
            title = (getattr(entry, "title", "") or "").strip()
            summary = getattr(entry, "summary", None)
            published = getattr(entry, "published", None) or getattr(entry, "updated", None)

            # 分類／tags（如有）
            source_categories = []
            try:
                if getattr(entry, "tags", None):
                    for t in entry.tags:
                        term = getattr(t, "term", None)
                        if term:
                            term = str(term).strip()
                            if term and term not in source_categories:
                                source_categories.append(term)
            except Exception:
                pass
            if not source_categories:
                cat = getattr(entry, "category", None)
                if cat:
                    source_categories = [str(cat).strip()]
            source_category = source_categories[0] if source_categories else None
            
            # 日期：RSS 有就用；沒有就入內頁補
            pub = normalize_date(published) if published else None
            if not pub and link:
                pub = fetch_date_from_page(link)

            # UTC / Local 雙欄位
            fetched_utc = now_utc()
            if pub:
                try:
                    published_utc = dt.datetime.fromisoformat(pub.replace("Z", "+00:00"))
                except Exception:
                    published_utc = fetched_utc
            else:
                published_utc = fetched_utc
            published_utc = ensure_utc(published_utc)

            # 去重 key
            _id = hashlib.md5((link or title).encode()).hexdigest()
            item = {
                "id": _id,
                # 後端轉繁（香港）
                "title": to_trad(title) or link,
                "link": link,
                "summary": to_trad(summary),
                "publishedAt": to_iso(published_utc),
                "fetchedAt": to_iso(fetched_utc),
                "publishedAtLocal": to_iso(as_sydney(published_utc)),
                "fetchedAtLocal": to_iso(as_sydney(fetched_utc)),
                "localTimezone": "Australia/Sydney",
                "source": SOURCE_NAME,
                "sourceCategory": source_category,
                "sourceCategories": source_categories or None,
                "sourceSectionPath": None,
            }
            items.append(item)

        time.sleep(SLEEP)
        if len(items) >= MAX_ITEMS:
            break

    return items

def fetch_all() -> list[dict]:
    collected: list[dict] = []
    seen = set()
    for base in FEED_CANDIDATES:
        got = parse_one_feed(base)
        for it in got:
            key = it["link"] or it["id"]
            if key in seen:
                continue
            seen.add(key)
            collected.append(it)
        if len(collected) >= MAX_ITEMS:
            break
    # 以日期 desc 排
    def key_dt(it):
        s = it.get("publishedAt")
        try:
            return dt.datetime.fromisoformat(s.replace("Z","+00:00")) if s else dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        except Exception:
            return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    collected.sort(key=key_dt, reverse=True)
    return collected[:MAX_ITEMS]

def json_out(items: list[dict], path: str):
    payload = {
        "source": SOURCE_NAME,
        "generatedAt": now_iso_utc(),
        "generatedAtLocal": to_iso(as_sydney(now_utc())),
        "localTimezone": "Australia/Sydney",
        "count": len(items),
        "items": items
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    items = fetch_all()
    json_out(items, OUT_PATH)
    print(f"[DONE] 2CR items: {len(items)}")
