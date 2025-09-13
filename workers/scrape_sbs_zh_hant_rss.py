# workers/scrape_sbs_zh_hant_rss.py
import json, hashlib, requests, feedparser
from bs4 import BeautifulSoup
from datetime import datetime, timezone

HEADERS = {"User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"}
TIMEOUT = 20
MAX_ITEMS = 120
# 逐個試：繁中 → 中文總頁 → 語言中文頁（不同路徑都可能有 feed）
FEED_URLS = [
    "https://www.sbs.com.au/language/chinese/zh-hant/feed",
    "https://www.sbs.com.au/language/chinese/feed",
]
SOURCE_NAME = "SBS 中文（繁體）"

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_date(raw: str | None) -> str | None:
    """將 RSS/HTML 抓到嘅日期字串盡量轉做 ISO8601（UTC）"""
    if not raw:
        return None
    raw = raw.strip()
    # 先嘗試 ISO8601
    try:
        # fromisoformat 唔識 'Z'，換做 +00:00
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    # 再嘗試 RFC822 等
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)  # e.g. "Fri, 13 Sep 2024 10:00:00 GMT"
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def fetch_date_from_page(url: str) -> str | None:
    """入內容頁再補發佈時間（同 EN 版一致：meta + JSON-LD）"""
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # 1) meta: article:published_time / og:article:published_time
        for key in ("article:published_time", "og:article:published_time"):
            m = soup.find("meta", {"property": key})
            if m and m.get("content"):
                d = normalize_date(m["content"])
                if d:
                    return d

        # 2) JSON-LD（支援單個或 @graph）
        for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t):
            try:
                data = json.loads(tag.string or tag.text or "")
            except Exception:
                continue
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if isinstance(obj, dict) and "datePublished" in obj:
                    d = normalize_date(obj.get("datePublished"))
                    if d:
                        return d
                if isinstance(obj, dict) and "@graph" in obj:
                    for g in obj["@graph"]:
                        if isinstance(g, dict) and "datePublished" in g:
                            d = normalize_date(g.get("datePublished"))
                            if d:
                                return d
    except Exception:
        pass
    return None

def first(*vals):
    for v in vals:
        if v:
            s = str(v).strip()
            if s:
                return s
    return None

def fetch_items() -> list[dict]:
    items = []
    seen_links = set()

    feed = None
    for url in FEED_URLS:
        f = feedparser.parse(url)
        if getattr(f, "entries", None):
            feed = f
            break
    if not feed or not feed.entries:
        # 沒有任何中文 feed 可用，回空（保留架構簡潔）
        return []

    for entry in feed.entries[:MAX_ITEMS]:
        link = first(getattr(entry, "link", None))
        title = first(getattr(entry, "title", None))
        summary = getattr(entry, "summary", None)

        # published 可能叫 published / updated
        published_raw = first(getattr(entry, "published", None),
                              getattr(entry, "updated", None))
        published = normalize_date(published_raw) if published_raw else None
        if not published and link:
            published = fetch_date_from_page(link)

        # 去重（以 link 為主）
        key = link or title or ""
        if key in seen_links:
            continue
        seen_links.add(key)

        items.append({
            "id": hashlib.md5((link or title or str(len(items))).encode()).hexdigest(),
            "title": title or (link or ""),
            "link": link or "",
            "summary": summary,
            "publishedAt": published,
            "source": SOURCE_NAME,
            "fetchedAt": iso_now(),
        })
    return items

def json_out(items: list[dict], path: str):
    payload = {
        "source": SOURCE_NAME,
        "generatedAt": iso_now(),
        "count": len(items),
        "items": items
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    items = fetch_items()
    json_out(items, "sbs_zh_hant.json")
    print(f"[DONE] SBS zh-Hant items: {len(items)}")
