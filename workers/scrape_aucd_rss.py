# workers/scrape_aucd_rss.py
import json, time, hashlib, requests, feedparser
from bs4 import BeautifulSoup
from datetime import datetime, timezone

HEADERS = {
    "User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)",
    "Accept-Language": "zh-HK,zh-TW;q=0.9,zh;q=0.8,en;q=0.5"
}
TIMEOUT = 20
SLEEP = 0.4

SITE = "https://aucd.com.au"
SOURCE_NAME = "澳洲新報"
MAX_PAGES = 8          # 👈 想再多就加大
MAX_ITEMS = 200        # 👈 總數上限（多頁合併之後再截）

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_date(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    # 已是 UTC Z
    if s.endswith("Z"):
        return s
    # ISO8601
    try:
        dt = datetime.fromisoformat(s.replace("Z",""))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    # RFC822/1123
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(s)
        if not dt:
            return None
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def fetch_html(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception:
        return None

def fetch_date_from_page(url: str) -> str | None:
    """冇 pubDate 嘅時候，入文內頁補日期（meta / JSON-LD / <time>）"""
    html = fetch_html(url)
    if not html:
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
        # meta 優先
        for key in ("article:published_time", "og:article:published_time", "og:published_time"):
            tag = soup.find("meta", {"property": key})
            if tag and tag.get("content"):
                return normalize_date(tag["content"])
        # itemprop
        tag = soup.find("meta", attrs={"itemprop": "datePublished"})
        if tag and tag.get("content"):
            return normalize_date(tag["content"])
        # <time datetime="...">
        t = soup.find("time", attrs={"datetime": True})
        if t and t.get("datetime"):
            return normalize_date(t["datetime"])
        # JSON-LD
        for s in soup.find_all("script", type=lambda t: t and "ld+json" in t):
            try:
                import json as _json
                data = _json.loads(s.string or s.text or "")
            except Exception:
                continue
            def scan(o):
                if isinstance(o, dict):
                    if "@graph" in o and isinstance(o["@graph"], list):
                        for g in o["@graph"]:
                            d = g.get("datePublished") or g.get("uploadDate") or g.get("dateCreated")
                            if d: return d
                    d = o.get("datePublished") or o.get("uploadDate") or o.get("dateCreated")
                    if d: return d
                if isinstance(o, list):
                    for each in o:
                        got = scan(each)
                        if got: return got
                return None
            d = scan(data)
            if d:
                return normalize_date(d)
    except Exception:
        return None
    return None

def candidate_feed_urls(max_pages: int) -> list[str]:
    """WordPress 常見翻頁樣式：/feed/?paged=2、/page/2/feed/、/page/2/?feed=rss2"""
    urls = []
    # 第一頁
    urls.append(f"{SITE}/feed/")
    # 之後幾頁
    for p in range(2, max_pages + 1):
        urls.append(f"{SITE}/feed/?paged={p}")
        urls.append(f"{SITE}/page/{p}/feed/")
        urls.append(f"{SITE}/page/{p}/?feed=rss2")
    # 去重，保持順序
    seen = set(); out = []
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def parse_feed(url: str) -> list[dict]:
    fp = feedparser.parse(url)
    items = []
    for e in fp.entries:
        link = getattr(e, "link", "").strip()
        title = getattr(e, "title", "").strip()
        # 優先 content:encoded / content[0].value，再退回 summary
        summary = None
        if getattr(e, "content", None):
            try:
                summary = e.content[0].value
            except Exception:
                summary = None
        if not summary:
            summary = getattr(e, "summary", None)

        published = getattr(e, "published", None) or getattr(e, "updated", None)
        pub_norm = normalize_date(published) if published else None
        if not pub_norm and link:
            pub_norm = fetch_date_from_page(link)

        item = {
            "id": hashlib.md5((link or title).encode()).hexdigest(),
            "title": title or link,
            "link": link,
            "summary": summary,
            "publishedAt": pub_norm,
            "source": SOURCE_NAME,
            "fetchedAt": iso_now(),
        }
        items.append(item)
    return items

def merge_dedupe(all_items: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for it in all_items:
        key = it.get("link") or it.get("id")
        if key in seen:
            continue
        seen.add(key); out.append(it)
    # 按日期 desc（冇日期放後）
    def key_dt(it):
        s = it.get("publishedAt")
        if not s:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)
    out.sort(key=key_dt, reverse=True)
    return out

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
    pages = candidate_feed_urls(MAX_PAGES)
    bag = []
    for u in pages:
        try:
            bag.extend(parse_feed(u))
        except Exception as e:
            print(f"[WARN] parse fail: {u}: {e}")
        time.sleep(SLEEP)

    merged = merge_dedupe(bag)[:MAX_ITEMS]
    json_out(merged, "aucd.json")
    print(f"[DONE] AUCD items: {len(merged)}")
