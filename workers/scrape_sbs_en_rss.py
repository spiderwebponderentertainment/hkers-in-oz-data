# workers/scrape_sbs_en_rss.py
import json, hashlib, requests, feedparser
from bs4 import BeautifulSoup
from datetime import datetime, timezone

HEADERS = {"User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"}
TIMEOUT = 20
MAX_ITEMS = 120
FEED_URL = "https://www.sbs.com.au/news/feed"
SOURCE_NAME = "SBS English"

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_date(raw: str | None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    try:
        # feedparser 已將 published_parsed 解析成 time.struct_time（若有）
        # 但為兼容性，先試 ISO 直轉；唔得再交比 parser
        if raw.endswith("Z"):
            return raw
        # 嘗試加 Z（簡單化處理）
        return datetime.fromisoformat(raw.replace("Z","")).isoformat() + "Z"
    except Exception:
        # 讓 feedparser 幫手 parse
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(raw)  # RFC822
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return None

def fetch_date_from_page(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # 1) og:article:published_time / article:published_time
        for key in ("article:published_time", "og:article:published_time"):
            meta = soup.find("meta", {"property": key})
            if meta and meta.get("content"):
                return normalize_date(meta["content"])

        # 2) JSON-LD
        for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t):
            try:
                data = json.loads(tag.string or tag.text or "")
            except Exception:
                continue
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if isinstance(obj, dict) and "datePublished" in obj:
                    return normalize_date(obj.get("datePublished"))
                if isinstance(obj, dict) and "@graph" in obj:
                    for g in obj["@graph"]:
                        if isinstance(g, dict) and "datePublished" in g:
                            return normalize_date(g.get("datePublished"))
    except Exception:
        pass
    return None

def fetch_items() -> list[dict]:
    feed = feedparser.parse(FEED_URL)
    items = []
    for entry in feed.entries[:MAX_ITEMS]:
        link = getattr(entry, "link", "").strip()
        title = getattr(entry, "title", "").strip()
        summary = getattr(entry, "summary", None)
        published = getattr(entry, "published", None)

        if not published:
            published = fetch_date_from_page(link)
        else:
            published = normalize_date(published) or fetch_date_from_page(link)

        item = {
            "id": hashlib.md5(link.encode()).hexdigest() if link else hashlib.md5(title.encode()).hexdigest(),
            "title": title or link,
            "link": link,
            "summary": summary,
            "publishedAt": published,
            "source": SOURCE_NAME,
            "fetchedAt": iso_now(),
        }
        items.append(item)
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
    json_out(items, "sbs_en.json")
    print(f"[DONE] SBS EN items: {len(items)}")
