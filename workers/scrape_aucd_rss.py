# workers/scrape_aucd_rss.py
import json, time, hashlib, requests, feedparser, re
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

HEADERS = {
    "User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)",
    "Accept-Language": "zh-HK,zh-TW;q=0.9,zh;q=0.8,en;q=0.5"
}
TIMEOUT = 20
SLEEP = 0.4

SITE = "https://aucd.com.au"
SOURCE_NAME = "澳洲新報"
MAX_PAGES = 8           # RSS 翻頁嘗試上限
MAX_ITEMS = 300         # 總輸出上限

# —— 新增：分類連結 & 每類要爬幾頁 ——
CATEGORY_URLS = [
    "https://aucd.com.au/category/australian-news/",
    "https://aucd.com.au/category/chinese-news/",
    "https://aucd.com.au/category/world-news/",
    "https://aucd.com.au/category/chinese-community-news/",
    "https://aucd.com.au/category/financial-news/",
    "https://aucd.com.au/category/property-news/",
    "https://aucd.com.au/category/entertainment-news/",
    "https://aucd.com.au/category/sport/",
    "https://aucd.com.au/category/food-and-beverage/",
    "https://aucd.com.au/category/health/",
    "https://aucd.com.au/category/holidays/",
    "https://aucd.com.au/category/education/",
    "https://aucd.com.au/category/pets/",
    "https://aucd.com.au/category/immigration/",
    "https://aucd.com.au/category/forum/",
    "https://aucd.com.au/category/culture/",
    "https://aucd.com.au/category/motoring/",
    "https://aucd.com.au/category/technology/",
    "https://aucd.com.au/category/seniors/",
]
CATEGORY_PAGES = 5

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_date(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    if s.endswith("Z"):
        return s
    try:
        dt = datetime.fromisoformat(s.replace("Z",""))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
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

def extract_title_desc_from_page(url: str) -> tuple[str, str | None]:
    html = fetch_html(url)
    if not html:
        return url, None
    try:
        soup = BeautifulSoup(html, "html.parser")
        # Title
        title = None
        ogt = soup.find("meta", property="og:title")
        if ogt and ogt.get("content"):
            title = ogt["content"].strip()
        if not title and soup.title and soup.title.string:
            title = soup.title.string.strip()
        title = title or url
        # Description
        desc = None
        ogd = soup.find("meta", property="og:description")
        if ogd and ogd.get("content"):
            desc = ogd["content"].strip()
        if not desc:
            md = soup.find("meta", attrs={"name": "description"})
            if md and md.get("content"):
                desc = md["content"].strip()
        return title, desc
    except Exception:
        return url, None

def candidate_feed_urls(max_pages: int) -> list[str]:
    """WordPress 常見翻頁樣式：/feed/?paged=2、/page/2/feed/、/page/2/?feed=rss2"""
    urls = []
    urls.append(f"{SITE}/feed/")
    for p in range(2, max_pages + 1):
        urls.append(f"{SITE}/feed/?paged={p}")
        urls.append(f"{SITE}/page/{p}/feed/")
        urls.append(f"{SITE}/page/{p}/?feed=rss2")
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

# —— 分類頁抽鏈：WordPress 常見文章網址是 /YYYY/MM/.../ —— #
DATE_PATH_RE = re.compile(r"/\d{4}/\d{2}/")

def looks_like_article_url(u: str) -> bool:
    try:
        p = urlparse(u)
        if not (p.scheme in ("http","https") and p.netloc and p.netloc.endswith("aucd.com.au")):
            return False
        path = p.path or ""
        # 排除非文章
        if any(seg in path for seg in ("/category/", "/tag/", "/page/", "/feed/", "/wp-json/")):
            return False
        # 常見文章路徑有年份月份
        if DATE_PATH_RE.search(path):
            return True
        # 兼容可能的 /?p=12345
        if (p.query or "").find("p=") >= 0:
            return True
        return False
    except Exception:
        return False

def extract_article_links_from_category_page(url: str) -> list[str]:
    html = fetch_html(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    seen, out = set(), []
    # 直接抓所有 <a> 再過濾
    for a in soup.find_all("a", href=True):
        u = a["href"].strip()
        if not u:
            continue
        if u.startswith("/"):
            u = urljoin(SITE, u)
        if u in seen:
            continue
        if looks_like_article_url(u):
            seen.add(u); out.append(u)
    return out

def crawl_categories(category_urls: list[str], pages_each: int) -> list[str]:
    found = []
    for base in category_urls:
        # 第 1 頁就係 base，本身已是末尾帶 /
        pages = [base.rstrip("/")+ "/"] + [
            f"{base.rstrip('/')}/page/{i}/" for i in range(2, pages_each + 1)
        ]
        for pg in pages:
            links = extract_article_links_from_category_page(pg)
            found.extend(links)
            time.sleep(SLEEP)
    # 去重保持順序
    seen = set(); uniq = []
    for u in found:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

def make_item_from_article(url: str) -> dict | None:
    title, desc = extract_title_desc_from_page(url)
    pub = fetch_date_from_page(url)
    return {
        "id": hashlib.md5((url or title).encode()).hexdigest(),
        "title": title or url,
        "link": url,
        "summary": desc,
        "publishedAt": pub,
        "source": SOURCE_NAME,
        "fetchedAt": iso_now(),
    }

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
    bag = []

    # A) RSS 多頁
    for u in candidate_feed_urls(MAX_PAGES):
        try:
            bag.extend(parse_feed(u))
        except Exception as e:
            print(f"[WARN] parse fail: {u}: {e}")
        time.sleep(SLEEP)

    # B) 分類頁逐頁抽連 + 入文補資料
    cat_links = crawl_categories(CATEGORY_URLS, CATEGORY_PAGES)
    for link in cat_links:
        try:
            item = make_item_from_article(link)
            bag.append(item)
        except Exception as e:
            print(f"[WARN] article parse fail: {link}: {e}")
        time.sleep(SLEEP)

    # C) 合併去重 & 截頂
    merged = merge_dedupe(bag)[:MAX_ITEMS]
    json_out(merged, "aucd.json")
    print(f"[DONE] AUCD items: {len(merged)}")
