# workers/scrape_7news_en.py
import json, re, sys, html, hashlib, time
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin, parse_qs, unquote
from collections import deque

import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

# ---------------- 基本設定 ----------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://7news.com.au/"
}

TIMEOUT = 25
MAX_ITEMS = 200
FETCH_SLEEP = 0.5

SEVEN_HOST = "7news.com.au"
ROBOTS_URL = "https://7news.com.au/robots.txt"
SYD = ZoneInfo("Australia/Sydney")

# 🔥 [新功能] 標題黑名單 (Title Blacklist)
# 凡是標題包含這些字眼 (不分大細寫)，一律丟棄
BLOCKED_TITLES = (
    "About Us",
    "Download The 7NEWS app today",
    "Editorial Policy",
    "Sponsored Content",
    "Newsletters",
    "Contact Us",
    "Terms & Conditions",
    "Privacy Policy",
    "Code of Conduct",
    "Advertise with us",
    "FAQs",
    "7plus"
)

# 入口
ENTRY_BASES = [
    "https://7news.com.au/",
    "https://7news.com.au/news",
    "https://7news.com.au/politics",
    "https://7news.com.au/world",
    "https://7news.com.au/business",
    "https://7news.com.au/sport",
    "https://7news.com.au/lifestyle",
    "https://7news.com.au/technology",
]

SECTION_ALLOWED_PREFIXES = ("https://7news.com.au/",)

# Google News 作補位
GN_URL = (
    "https://news.google.com/rss/search"
    "?q=site:7news.com.au"
    "&hl=en-AU&gl=AU&ceid=AU:en"
)

# ---------------- 小工具 ----------------
def iso_now(): return datetime.now(timezone.utc).isoformat()
def clean(s: str) -> str: return re.sub(r"\s+", " ", (s or "")).strip()

def normalize_date(raw: str | None) -> str | None:
    if not raw: return None
    s = raw.strip()
    if not s: return None
    if s.endswith("Z"): return s
    try:
        dt = datetime.fromisoformat(s.replace("Z",""))
        if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(s)
        if not dt: return None
        if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r

def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def as_sydney(dt_utc: datetime) -> datetime:
    return ensure_utc(dt_utc).astimezone(SYD)

def parse_iso_dt(s: str | None) -> datetime | None:
    if not s: return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except: return None
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def canonicalize_link(url: str, html_text: str | None = None) -> str:
    if html_text:
        try:
            soup = BeautifulSoup(html_text, "html.parser")
            link_tag = soup.find("link", rel=lambda x: x and "canonical" in x)
            if link_tag and link_tag.get("href"):
                c_url = link_tag["href"].strip()
                if c_url.startswith("//"): c_url = "https:" + c_url
                if SEVEN_HOST in c_url:
                    url = c_url
        except: pass
    
    if url.startswith("//"): url = "https:" + url
    try:
        p = urlparse(url)
        scheme = "https"
        netloc = p.netloc.lower()
        path = p.path.rstrip("/")
        if not path: path = "/"
        return f"{scheme}://{netloc}{path}"
    except:
        return url

# 🔥 URL 黑名單 (已加入 /world/world 防止 404 warning)
NON_ARTICLE_SEGMENTS = (
    "/video/", "/watch/", "/weather/", "/privacy", "/terms", "/contact", 
    "/coupons/", "/competitions/", "/sunrise/", "/the-morning-show/", 
    "/spotlight/", "/sitemap", "/tag/", "/category/", "/live/", 
    "/profile/", "/login", "/register", "/search", "/authors/",
    "support.7news", "/7you", "/fixtures", "/world/world"
)
MEDIA_EXTS = (".mp3",".mp4",".m4a",".jpg",".jpeg",".png",".gif",".pdf",".webp",".svg",".webm",".m3u8",".js",".css",".json")

def looks_like_article_url(u: str) -> bool:
    try:
        if not u: return False
        u_lower = u.lower()
        if any(u_lower.endswith(ext) for ext in MEDIA_EXTS): return False
        if any(seg in u_lower for seg in NON_ARTICLE_SEGMENTS): return False
        
        p = urlparse(u)
        if p.scheme not in ("http","https"): return False
        if "support." in (p.netloc or ""): return False
        if SEVEN_HOST not in (p.netloc or ""): return False
        
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        if len(parts) < 1: return False
        
        return True
    except:
        return False

# ---------------- Category 判斷 ----------------
def _slug_title(slug: str) -> str:
    m = {
        "news": "News",
        "politics": "Politics",
        "world": "World",
        "business": "Business",
        "sport": "Sport",
        "entertainment": "Entertainment",
        "lifestyle": "Lifestyle",
        "technology": "Technology",
        "travel": "Travel",
    }
    return m.get(slug.lower(), slug.capitalize())

def category_from_url(u: str) -> str | None:
    try:
        p = urlparse(u)
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        if parts:
            return _slug_title(parts[0])
    except: pass
    return None

# ---------------- JSON-LD / meta 解析 ----------------
def parse_json_ld(html_text: str):
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t):
            txt = tag.string or tag.get_text() or ""
            try: data = json.loads(txt)
            except: continue

            def select(obj: dict) -> dict | None:
                if not isinstance(obj, dict): return None
                t = obj.get("@type")
                if isinstance(t, list): t = next((x for x in t if isinstance(x, str)), None)
                if t not in ("NewsArticle", "Article", "BlogPosting", "ReportageNewsArticle"): return None
                
                date = (obj.get("datePublished") or obj.get("uploadDate") or obj.get("dateCreated") or obj.get("dateModified") or "")
                section = obj.get("articleSection")
                if isinstance(section, list): section = next((x for x in section if isinstance(x, str)), None)
                
                return {
                    "headline": obj.get("headline") or obj.get("name") or "",
                    "description": obj.get("description") or "",
                    "datePublished": date,
                    "url": obj.get("url") or "",
                    "articleSection": section or "",
                }

            def scan(o):
                if isinstance(o, dict):
                    if "@graph" in o and isinstance(o["@graph"], list):
                        for g in o["@graph"]:
                            got = select(g)
                            if got: return got
                    got = select(o)
                    if got: return got
                if isinstance(o, list):
                    for each in o:
                        got = scan(each)
                        if got: return got
                return None

            candidate = scan(data)
            if candidate: return candidate
    except: pass
    return {}

def extract_meta_from_html(html_text: str):
    soup = BeautifulSoup(html_text, "html.parser")
    title = (soup.find("meta", property="og:title") or {}).get("content") or (soup.title.string if soup.title else "") or ""
    desc = (soup.find("meta", property="og:description") or {}).get("content") or (soup.find("meta", attrs={"name": "description"}) or {}).get("content") or ""
    pub = (
        (soup.find("meta", property="article:published_time") or {}).get("content")
        or (soup.find("meta", property="og:article:published_time") or {}).get("content")
        or (soup.find("meta", property="og:published_time") or {}).get("content")
        or (soup.find("meta", attrs={"itemprop": "datePublished"}) or {}).get("content")
        or (soup.find("time", attrs={"datetime": True}) or {}).get("datetime")
        or (soup.find("meta", attrs={"name": "date"}) or {}).get("content")
        or None
    )
    section = ((soup.find("meta", property="article:section") or {}).get("content") or (soup.find("meta", attrs={"name": "section"}) or {}).get("content") or None)
    return clean(title), clean(desc), pub, section

def make_item(url: str, html_text: str, hint_section: str | None = None):
    link_final = canonicalize_link(url, html_text)
    section = category_from_url(link_final) or hint_section
    ld = parse_json_ld(html_text)
    
    if ld:
        title = clean(ld.get("headline", "")) or None
        desc = clean(ld.get("description", "")) or ""
        pub = normalize_date(ld.get("datePublished") or None)
        section = section or (ld.get("articleSection") or None)
        if not title:
            t2, d2, p2, s2 = extract_meta_from_html(html_text)
            title = t2; desc = desc or d2; pub = pub or normalize_date(p2); section = section or s2
    else:
        t2, d2, p2, s2 = extract_meta_from_html(html_text)
        title = t2; desc = d2; pub = normalize_date(p2); section = section or s2

    now_utc = ensure_utc(datetime.now(timezone.utc))
    pub_dt_utc = parse_iso_dt(pub) if pub else None
    
    return {
        "id": hashlib.md5(link_final.encode()).hexdigest(),
        "title": title or link_final,
        "link": link_final,
        "summary": desc,
        "publishedAt": pub,
        "source": "7NEWS",
        "fetchedAt": now_utc.isoformat(),
        "publishedAtLocal": (as_sydney(pub_dt_utc).isoformat() if pub_dt_utc else None),
        "fetchedAtLocal": as_sydney(now_utc).isoformat(),
        "localTimezone": "Australia/Sydney",
        "sourceCategory": section,
        "sourceCategories": [section] if section else None,
    }

# ---------------- Robots & Sitemap ----------------
SITEMAP_RE = re.compile(r"(?im)^\s*Sitemap:\s*(https?://\S+)\s*$")

def sitemaps_from_robots() -> list[str]:
    try: txt = fetch(ROBOTS_URL).text
    except: return []
    return SITEMAP_RE.findall(txt)

def parse_sitemap_urls(xml_text: str) -> list[str]:
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    try:
        root = ET.fromstring(xml_text)
        for loc in root.findall(".//sm:url/sm:loc", ns):
            if loc.text: urls.append(loc.text.strip())
        for loc in root.findall(".//sm:sitemap/sm:loc", ns):
            if loc.text: urls.append(loc.text.strip())
    except:
        urls = [m.group(1) for m in re.finditer(r"<loc>\s*(.*?)\s*</loc>", xml_text)]
    return urls

def collect_from_sitemaps() -> list[str]:
    out = []
    for sm in sitemaps_from_robots():
        if not sm.lower().endswith(".xml"): continue
        try:
            xml = fetch(sm).text
            for u in parse_sitemap_urls(xml):
                if "7news.com.au" in u and looks_like_article_url(u):
                    out.append(u)
        except: continue
        if len(out) >= 12 * MAX_ITEMS: break
    seen = set(); uniq = []
    for u in out:
        u_clean = u.rstrip("/")
        if u_clean not in seen:
            seen.add(u_clean); uniq.append(u)
    return uniq

# ---------------- 入口頁 Link ----------------
def sanitize_7news(u: str, base: str) -> str | None:
    if not u: return None
    u = html.unescape(u).strip()
    if u.startswith("//"): u = "https:" + u
    if u.startswith("/"): u = urljoin(base, u)
    u = u.split("#",1)[0].split("?",1)[0]
    u = u.rstrip("/")
    if looks_like_article_url(u): return u
    return None

def links_from_html_anywhere(html_text: str, base: str) -> list[str]:
    links, seen = [], set()
    soup = BeautifulSoup(html_text, "html.parser")
    for a in soup.find_all("a", href=True):
        u = sanitize_7news(a["href"], base)
        if u and u not in seen:
            seen.add(u); links.append(u)
    return links

def collect_from_entrypages() -> dict[str, str | None]:
    out = {}
    for base in ENTRY_BASES:
        hint = category_from_url(base)
        try:
            html_text = fetch(base).text
            for u in links_from_html_anywhere(html_text, base=base):
                if u not in out: out[u] = hint
        except: pass
        time.sleep(0.2)
    return out

# ---------------- 爬蟲 ----------------
def crawl_site(seeds: list[str], max_pages: int = 30) -> list[str]:
    q = deque(); seen_pages = set(); found_articles = set()
    for s in seeds:
        if looks_like_article_url(s) or s in ENTRY_BASES:
            q.append(s); seen_pages.add(s)
    pages_visited = 0
    while q and pages_visited < max_pages:
        url = q.popleft()
        try: html_text = fetch(url).text
        except: continue
        for art in links_from_html_anywhere(html_text, base=url):
            found_articles.add(art)
        soup = BeautifulSoup(html_text, "html.parser")
        for a in soup.find_all("a", href=True):
            u = sanitize_7news(a["href"], url)
            if u and u not in seen_pages:
                if u in ENTRY_BASES or (category_from_url(u) and len(urlparse(u).path.split('/')) < 4):
                    seen_pages.add(u); q.append(u)
        pages_visited += 1
        time.sleep(FETCH_SLEEP)
    return list(found_articles)

# ---------------- Google News ----------------
def extract_7news_from_text(text: str) -> str | None:
    if not text: return None
    text = html.unescape(text)
    for m in re.finditer(r'https?://[^\s\'">]+', text):
        u = m.group(0)
        if SEVEN_HOST in u: return u
    return None

def decode_gn_item(link_text: str, guid_text: str | None, desc_html: str | None) -> str | None:
    cand = None
    if link_text and SEVEN_HOST in link_text: cand = link_text.strip()
    elif guid_text and SEVEN_HOST in guid_text: cand = guid_text.strip()
    else:
        u = extract_7news_from_text(desc_html or "")
        if u: cand = u
        elif link_text and "news.google.com" in link_text:
            try:
                p = urlparse(link_text); qs = parse_qs(p.query)
                for key in ("u","url","q"):
                    if key in qs and qs[key]:
                        c = unquote(qs[key][0])
                        if SEVEN_HOST in c: cand = c
            except: pass
    if cand:
        cand = cand.split("?", 1)[0].rstrip("/")
        if looks_like_article_url(cand): return cand
    return None

def collect_from_google_news() -> list[str]:
    try: xml = fetch(GN_URL).text
    except: return []
    urls = []
    try:
        root = ET.fromstring(xml)
        for it in root.findall(".//item"):
            link_text = (it.findtext("link") or "").strip()
            guid_text = (it.findtext("guid") or "").strip()
            desc_html = it.findtext("description") or ""
            real = decode_gn_item(link_text, guid_text, desc_html)
            if real: urls.append(real)
    except: return []
    seen = set(); uniq = []
    for u in urls:
        if u not in seen: seen.add(u); uniq.append(u)
    return uniq

# ---------------- 輸出 ----------------
def json_out(items, path):
    now_utc = ensure_utc(datetime.now(timezone.utc))
    payload = {
        "source": "7NEWS",
        "generatedAt": now_utc.isoformat(),
        "generatedAtLocal": as_sydney(now_utc).isoformat(),
        "localTimezone": "Australia/Sydney",
        "count": len(items),
        "items": items
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def rss_out(items, path):
    try: from feedgen.feed import FeedGenerator
    except: return
    fg = FeedGenerator()
    fg.title("7NEWS – Aggregated (Unofficial)")
    fg.link(href="https://7news.com.au/", rel='alternate')
    fg.description("Auto-generated (headings & summaries only).")
    fg.language("en")
    for it in items:
        fe = fg.add_entry()
        fe.id(it["id"]); fe.title(it["title"]); fe.link(href=it["link"])
        fe.description(it.get("summary") or it["title"])
    fg.rss_file(path)

# ---------------- 主程式 ----------------
if __name__ == "__main__":
    urls_a = collect_from_sitemaps()
    print(f"[INFO] sitemap urls: {len(urls_a)}", file=sys.stderr)

    seed_pages = ENTRY_BASES[:]
    url_to_hint = collect_from_entrypages()
    urls_b = list(url_to_hint.keys())
    print(f"[INFO] entry page urls: {len(urls_b)}", file=sys.stderr)

    urls_crawl = crawl_site(seeds=seed_pages, max_pages=30)
    print(f"[INFO] crawl urls: {len(urls_crawl)}", file=sys.stderr)

    hint_map = dict(url_to_hint)
    seen, merged = set(), []
    
    for u in urls_b + urls_crawl + urls_a:
        u_clean = u.rstrip("/")
        if u_clean not in seen and looks_like_article_url(u_clean):
            seen.add(u_clean); merged.append(u_clean)

    LIMIT_PROCESS = 350
    if len(merged) > LIMIT_PROCESS:
        print(f"[INFO] Capping items to {LIMIT_PROCESS}...", file=sys.stderr)
        merged = merged[:LIMIT_PROCESS]

    articles = []
    seen_ids = set()
    
    print(f"[INFO] Fetching {len(merged)} items...", file=sys.stderr)
    for i, u in enumerate(merged):
        if i % 50 == 0: print(f"[INFO] Processing {i}/{len(merged)}...", file=sys.stderr)
        try:
            html_text = fetch(u).text
            hint = hint_map.get(u)
            item = make_item(u, html_text, hint_section=hint)
            
            if item["id"] in seen_ids: continue
            if not item["title"]: continue
            
            # 🔥 [標題過濾] 凡是標題含有黑名單字眼，即刻 Skip
            title_lower = item["title"].lower()
            if any(bad_word.lower() in title_lower for bad_word in BLOCKED_TITLES):
                # print(f"[INFO] Skipped blocked title: {item['title']}", file=sys.stderr)
                continue

            seen_ids.add(item["id"])
            articles.append(item)
            time.sleep(FETCH_SLEEP)
        except Exception as e:
            print(f"[WARN] fetch article fail {u}: {e}", file=sys.stderr)

    if len(articles) < MAX_ITEMS // 2:
        print("[INFO] few items; fallback Google News", file=sys.stderr)
        urls_gn = collect_from_google_news()
        gn_count = 0
        for u in urls_gn:
            if gn_count >= 50: break
            try:
                html_text = fetch(u).text
                item = make_item(u, html_text)
                
                if item["id"] in seen_ids: continue
                if not item["title"]: continue
                
                # 🔥 [標題過濾] 同樣適用於 Google News 補位
                title_lower = item["title"].lower()
                if any(bad_word.lower() in title_lower for bad_word in BLOCKED_TITLES):
                    continue

                seen_ids.add(item["id"])
                articles.append(item)
                gn_count += 1
                time.sleep(FETCH_SLEEP)
            except: pass

    def key_dt(it):
        s = it.get("publishedAt")
        if not s: return datetime.min.replace(tzinfo=timezone.utc)
        try: return datetime.fromisoformat(s.replace("Z","+00:00"))
        except: return datetime.min.replace(tzinfo=timezone.utc)

    articles.sort(key=key_dt, reverse=True)
    latest = articles[:MAX_ITEMS]
    
    json_out(latest, "seven_en.json")
    rss_out(latest, "seven_en.xml")
    print(f"[DONE] output {len(latest)} items", file=sys.stderr)
