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
HEADERS = {"User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"}
TIMEOUT = 25
MAX_ITEMS = 200
FETCH_SLEEP = 0.4

SEVEN_HOST = "7news.com.au"
ROBOTS_URL = "https://7news.com.au/robots.txt"
SYD = ZoneInfo("Australia/Sydney")

# 入口（首頁 + 常見版塊；只用首頁做 seed，唔試分頁）
ENTRY_BASES = [
    "https://7news.com.au/",
    "https://7news.com.au/news",
    "https://7news.com.au/politics",
    "https://7news.com.au/world",
    "https://7news.com.au/business",
    "https://7news.com.au/sport",
    "https://7news.com.au/entertainment",
    "https://7news.com.au/lifestyle",
    "https://7news.com.au/technology",
    "https://7news.com.au/travel",
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

def canonicalize_link(url: str, html_text: str | None = None) -> str:
    # https scheme + strip trailing slash; prefer <link rel="canonical">
    if html_text:
        try:
            soup = BeautifulSoup(html_text, "html.parser")
            link_tag = soup.find("link", rel=lambda x: x and "canonical" in x)
            if link_tag and link_tag.get("href"):
                url = link_tag["href"].strip()
        except Exception:
            pass
    if url.startswith("//"): url = "https:" + url
    p = urlparse(url)
    scheme = "https"
    netloc = p.netloc.lower()
    path = p.path.rstrip("/") or "/"
    q = ""  # drop query for canonical
    frag = ""
    return f"{scheme}://{netloc}{path}"

def parse_iso_dt(s: str | None) -> datetime | None:
    """ISO8601（可有/無 Z）→ aware UTC datetime；錯就回 None。"""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

# 似係文章嘅 URL（粗略規則）：排除 sitemap/xml、明顯非內容頁、媒體檔
NON_ARTICLE_SEGMENTS = (
    "/sitemap/", "/tag/", "/category/", "/live/", "/weather/", "/privacy", "/terms",
)
MEDIA_EXTS = (".mp3",".mp4",".m4a",".jpg",".jpeg",".png",".gif",".pdf",".webp",".svg",".webm",".m3u8")

def looks_like_article_url(u: str) -> bool:
    try:
        if not u or u.endswith(".xml"):
            return False
        if any(seg in u for seg in NON_ARTICLE_SEGMENTS):
            return False
        p = urlparse(u)
        if p.scheme not in ("http","https"):
            return False
        if SEVEN_HOST not in (p.netloc or ""):
            return False
        if any(u.lower().endswith(ext) for ext in MEDIA_EXTS):
            return False
        # 7NEWS 文章通常係 /{section}/{slug} 結構；至少要有一段 section
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        return len(parts) >= 1
    except Exception:
        return False

# ---------------- Category 判斷（URL 優先） ----------------
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
    return m.get(slug, slug.capitalize())

def category_from_url(u: str) -> str | None:
    try:
        p = urlparse(u)
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        if parts:
            # 第一段大多是類別（/world/...、/politics/...）
            return _slug_title(parts[0])
    except Exception:
        pass
    return None

# ---------------- JSON-LD / meta 解析 ----------------
def parse_json_ld(html_text: str):
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t):
            txt = tag.string or tag.get_text() or ""
            try:
                data = json.loads(txt)
            except Exception:
                continue

            def select(obj: dict) -> dict | None:
                if not isinstance(obj, dict): return None
                t = obj.get("@type")
                if isinstance(t, list): t = next((x for x in t if isinstance(x, str)), None)
                if t not in ("NewsArticle", "Article", "BlogPosting"): return None
                date = (
                    obj.get("datePublished") or obj.get("uploadDate") or
                    obj.get("dateCreated") or obj.get("dateModified") or ""
                )
                section = obj.get("articleSection")
                if isinstance(section, list):
                    section = next((x for x in section if isinstance(x, str)), None)
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
    except Exception:
        pass
    return {}

def extract_meta_from_html(html_text: str):
    soup = BeautifulSoup(html_text, "html.parser")
    title = (soup.find("meta", property="og:title") or {}).get("content") \
        or (soup.title.string if soup.title else "") or ""
    desc = (soup.find("meta", property="og:description") or {}).get("content") \
        or (soup.find("meta", attrs={"name": "description"}) or {}).get("content") \
        or ""
    pub = (
        (soup.find("meta", property="article:published_time") or {}).get("content")
        or (soup.find("meta", property="og:article:published_time") or {}).get("content")
        or (soup.find("meta", property="og:published_time") or {}).get("content")
        or (soup.find("meta", attrs={"itemprop": "datePublished"}) or {}).get("content")
        or (soup.find("time", attrs={"datetime": True}) or {}).get("datetime")
        or (soup.find("meta", attrs={"name": "date"}) or {}).get("content")
        or None
    )
    section = (
        (soup.find("meta", property="article:section") or {}).get("content")
        or (soup.find("meta", attrs={"name": "section"}) or {}).get("content")
        or None
    )
    return clean(title), clean(desc), pub, section

def make_item(url: str, html_text: str, hint_section: str | None = None):
    section = category_from_url(url) or hint_section
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

    link_final = canonicalize_link(url, html_text)
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
        # 👇 悉尼本地時間（自動 AEST/AEDT）
        "publishedAtLocal": (as_sydney(pub_dt_utc).isoformat() if pub_dt_utc else None),
        "fetchedAtLocal": as_sydney(now_utc).isoformat(),
        "localTimezone": "Australia/Sydney",
         "sourceCategory": section,
        "sourceCategories": [section] if section else None,
    }

# ---------------- A) robots.txt ➜ sitemap ----------------
SITEMAP_RE = re.compile(r"(?im)^\s*Sitemap:\s*(https?://\S+)\s*$")

def sitemaps_from_robots() -> list[str]:
    try:
        txt = fetch(ROBOTS_URL).text
    except Exception as e:
        print(f"[WARN] robots fetch fail: {e}", file=sys.stderr); return []
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
    except ET.ParseError:
        urls = [m.group(1) for m in re.finditer(r"<loc>\s*(.*?)\s*</loc>", xml_text)]
    return urls

def collect_from_sitemaps() -> list[str]:
    out = []
    for sm in sitemaps_from_robots():
        if not sm.lower().endswith(".xml"): continue
        try:
            xml = fetch(sm).text
            for u in parse_sitemap_urls(xml):
                # 🚫 跳過 sitemap 本身或任何 .xml
                if u.endswith(".xml") or "/sitemap/" in u:
                    continue
                # ✅ 只收似係文章的 URL
                if "7news.com.au" in u and looks_like_article_url(u):
                    out.append(u)
        except Exception as e:
            print(f"[WARN] sitemap fail {sm}: {e}", file=sys.stderr)
        if len(out) >= 12 * MAX_ITEMS: break
    # 去重
    seen = set(); uniq = []
    for u in out:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ---------------- B) 入口頁抽 link ----------------
ARTICLE_HREF_RE = re.compile(r'https?://(?:www\.)?7news\.com\.au/[A-Za-z0-9\-/_.]+')
REL_ARTICLE_RE = re.compile(r'/[A-Za-z0-9\-/_.]+')

def sanitize_7news(u: str, base: str) -> str | None:
    if not u: return None
    u = html.unescape(u).strip()
    if u.startswith("//"): u = "https:" + u
    if u.startswith("/"): u = urljoin(base, u)
    # drop fragments
    u = u.split("#",1)[0]
    # basic filter: same host & not media
    p = urlparse(u)
    if p.scheme not in ("http","https"): return None
    if SEVEN_HOST not in p.netloc: return None
    if any(u.lower().endswith(ext) for ext in (".mp3",".mp4",".m4a",".jpg",".jpeg",".png",".gif",".pdf",".webp",".svg")):
        return None
    return u

def links_from_html_anywhere(html_text: str, base: str) -> list[str]:
    links, seen = [], set()
    soup = BeautifulSoup(html_text, "html.parser")
    for a in soup.find_all("a", href=True):
        u = sanitize_7news(a["href"], base)
        if u and u not in seen and looks_like_article_url(u):
            seen.add(u); links.append(u)
    for m in ARTICLE_HREF_RE.finditer(html_text):
        u = sanitize_7news(m.group(0), base)
        if u and u not in seen and looks_like_article_url(u):
            seen.add(u); links.append(u)
    for m in REL_ARTICLE_RE.finditer(html_text):
        u = sanitize_7news(urljoin(base, m.group(0)), base)
        if u and u not in seen and looks_like_article_url(u):
            seen.add(u); links.append(u)
    return links

def collect_from_entrypages() -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for base in ENTRY_BASES:
        hint = category_from_url(base)
        try:
            html_text = fetch(base).text
            for u in links_from_html_anywhere(html_text, base=base):
                out.setdefault(u, hint)
        except Exception as e:
            print(f"[WARN] entry scrape fail {base}: {e}", file=sys.stderr)
        time.sleep(0.2)
    return out

# ---------------- C) 淺層 BFS ----------------
def should_visit(url: str) -> bool:
    if not url.startswith(SECTION_ALLOWED_PREFIXES): return False
    if any(x in url for x in [".mp3",".mp4",".jpg",".jpeg",".png",".gif",".pdf",".svg",".webp"]): return False
    return True

def crawl_site(seeds: list[str], max_pages: int = 100) -> list[str]:
    q = deque(); seen_pages = set(); found_articles = set()
    for s in seeds:
        if should_visit(s): q.append(s); seen_pages.add(s)
    pages_visited = 0
    while q and pages_visited < max_pages:
        url = q.popleft()
        try:
            html_text = fetch(url).text
        except Exception as e:
            print(f"[WARN] crawl fetch fail {url}: {e}", file=sys.stderr); continue
        for art in links_from_html_anywhere(html_text, base=url):
            found_articles.add(art)
        soup = BeautifulSoup(html_text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/"): href = urljoin(url, href)
            if href and href not in seen_pages and should_visit(href):
                seen_pages.add(href); q.append(href)
        pages_visited += 1
        time.sleep(FETCH_SLEEP)
    return list(found_articles)

# ---------------- D) Google News 補位 ----------------
def extract_7news_from_text(text: str) -> str | None:
    if not text: return None
    text = html.unescape(text)
    for m in re.finditer(r'https?://[^\s\'">]+', text):
        u = m.group(0)
        if SEVEN_HOST in u:
            return u
    return None

def decode_gn_item(link_text: str, guid_text: str | None, desc_html: str | None) -> str | None:
    if link_text and SEVEN_HOST in link_text: return link_text.strip()
    if guid_text and SEVEN_HOST in guid_text: return guid_text.strip()
    u = extract_7news_from_text(desc_html or "")
    if u: return u
    if link_text and "news.google.com" in link_text:
        try:
            p = urlparse(link_text); qs = parse_qs(p.query)
            for key in ("u","url","q"):
                if key in qs and qs[key]:
                    cand = unquote(qs[key][0])
                    if SEVEN_HOST in cand: return cand
        except Exception:
            pass
    return None

def collect_from_google_news() -> list[str]:
    try:
        xml = fetch(GN_URL).text
    except Exception as e:
        print(f"[WARN] google news fetch fail: {e}", file=sys.stderr); return []
    urls = []
    try:
        root = ET.fromstring(xml)
        for it in root.findall(".//item"):
            link_text = (it.findtext("link") or "").strip()
            guid_text = (it.findtext("guid") or "").strip()
            desc_html = it.findtext("description") or ""
            real = decode_gn_item(link_text, guid_text, desc_html)
            if real and SEVEN_HOST in real and looks_like_article_url(real):
                urls.append(real)
    except Exception as e:
        print(f"[WARN] parse google news rss fail: {e}", file=sys.stderr); return []
    # 去重
    seen = set(); uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u); uniq.append(u)
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
    try:
        from feedgen.feed import FeedGenerator
    except Exception as e:
        print("[WARN] feedgen not available, skip XML:", e, file=sys.stderr); return
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

    seed_pages = ENTRY_BASES[:]  # 只用入口首頁
    url_to_hint = collect_from_entrypages()
    urls_b = list(url_to_hint.keys())
    print(f"[INFO] entry page urls: {len(urls_b)}", file=sys.stderr)

    urls_crawl = crawl_site(seeds=seed_pages, max_pages=100)
    print(f"[INFO] crawl urls: {len(urls_crawl)}", file=sys.stderr)

    # 合併去重（先用原 URL 去重，入文時再 canonicalize）
    hint_map = dict(url_to_hint)
    seen, merged = set(), []
    for u in urls_a + urls_b + urls_crawl:
        # 🚫 來源就已經略過 sitemap / .xml
        if (u not in seen) and looks_like_article_url(u):
            seen.add(u); merged.append(u)

    articles = []
    seen_links = set()
    for u in merged:
        try:
            html_text = fetch(u).text
            link_final = canonicalize_link(u, html_text)
            if link_final in seen_links: continue
            hint = hint_map.get(u)
            item = make_item(u, html_text, hint_section=hint)
            seen_links.add(link_final)
            articles.append(item)
            if len(articles) >= MAX_ITEMS * 2: break  # 多抓少少再截
            time.sleep(FETCH_SLEEP)
        except Exception as e:
            print(f"[WARN] fetch article fail {u}: {e}", file=sys.stderr)

    if len(articles) < MAX_ITEMS // 2:
        print("[INFO] few items; fallback Google News", file=sys.stderr)
        urls_gn = collect_from_google_news()
        print(f"[INFO] google news urls: {len(urls_gn)}", file=sys.stderr)
        for u in urls_gn:
            try:
                html_text = fetch(u).text
                link_final = canonicalize_link(u, html_text)
                if link_final in seen_links: continue
                item = make_item(u, html_text)
                seen_links.add(link_final)
                articles.append(item)
                if len(articles) >= MAX_ITEMS: break
                time.sleep(FETCH_SLEEP)
            except Exception as e:
                print(f"[WARN] GN article fetch fail {u}: {e}", file=sys.stderr)

    def key_dt(it):
        s = it.get("publishedAt")
        if not s: return datetime.min.replace(tzinfo=timezone.utc)
        try: return datetime.fromisoformat(s.replace("Z","+00:00"))
        except Exception: return datetime.min.replace(tzinfo=timezone.utc)

    articles.sort(key=key_dt, reverse=True)
    latest = articles[:MAX_ITEMS]
    json_out(latest, "seven_en.json")
    rss_out(latest, "seven_en.xml")
    print(f"[DONE] output {len(latest)} items", file=sys.stderr)
