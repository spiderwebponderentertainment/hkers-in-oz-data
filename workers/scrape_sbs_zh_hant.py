# workers/scrape_sbs_zh_hant.py
import json, re, sys, html, hashlib, time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote, urljoin
from collections import deque

import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

# ---------------- 基本設定 ----------------
HEADERS = {"User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"}
TIMEOUT = 25
MAX_ITEMS = 120            # 想多啲就加大
FETCH_SLEEP = 0.5         # 抓單篇之間小睡，對站方友善

SBS_HOST = "www.sbs.com.au"
ROBOTS_URL = "https://www.sbs.com.au/robots.txt"
ENTRY_PAGES = [
    "https://www.sbs.com.au/language/chinese/zh-hant/",
    "https://www.sbs.com.au/language/chinese/",
]
SECTION_ALLOWED_PREFIXES = (
    "https://www.sbs.com.au/language/chinese/",
)

# Google News（繁體 + AU）作補位
GN_URL = (
    "https://news.google.com/rss/search"
    "?q=site:sbs.com.au/language/chinese"
    "&hl=zh-HK&gl=AU&ceid=AU:zh-Hant"
)

# ---------------- 小工具 ----------------
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def normalize_date(raw: str | None) -> str | None:
    """標準化常見日期格式為 UTC ISO8601。"""
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    # 已是 UTC Z
    if raw.endswith("Z"):
        return raw
    # ISO 8601 與帶時區
    try:
        dt = None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", ""))
        except Exception:
            dt = None
        if dt:
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    # RFC822/1123 後備
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r

def looks_zh_hant_by_url(url: str) -> bool:
    return "/zh-hant/" in url

def is_zh_hant_by_html(html_text: str) -> bool:
    patterns = [
        r'<html[^>]+lang=["\']zh-?Hant(?:-[A-Z]{2})?["\']',
        r'property=["\']og:locale["\']\s+content=["\']zh_?Hant',
        r'name=["\']language["\']\s+content=["\']zh-?Hant',
    ]
    return any(re.search(p, html_text, re.I) for p in patterns)

def parse_json_ld(html_text: str):
    """
    由 JSON-LD 取標題/描述/日期/url。
    支援: NewsArticle / Article / BlogPosting / PodcastEpisode / AudioObject。
    日期欄位優先順序：
      datePublished > uploadDate > dateCreated > dateModified
    """
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t):
            txt = tag.string or tag.get_text() or ""
            try:
                data = json.loads(txt)
            except Exception:
                continue

            def select(obj: dict) -> dict | None:
                if not isinstance(obj, dict):
                    return None
                t = obj.get("@type")
                if isinstance(t, list):
                    t = next((x for x in t if isinstance(x, str)), None)
                if t not in ("NewsArticle", "Article", "BlogPosting", "PodcastEpisode", "AudioObject"):
                    return None
                date = (
                    obj.get("datePublished")
                    or obj.get("uploadDate")
                    or obj.get("dateCreated")
                    or obj.get("dateModified")
                    or ""
                )
                return {
                    "headline": obj.get("headline") or obj.get("name") or "",
                    "description": obj.get("description") or "",
                    "datePublished": date,
                    "url": obj.get("url") or "",
                }

            # 掃描物件 / 陣列 / @graph
            def scan(o):
                if isinstance(o, dict):
                    if "@graph" in o and isinstance(o["@graph"], list):
                        for g in o["@graph"]:
                            got = select(g)
                            if got:
                                return got
                    got = select(o)
                    if got:
                        return got
                if isinstance(o, list):
                    for each in o:
                        got = scan(each)
                        if got:
                            return got
                return None

            candidate = scan(data)
            if candidate:
                return candidate
    except Exception:
        pass
    return {}

def extract_meta_from_html(html_text: str):
    soup = BeautifulSoup(html_text, "html.parser")
    # 標題
    title = (soup.find("meta", property="og:title") or {}).get("content") \
        or (soup.title.string if soup.title else "") \
        or ""
    # 描述
    desc = (soup.find("meta", property="og:description") or {}).get("content") \
        or (soup.find("meta", attrs={"name": "description"}) or {}).get("content") \
        or ""
    # 日期：盡量多路徑
    pub = (
        (soup.find("meta", property="article:published_time") or {}).get("content")
        or (soup.find("meta", property="og:article:published_time") or {}).get("content")
        or (soup.find("meta", property="og:published_time") or {}).get("content")
        or (soup.find("meta", property="article:modified_time") or {}).get("content")   # 後備
        or (soup.find("meta", property="og:updated_time") or {}).get("content")        # 後備
        or (soup.find("meta", attrs={"itemprop": "datePublished"}) or {}).get("content")
        or (soup.find("meta", attrs={"name": "date"}) or {}).get("content")            # 最舊式
        or None
    )
    # <time datetime="...">
    if not pub:
        t = soup.find("time", attrs={"datetime": True})
        if t and t.get("datetime"):
            pub = t["datetime"]
    return clean(title), clean(desc), pub

def make_item(url: str, html_text: str):
    ld = parse_json_ld(html_text)
    if ld:
        title = clean(ld.get("headline", "")) or None
        desc = clean(ld.get("description", "")) or ""
        pub = normalize_date(ld.get("datePublished") or None)
        if not title:
            t2, d2, p2 = extract_meta_from_html(html_text)
            title = t2
            desc = desc or d2
            pub = pub or normalize_date(p2)
    else:
        title, desc, pub_raw = extract_meta_from_html(html_text)
        pub = normalize_date(pub_raw)
    return {
        "id": hashlib.md5(url.encode()).hexdigest(),
        "title": title or url,
        "link": url,
        "summary": desc,
        "publishedAt": pub,
        "source": "SBS 中文（繁體）",
        "fetchedAt": iso_now(),
    }

# ---------------- A) robots.txt ➜ 所有 sitemap ----------------
SITEMAP_RE = re.compile(r"(?im)^\s*Sitemap:\s*(https?://\S+)\s*$")

def sitemaps_from_robots() -> list[str]:
    try:
        txt = fetch(ROBOTS_URL).text
    except Exception as e:
        print(f"[WARN] robots fetch fail: {e}", file=sys.stderr)
        return []
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
    all_sitemaps = sitemaps_from_robots()
    out = []
    for sm in all_sitemaps:
        if not sm.lower().endswith(".xml"):
            continue
        try:
            xml = fetch(sm).text
            urls = parse_sitemap_urls(xml)
            for u in urls:
                # 收中文區嘅文章 + podcast-episode
                if "/language/chinese/" in u and ("/article/" in u or "/podcast-episode/" in u):
                    out.append(u)
        except Exception as e:
            print(f"[WARN] sitemap fail {sm}: {e}", file=sys.stderr)
            continue
        if len(out) >= 12 * MAX_ITEMS:
            break
    # 去重
    seen = set(); uniq = []
    for u in out:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ---------------- B) 入口頁抽 link（含 script/JSON） ----------------
ARTICLE_HREF_RE = re.compile(
    r'https?://www\.sbs\.com\.au(?:/language/chinese(?:/zh-hant)?)?/(?:article|podcast-episode)/[A-Za-z0-9\-/_]+'
)
REL_ARTICLE_RE = re.compile(
    r'/(?:language/chinese(?:/zh-hant)?)/(?:article|podcast-episode)/[A-Za-z0-9\-/_]+'
)

def links_from_html_anywhere(html_text: str, base: str) -> list[str]:
    links = set()
    soup = BeautifulSoup(html_text, "html.parser")
    # 1) <a>
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            href = urljoin(base, href)
        if "/language/chinese/" in href and ("/article/" in href or "/podcast-episode/" in href):
            links.add(href)
    # 2) script/JSON 文字內的 URL
    for m in ARTICLE_HREF_RE.finditer(html_text):
        links.add(m.group(0))
    for m in REL_ARTICLE_RE.finditer(html_text):
        links.add(urljoin(base, m.group(0)))
    return list(links)

def collect_from_entrypages() -> list[str]:
    out = []
    for page in ENTRY_PAGES:
        try:
            html_text = fetch(page).text
            out += links_from_html_anywhere(html_text, base=page)
        except Exception as e:
            print(f"[WARN] entry scrape fail {page}: {e}", file=sys.stderr)
            continue
    seen = set(); uniq = []
    for u in out:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ---------------- C) 中文區淺層 BFS 爬（擴大覆蓋） ----------------
def should_visit(url: str) -> bool:
    if not url.startswith(SECTION_ALLOWED_PREFIXES):
        return False
    if any(x in url for x in [".mp3", ".mp4", ".jpg", ".jpeg", ".png", ".gif"]):
        return False
    return True

def crawl_chinese_section(seeds: list[str], max_pages: int = 80) -> list[str]:
    q = deque()
    seen_pages = set()
    found_articles = set()

    for s in seeds:
        if should_visit(s):
            q.append(s); seen_pages.add(s)

    pages_visited = 0
    while q and pages_visited < max_pages:
        url = q.popleft()
        try:
            html_text = fetch(url).text
        except Exception as e:
            print(f"[WARN] crawl fetch fail {url}: {e}", file=sys.stderr)
            continue

        # 1) 抽文章 + podcast-episode link
        for art in links_from_html_anywhere(html_text, base=url):
            found_articles.add(art)

        # 2) 將頁面內可巡航 link 入隊
        soup = BeautifulSoup(html_text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/"):
                href = urljoin(url, href)
            if not href or href in seen_pages:
                continue
            if should_visit(href):
                seen_pages.add(href)
                q.append(href)

        pages_visited += 1
        time.sleep(FETCH_SLEEP)

    return list(found_articles)

# ---------------- D) Google News 補位（解 redirect） ----------------
def extract_sbs_url_from_text(text: str) -> str | None:
    if not text: return None
    text = html.unescape(text)
    for m in re.finditer(r'https?://[^\s\'">]+', text):
        u = m.group(0)
        if SBS_HOST in u:
            return u
    return None

def decode_gn_item_to_article_url(link_text: str, guid_text: str | None, desc_html: str | None) -> str | None:
    if link_text and SBS_HOST in link_text:
        return link_text.strip()
    if guid_text and SBS_HOST in guid_text:
        return guid_text.strip()
    u = extract_sbs_url_from_text(desc_html or "")
    if u: return u
    if link_text and "news.google.com" in link_text:
        try:
            p = urlparse(link_text); qs = parse_qs(p.query)
            for key in ("u", "url", "q"):
                if key in qs and qs[key]:
                    cand = unquote(qs[key][0])
                    if SBS_HOST in cand:
                        return cand
        except Exception:
            pass
    return None

def collect_from_google_news() -> list[str]:
    try:
        xml = fetch(GN_URL).text
    except Exception as e:
        print(f"[WARN] google news fetch fail: {e}", file=sys.stderr)
        return []
    urls = []
    try:
        root = ET.fromstring(xml)
        for it in root.findall(".//item"):
            link_text = (it.findtext("link") or "").strip()
            guid_text = (it.findtext("guid") or "").strip()
            desc_html = it.findtext("description") or ""
            real = decode_gn_item_to_article_url(link_text, guid_text, desc_html)
            if not real:
                continue
            if "/language/chinese/" in real and ("/article/" in real or "/podcast-episode/" in real):
                urls.append(real)
    except Exception as e:
        print(f"[WARN] parse google news rss fail: {e}", file=sys.stderr)
        return []
    seen = set(); uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ---------------- 輸出 ----------------
def json_out(items, path):
    payload = {"source": "SBS 中文（繁體）", "generatedAt": iso_now(), "count": len(items), "items": items}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def rss_out(items, path):
    try:
        from feedgen.feed import FeedGenerator
    except Exception as e:
        print("[WARN] feedgen not available, skip XML:", e, file=sys.stderr)
        return
    fg = FeedGenerator()
    fg.title("SBS 中文（繁體）新聞／Podcast（非官方聚合）")
    fg.link(href="https://www.sbs.com.au/language/chinese/zh-hant/", rel='alternate')
    fg.description("Auto-generated (headings & summaries only).")
    fg.language("zh-hant")
    for it in items:
        fe = fg.add_entry()
        fe.id(it["id"]); fe.title(it["title"]); fe.link(href=it["link"])
        fe.description(it["summary"] or it["title"])
        # feedgen 可接受 pubDate，但呢度我哋保持簡單：輸出 JSON 為主
    fg.rss_file(path)

# ---------------- 主程式 ----------------
if __name__ == "__main__":
    # A) robots 所有 sitemap
    urls_a = collect_from_sitemaps()
    print(f"[INFO] sitemap urls: {len(urls_a)}", file=sys.stderr)

    # B) 入口頁直抓（含 script/JSON 內 link）
    urls_b = collect_from_entrypages()
    print(f"[INFO] entry page urls: {len(urls_b)}", file=sys.stderr)

    # C) 中文區淺層 BFS（擴大覆蓋）
    urls_crawl = crawl_chinese_section(
        seeds=ENTRY_PAGES,   # 兩個入口
        max_pages=80         # 想再多就加到 120；會慢啲
    )
    print(f"[INFO] crawl urls: {len(urls_crawl)}", file=sys.stderr)

    # 合併去重
    seen, merged = set(), []
    for u in urls_a + urls_b + urls_crawl:
        if u not in seen:
            seen.add(u); merged.append(u)

    # 逐篇抓（文字 + podcast），保留繁體（URL含 zh-hant 或 HTML 判斷）
    articles = []
    for u in merged:
        try:
            html_text = fetch(u).text
            if not (looks_zh_hant_by_url(u) or is_zh_hant_by_html(html_text)):
                continue
            item = make_item(u, html_text)
            articles.append(item)
            if len(articles) >= MAX_ITEMS:
                break
            time.sleep(FETCH_SLEEP)
        except Exception as e:
            print(f("[WARN] fetch article fail {u}: {e}"), file=sys.stderr)

    # D) 如果仍然未夠 → Google News 補位
    if len(articles) < MAX_ITEMS // 2:
        print("[INFO] few items; fallback Google News", file=sys.stderr)
        urls_gn = collect_from_google_news()
        print(f"[INFO] google news urls: {len(urls_gn)}", file=sys.stderr)
        for u in urls_gn:
            try:
                html_text = fetch(u).text
                if not (looks_zh_hant_by_url(u) or is_zh_hant_by_html(html_text)):
                    continue
                item = make_item(u, html_text)
                # 去重（以 link 去重）
                if any(x["link"] == item["link"] for x in articles):
                    continue
                articles.append(item)
                if len(articles) >= MAX_ITEMS:
                    break
                time.sleep(FETCH_SLEEP)
            except Exception as e:
                print(f"[WARN] GN article fetch fail {u}: {e}", file=sys.stderr)

    # 輸出到 repo root（配合 Pages: root）
    json_out(articles, "sbs_zh_hant.json")
    rss_out(articles,  "sbs_zh_hant.xml")
    print(f"[DONE] output {len(articles)} items", file=sys.stderr)
