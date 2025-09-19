# workers/scrape_sbs_en.py
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
MAX_ITEMS = 200            # 要 200
FETCH_SLEEP = 0.5          # 抓單篇之間小睡，對站方友善

SBS_HOST = "www.sbs.com.au"
ROBOTS_URL = "https://www.sbs.com.au/robots.txt"

# 英文入口頁（你提供的）
ENTRY_BASES = [
    "https://www.sbs.com.au/news/collection/just-in",
    "https://www.sbs.com.au/news/collection/top-stories",
    "https://www.sbs.com.au/news/topic/cost-of-living",
    "https://www.sbs.com.au/news/topic/australia",
    "https://www.sbs.com.au/news/topic/hamas-israel-war",
    "https://www.sbs.com.au/news/topic/world",
    "https://www.sbs.com.au/news/topic/politics",
    "https://www.sbs.com.au/news/topic/immigration",
    "https://www.sbs.com.au/news/topic/indigenous",
    "https://www.sbs.com.au/news/topic/environment",
    "https://www.sbs.com.au/news/topic/life",
]

# 只巡航 /news/ 範圍
SECTION_ALLOWED_PREFIXES = (
    "https://www.sbs.com.au/news",
)

# Google News（English + AU）作補位
GN_URL = (
    "https://news.google.com/rss/search"
    "?q=site:sbs.com.au/news"
    "&hl=en-AU&gl=AU&ceid=AU:en"
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
    s = raw.strip()
    if not s:
        return None
    # 已是 UTC Z
    if s.endswith("Z"):
        return s
    # ISO 8601 與帶時區
    try:
        dt = None
        try:
            dt = datetime.fromisoformat(s.replace("Z", ""))
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
        dt = parsedate_to_datetime(s)
        if not dt:
            return None
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r

# ---------------- Category 判斷（URL 優先） ----------------
def _slug_title(slug: str) -> str:
    m = {
        "just-in": "Just In",
        "top-stories": "Top Stories",
        "cost-of-living": "Cost of Living",
        "australia": "Australia",
        "hamas-israel-war": "Hamas-Israel War",
        "world": "World",
        "politics": "Politics",
        "immigration": "Immigration",
        "indigenous": "Indigenous",
        "environment": "Environment",
        "life": "Life",
    }
    if slug in m: return m[slug]
    # fallback: Title Case by hyphen
    return " ".join(w.capitalize() for w in slug.split("-") if w)

def category_from_url(u: str) -> str | None:
    try:
        p = urlparse(u)
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        # /news/topic/<slug>/...
        if len(parts) >= 3 and parts[0] == "news" and parts[1] == "topic":
            return _slug_title(parts[2])
        # /news/collection/<slug>/...
        if len(parts) >= 3 and parts[0] == "news" and parts[1] == "collection":
            return _slug_title(parts[2])
    except Exception:
        pass
    return None

# ---------------- JSON-LD / meta 解析 ----------------
def parse_json_ld(html_text: str):
    """
    由 JSON-LD 取標題/描述/日期/url/分類。
    支援: NewsArticle / Article / BlogPosting / PodcastEpisode / AudioObject。
    日期欄位優先：
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
                # 分類（可能在 articleSection）
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
    title = (soup.find("meta", property="og:title") or {}).get("content") \
        or (soup.title.string if soup.title else "") \
        or ""
    desc = (soup.find("meta", property="og:description") or {}).get("content") \
        or (soup.find("meta", attrs={"name": "description"}) or {}).get("content") \
        or ""
    pub = (
        (soup.find("meta", property="article:published_time") or {}).get("content")
        or (soup.find("meta", property="og:article:published_time") or {}).get("content")
        or (soup.find("meta", property="og:published_time") or {}).get("content")
        or (soup.find("meta", attrs={"itemprop": "datePublished"}) or {}).get("content")
        or (soup.find("meta", attrs={"itemprop": "uploadDate"}) or {}).get("content")
        or (soup.find("meta", attrs={"name": "date"}) or {}).get("content")
        or None
    )
    # <time datetime="...">
    if not pub:
        t = soup.find("time", attrs={"datetime": True})
        if t and t.get("datetime"):
            pub = t["datetime"]
    # Section 後備
    section = (
        (soup.find("meta", property="article:section") or {}).get("content")
        or (soup.find("meta", attrs={"name": "section"}) or {}).get("content")
        or ""
    )
    return clean(title), clean(desc), pub, (section or None)

def make_item(url: str, html_text: str, hint_section: str | None = None):
    # 1) URL 優先
    section = category_from_url(url) or hint_section

    # 2) 內容頁解析（日期 + 可能的分類後備）
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

    return {
        "id": hashlib.md5(url.encode()).hexdigest(),
        "title": title or url,
        "link": url,
        "summary": desc,
        "publishedAt": pub,
        "source": "SBS English",
        "fetchedAt": iso_now(),
        "sourceCategory": section,   # 👈 輸出分類
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
                # 目標：英語新聞/節目文章
                if "/news/" in u and any(seg in u for seg in ("/article/", "/podcast-episode/", "/story/")):
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
    r'https?://www\.sbs\.com\.au/news/(?:article|story|podcast-episode)/[A-Za-z0-9\-/_]+'
)
REL_ARTICLE_RE = re.compile(
    r'/news/(?:article|story|podcast-episode)/[A-Za-z0-9\-/_]+'
)

def links_from_html_anywhere(html_text: str, base: str) -> list[str]:
    links = set()
    soup = BeautifulSoup(html_text, "html.parser")
    # 1) <a>
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            href = urljoin(base, href)
        if "/news/" in href and any(seg in href for seg in ("/article/", "/story/", "/podcast-episode/")):
            links.add(href)
    # 2) script/JSON 文字內的 URL
    for m in ARTICLE_HREF_RE.finditer(html_text):
        links.add(m.group(0))
    for m in REL_ARTICLE_RE.finditer(html_text):
        links.add(urljoin(base, m.group(0)))
    return list(links)

def category_from_entry_base(base: str) -> str | None:
    """由入口 base URL 推斷該入口對應的 Category（hint）"""
    try:
        p = urlparse(base)
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        if len(parts) >= 3 and parts[0] == "news" and parts[1] in ("topic", "collection"):
            return _slug_title(parts[2])
    except Exception:
        pass
    return None

def collect_from_entrypages() -> dict[str, str | None]:
    """
    對每個入口 + 分頁候選頁抓連結，並帶上入口推斷的 category hint。
    回傳：{ article_url: category_hint_or_None }
    """
    out: dict[str, str | None] = {}
    for base in ENTRY_BASES:
        hint = category_from_entry_base(base)
        try:
            # 只抓入口首頁；不再嘗試分頁
            html_text = fetch(base).text
            for u in links_from_html_anywhere(html_text, base=base):
                if u not in out:
                    out[u] = hint
        except Exception as e:
            print(f"[WARN] entry scrape fail {base}: {e}", file=sys.stderr)
            continue
        time.sleep(0.2)
    return out

# ---------------- C) 英文新聞區淺層 BFS 爬（擴大覆蓋） ----------------
def should_visit(url: str) -> bool:
    if not url.startswith(SECTION_ALLOWED_PREFIXES):
        return False
    if any(x in url for x in [".mp3", ".mp4", ".jpg", ".jpeg", ".png", ".gif"]):
        return False
    return True

def crawl_news_section(seeds: list[str], max_pages: int = 120) -> list[str]:
    """
    用 BFS 擴大覆蓋；從入口 seeds（已含分頁候選）開始
    """
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
            if "/news/" in real and any(seg in real for seg in ("/article/", "/story/", "/podcast-episode/")):
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
    payload = {"source": "SBS English", "generatedAt": iso_now(), "count": len(items), "items": items}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def rss_out(items, path):
    try:
        from feedgen.feed import FeedGenerator
    except Exception as e:
        print("[WARN] feedgen not available, skip XML:", e, file=sys.stderr)
        return
    fg = FeedGenerator()
    fg.title("SBS English News (Unofficial Aggregation)")
    fg.link(href="https://www.sbs.com.au/news", rel='alternate')
    fg.description("Auto-generated (headings & summaries only).")
    fg.language("en")
    for it in items:
        fe = fg.add_entry()
        fe.id(it["id"]); fe.title(it["title"]); fe.link(href=it["link"])
        fe.description(it["summary"] or it["title"])
    fg.rss_file(path)

# ---------------- 主程式 ----------------
if __name__ == "__main__":
    # A) robots 所有 sitemap
    urls_a = collect_from_sitemaps()
    print(f"[INFO] sitemap urls: {len(urls_a)}", file=sys.stderr)

    # B) 入口頁直抓（只抓入口首頁；不再嘗試分頁）
    seed_pages = ENTRY_BASES[:]
    url_to_hint = collect_from_entrypages()
    urls_b = list(url_to_hint.keys())
    print(f"[INFO] entry page urls: {len(urls_b)}", file=sys.stderr)

    # C) /news/ 淺層 BFS（擴大覆蓋；以入口首頁作為 seeds）
    urls_crawl = crawl_news_section(
        seeds=seed_pages,
        max_pages=120
    )
    print(f"[INFO] crawl urls: {len(urls_crawl)}", file=sys.stderr)

    # 合併去重（保留從入口頁得到的 category hint）
    hint_map = dict(url_to_hint)  # article_url -> category_hint
    seen, merged = set(), []
    for u in urls_a + urls_b + urls_crawl:
        if u not in seen:
            seen.add(u); merged.append(u)

    # 逐篇抓內容（含 Podcast 日期），並套用 URL 優先的 Category
    articles = []
    for u in merged:
        try:
            html_text = fetch(u).text
            hint = hint_map.get(u)  # 入口頁帶來的分類提示
            item = make_item(u, html_text, hint_section=hint)
            articles.append(item)
            time.sleep(FETCH_SLEEP)
        except Exception as e:
            print(f"[WARN] fetch article fail {u}: {e}", file=sys.stderr)

    # D) 如果仍然未夠 → Google News 補位
    if len(articles) < MAX_ITEMS // 2:
        print("[INFO] few items; fallback Google News", file=sys.stderr)
        urls_gn = collect_from_google_news()
        print(f"[INFO] google news urls: {len(urls_gn)}", file=sys.stderr)
        for u in urls_gn:
            try:
                html_text = fetch(u).text
                item = make_item(u, html_text)
                # 去重（以 link 去重）
                if any(x["link"] == item["link"] for x in articles):
                    continue
                articles.append(item)
                time.sleep(FETCH_SLEEP)
            except Exception as e:
                print(f"[WARN] GN article fetch fail {u}: {e}", file=sys.stderr)

    # 以 publishedAt 排序（desc）；無日期放最後
    def key_dt(it):
        s = it.get("publishedAt")
        if not s:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    articles.sort(key=key_dt, reverse=True)
    latest = articles[:MAX_ITEMS]

    # 輸出到 repo root（配合 Pages: root）
    json_out(latest, "sbs_en.json")
    rss_out(latest,  "sbs_en.xml")
    print(f"[DONE] output {len(latest)} items", file=sys.stderr)
