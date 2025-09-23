# workers/scrape_sbs_zh_hant.py
import json, re, sys, html, hashlib, time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, parse_qs, unquote, urljoin
from collections import deque

import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

# ---------------- 基本設定 ----------------
HEADERS = {
    "User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)",
    "Accept-Language": "zh-HK,zh-TW;q=0.9,zh;q=0.8,en;q=0.5",
}
TIMEOUT = 25
MAX_ITEMS = 200            # 想多啲就加大
FETCH_SLEEP = 0.5         # 抓單篇之間小睡，對站方友善

SBS_HOST = "www.sbs.com.au"
ROBOTS_URL = "https://www.sbs.com.au/robots.txt"
SYD = ZoneInfo("Australia/Sydney")

# 入口頁（繁中）
ENTRY_BASES = [
    "https://www.sbs.com.au/language/chinese/zh-hant/",
    "https://www.sbs.com.au/language/chinese/",

    # 你新增嘅入口
    "https://www.sbs.com.au/language/chinese/zh-hant/topic/news",
    "https://www.sbs.com.au/language/chinese/zh-hant/australian-chinese",
    "https://www.sbs.com.au/language/chinese/zh-hant/collection/sbs50",
    "https://www.sbs.com.au/language/chinese/zh-hant/topic/life-in-australia",
    "https://www.sbs.com.au/language/chinese/zh-hant/collection/first-nations-stories-in-traditional-chinese",
    "https://www.sbs.com.au/language/chinese/zh-hant/collection/cantonese-community-notices",
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

def to_iso(dt: datetime) -> str:
    """安全輸出 ISO8601（保留偏移）"""
    return dt.isoformat()

def ensure_utc(dt: datetime | None) -> datetime | None:
    """任何 naive/其他時區 datetime → UTC aware"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def as_sydney(dt_utc: datetime | None) -> datetime | None:
    """UTC → 悉尼時區（自動處理 AEST/AEDT）"""
    if dt_utc is None:
        return None
    return dt_utc.astimezone(SYD)

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

# ---------------- 分類（以 URL 為先） ----------------
def _slug_title_zh(slug: str) -> str:
    """將已知 slug 轉成中文分類名；未知則回傳 slug 本身（保證有值）"""
    m = {
        "news": "新聞",
        "australian-chinese": "澳洲華人",
        "sbs50": "SBS50",
        "life-in-australia": "在澳生活",
        "first-nations-stories-in-traditional-chinese": "第一民族故事",
        "cantonese-community-notices": "粵語社區通告",
    }
    return m.get(slug, slug)

def category_from_url(u: str) -> str | None:
    """
    /language/chinese/zh-hant/topic/<slug>/...
    /language/chinese/zh-hant/collection/<slug>/...
    /language/chinese/zh-hant/australian-chinese
    """
    try:
        p = urlparse(u)
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        # .../zh-hant/topic/<slug>/...
        if len(parts) >= 5 and parts[0] == "language" and parts[1] == "chinese" and parts[2] == "zh-hant" and parts[3] == "topic":
            return _slug_title_zh(parts[4])
        # .../zh-hant/collection/<slug>/...
        if len(parts) >= 5 and parts[0] == "language" and parts[1] == "chinese" and parts[2] == "zh-hant" and parts[3] == "collection":
            return _slug_title_zh(parts[4])
        # 特例：/zh-hant/australian-chinese
        if len(parts) >= 4 and parts[0] == "language" and parts[1] == "chinese" and parts[2] == "zh-hant" and parts[3] == "australian-chinese":
            return _slug_title_zh("australian-chinese")
    except Exception:
        pass
    return None

# ---------------- JSON-LD / meta 解析 ----------------
def parse_json_ld(html_text: str):
    """
    由 JSON-LD 取標題/描述/日期/url/分類。
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
    # 標題
    title = (soup.find("meta", property="og:title") or {}).get("content") \
        or (soup.title.string if soup.title else "") \
        or ""
    # 描述
    desc = (soup.find("meta", property="og:description") or {}).get("content") \
        or (soup.find("meta", attrs={"name": "description"}) or {}).get("content") \
        or ""
    # 日期：盡量多路徑（含 podcast）
    pub = (
        (soup.find("meta", property="article:published_time") or {}).get("content")
        or (soup.find("meta", property="og:article:published_time") or {}).get("content")
        or (soup.find("meta", property="og:published_time") or {}).get("content")
        or (soup.find("meta", property="article:modified_time") or {}).get("content")   # 後備
        or (soup.find("meta", property="og:updated_time") or {}).get("content")        # 後備
        or (soup.find("meta", attrs={"itemprop": "datePublished"}) or {}).get("content")
        or (soup.find("meta", attrs={"itemprop": "uploadDate"}) or {}).get("content")
        or (soup.find("meta", attrs={"name": "date"}) or {}).get("content")            # 最舊式
        or None
    )
    # <time datetime="...">
    if not pub:
        t = soup.find("time", attrs={"datetime": True})
        if t and t.get("datetime"):
            pub = t["datetime"]
    # 分類後備
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
            title = t2
            desc = desc or d2
            pub = pub or normalize_date(p2)
            section = section or s2
    else:
        t2, d2, p2, s2 = extract_meta_from_html(html_text)
        title = t2
        desc = d2
        pub = normalize_date(p2)
        section = section or s2

    # 3) 構造時間欄位（UTC + 悉尼本地）
    pub_iso = pub  # normalize_date 已回傳 UTC ISO 或 None
    try:
        pub_dt_utc = ensure_utc(datetime.fromisoformat(pub_iso.replace("Z", "+00:00"))) if pub_iso else None
    except Exception:
        pub_dt_utc = None
    fetched_dt_utc = datetime.now(timezone.utc)
    pub_local = as_sydney(pub_dt_utc)
    fetched_local = as_sydney(fetched_dt_utc)

    return {
        "id": hashlib.md5(url.encode()).hexdigest(),
        "title": title or url,
        "link": url,
        "summary": desc,
        # UTC 欄位（排序/比較用）
        "publishedAt": pub_iso,
        "fetchedAt": to_iso(fetched_dt_utc),
        # 悉尼本地時間（顯示用；自動 AEST/AEDT）
        "publishedAtLocal": (to_iso(pub_local) if pub_local else None),
        "fetchedAtLocal": to_iso(fetched_local),
        "localTimezone": "Australia/Sydney",
        "source": "SBS 中文（繁體）",
        "sourceCategory": section,  # 👈 新增分類
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

# --- 規範化/清洗 SBS 連結 ---
def sanitize_sbs_url(u: str, base: str) -> str | None:
    if not u:
        return None
    u = html.unescape(u).strip()
    # //xxx → https://xxx
    if u.startswith("//"):
        u = "https:" + u
    # 相對路徑 → 絕對
    if u.startswith("/"):
        u = urljoin(base, u)
    # 取第一段（防止 'url1 %20http://url2' / 'url1 http://url2'）
    u0 = u.split()[0]
    pos = u0.find("http", 1)
    if pos > 0:
        u0 = u0[:pos]
    # 去尾部標點
    u0 = u0.rstrip('"\')]>.,')
    # 去尾部編碼空白（%20、%09、%0A、%0D）
    u0 = re.sub(r'(?:%20|%09|%0A|%0D)+$', '', u0, flags=re.IGNORECASE)
    # 基本合法性
    p = urlparse(u0)
    if not (p.scheme in ("http", "https") and p.netloc):
        return None
    if SBS_HOST not in p.netloc:
        return None
    # 只收中文區文章/Podcast
    if "/language/chinese/" not in u0 or ("/article/" not in u0 and "/podcast-episode/" not in u0):
        return None
    return u0

def links_from_html_anywhere(html_text: str, base: str) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    soup = BeautifulSoup(html_text, "html.parser")
    # 1) <a>
    for a in soup.find_all("a", href=True):
        href_raw = a["href"]
        href = sanitize_sbs_url(href_raw, base)
        if href and href not in seen:
            seen.add(href); links.append(href)
    # 2) script/JSON 文字內的 URL
    for m in ARTICLE_HREF_RE.finditer(html_text):
        u = sanitize_sbs_url(m.group(0), base)
        if u and u not in seen:
            seen.add(u); links.append(u)
    for m in REL_ARTICLE_RE.finditer(html_text):
        u = sanitize_sbs_url(urljoin(base, m.group(0)), base)
        if u and u not in seen:
            seen.add(u); links.append(u)
    return links

def category_from_entry_base(base: str) -> str | None:
    """由入口 base URL 推斷分類（hint）"""
    try:
        p = urlparse(base)
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        if len(parts) >= 5 and parts[0]=="language" and parts[1]=="chinese" and parts[2]=="zh-hant" and parts[3] in ("topic","collection"):
            return _slug_title_zh(parts[4])
        if len(parts) >= 4 and parts[0]=="language" and parts[1]=="chinese" and parts[2]=="zh-hant" and parts[3]=="australian-chinese":
            return _slug_title_zh("australian-chinese")
    except Exception:
        pass
    return None

def collect_from_entrypages() -> dict[str, str | None]:
    """對每個入口首頁抓連結，回傳 { article_url: category_hint_or_None }"""
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
    found_articles: list[str] = []
    seen_articles: set[str] = set()

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
            if art not in seen_articles:
                seen_articles.add(art)
                found_articles.append(art)

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

    return found_articles

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
        print(f"[WARN] google news rss fetch fail: {e}", file=sys.stderr)
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
    now_utc = datetime.now(timezone.utc)
    payload = {
        "source": "SBS 中文（繁體）",
        "generatedAt": to_iso(now_utc),
        "generatedAtLocal": to_iso(as_sydney(now_utc)),
        "localTimezone": "Australia/Sydney",
        "count": len(items),
        "items": items,
    }
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

    # C) 中文區淺層 BFS（擴大覆蓋；以入口首頁作為 seeds）
    urls_crawl = crawl_chinese_section(
        seeds=seed_pages,   # 入口首頁
        max_pages=80
    )
    print(f"[INFO] crawl urls: {len(urls_crawl)}", file=sys.stderr)

    # 合併去重（保留入口分類 hint）
    hint_map = dict(url_to_hint)  # article_url -> category_hint
    seen, merged = set(), []
    for u in (urls_a + urls_b + urls_crawl):
        if u not in seen:
            seen.add(u); merged.append(u)

    # 逐篇抓：抓多啲先（例如 3 倍），之後再按日期排序截 MAX_ITEMS
    articles = []
    HARD_CAP = MAX_ITEMS * 3
    for u in merged:
        try:
            html_text = fetch(u).text
            if not (looks_zh_hant_by_url(u) or is_zh_hant_by_html(html_text)):
                continue
            hint = hint_map.get(u)
            item = make_item(u, html_text, hint_section=hint)
            articles.append(item)
            if len(articles) >= HARD_CAP:
                break
            time.sleep(FETCH_SLEEP)
        except Exception as e:
            print(f"[WARN] fetch article fail {u}: {e}", file=sys.stderr)

    # D) 如果仍然未夠 → Google News 補位（同樣抓多啲）
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
                if len(articles) >= HARD_CAP:
                    break
                time.sleep(FETCH_SLEEP)
            except Exception as e:
                print(f"[WARN] GN article fetch fail {u}: {e}", file=sys.stderr)

    # 以 publishedAt（ISO 字串）排序（desc）；無日期放最後
    def key_dt(it):
        s = it.get("publishedAt")
        if not s:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            ss = s.replace("Z", "+00:00")  # 支援有/無 Z
            return datetime.fromisoformat(ss)
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    articles.sort(key=key_dt, reverse=True)
    latest = articles[:MAX_ITEMS]

    # 輸出
    json_out(latest, "sbs_zh_hant.json")
    rss_out(latest,  "sbs_zh_hant.xml")
    print(f"[DONE] output {len(latest)} items", file=sys.stderr)
