# workers/scrape_abc_en.py

import json, re, sys, html, hashlib, time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, parse_qs, unquote, urljoin
from collections import deque

import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

# ---------------- 基本設定 ----------------
HEADERS = {"User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"}
TIMEOUT = 25
MAX_ITEMS = 200  # 想再多可以加大
FETCH_SLEEP = 0.4
ABC_HOST = "www.abc.net.au"
ROBOTS_URL = "https://www.abc.net.au/robots.txt"
# URL 正規化：固定 Host / Scheme 及移除 tracking 參數
CANON_HOST = "www.abc.net.au"
CANON_SCHEME = "https"
SYD = ZoneInfo("Australia/Sydney")

# 入口頁（已剔走 environment / technology 兩條經常 404/403 的入口）
ENTRY_BASES = [
    "https://www.abc.net.au/news",
    "https://www.abc.net.au/news/justin",
    "https://www.abc.net.au/news/politics",
    "https://www.abc.net.au/news/world",
    "https://www.abc.net.au/news/business",
    "https://www.abc.net.au/news/sport",
    "https://www.abc.net.au/news/health",
    "https://www.abc.net.au/news/science",
]

# 只巡航 /news/，避免去到 iview 等大區域
SECTION_ALLOWED_PREFIXES = (
    "https://www.abc.net.au/news/",
)

# 官方 RSS（作補位/增量；同樣剔走 environment / technology）
ABC_FEEDS = [
    # Top / Just In
    "https://www.abc.net.au/news/feed/45910/rss.xml",  # Top Stories
    "https://www.abc.net.au/news/feed/51120/rss.xml",  # Just In / Main
    "https://www.abc.net.au/news/feed/52278/rss.xml",  # Australia
    "https://www.abc.net.au/news/feed/51892/rss.xml",  # World
    "https://www.abc.net.au/news/feed/51800/rss.xml",  # Business
    "https://www.abc.net.au/news/feed/53446/rss.xml",  # Science
    "https://www.abc.net.au/news/feed/43606/rss.xml",  # Health
    "https://www.abc.net.au/news/feed/45926/rss.xml",  # Sport
    "https://www.abc.net.au/news/feed/45920/rss.xml",  # Politics
    "https://www.abc.net.au/news/feed/45922/rss.xml",  # Analysis & Opinion
]

# Google News（English + AU）作最後補位
GN_URL = (
    "https://news.google.com/rss/search"
    "?q=site:abc.net.au/news"
    "&hl=en-AU&gl=AU&ceid=AU:en"
)

# ---------------- 小工具 ----------------
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def to_iso(dt: datetime) -> str:
    """Datetime ➜ ISO8601（保留偏移）"""
    return dt.isoformat()

def ensure_utc_from_iso(s: str | None) -> datetime | None:
    """接受 ISO/RFC 常見字串 ➜ 轉成 aware UTC datetime；失敗回 None。"""
    if not s:
        return None
    try:
        # 支援 '...Z' / 帶偏移
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def as_sydney(dt_utc: datetime | None) -> datetime | None:
    if not dt_utc:
        return None
    return dt_utc.astimezone(SYD)

def canonical_abc_url(u: str) -> str:
    """
    把 ABC 文章 URL 正規化：
      - 強制 https://www.abc.net.au
      - 移除 fragment
      - 剷走常見 tracking query（utm_*、?sf、?WT.* 等）
      - 去除多餘的結尾斜線（但保留 path 本身）
    """
    try:
        p = urlparse(u)
        # 只處理 abc.net.au 範圍，其餘照樣回傳
        if "abc.net.au" not in (p.netloc or ""):
            return u
        # 設定規範 host/scheme
        netloc = CANON_HOST
        scheme = CANON_SCHEME
        # 清理 query
        qs = parse_qs(p.query, keep_blank_values=False)
        cleaned = {}
        for k, v in qs.items():
            lk = k.lower()
            if lk.startswith("utm_") or lk in {"WT.mc_id", "WT.tsrc", "sf"}:
                continue
            cleaned[k] = v
        # 重新組裝
        from urllib.parse import urlencode
        new_q = urlencode({k: vals[0] for k, vals in cleaned.items()}) if cleaned else ""
        path = p.path.rstrip("/")  # 多數 ABC 內容頁無尾斜線
        return f"{scheme}://{netloc}{path}" + (f"?{new_q}" if new_q else "")
    except Exception:
        return u

def normalize_date(raw: str | None) -> str | None:
    """把常見日期字串統一為 UTC ISO8601。"""
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

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r

# ---------------- Category 判斷（URL 優先） ----------------
def _slug_title_en(slug: str) -> str:
    m = {
        "justin": "Just In",
        "politics": "Politics",
        "world": "World",
        "business": "Business",
        "sport": "Sport",
        "health": "Health",
        "science": "Science",
        # 主頁 /news 當成 Top / General
        "news": "News",
    }
    return m.get(slug, slug.capitalize())

def category_from_url(u: str) -> str | None:
    """ /news/<section>/... 或 /news（主頁） """
    try:
        # 用正規化後 URL 再判斷
        p = urlparse(canonical_abc_url(u))
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        if len(parts) >= 2 and parts[0] == "news":
            # /news/YYYY-MM-DD/... → 其實無分欄，當作 News
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", parts[1]):
                return _slug_title_en("news")
            # /news/<section>/... → 取 section
            return _slug_title_en(parts[1])
        if len(parts) == 1 and parts[0] == "news":
            return _slug_title_en("news")
    except Exception:
        pass
    return None

def category_from_entry_base(base: str) -> str | None:
    """
    由入口 base URL 推斷分類（/news/<section> ➜ 標題化；/news ➜ News）
    用於 collect_from_entrypages() 當 hint。
    """
    try:
        p = urlparse(base)
        parts = [x for x in (p.path or "").strip("/").split("/") if x]
        # /news/<section>/...
        if len(parts) >= 2 and parts[0] == "news":
            return _slug_title_en(parts[1])
        # /news
        if len(parts) == 1 and parts[0] == "news":
            return _slug_title_en("news")
    except Exception:
        pass
    return None

# ---------------- JSON-LD / meta 解析 ----------------
def parse_json_ld(html_text: str):
    """由 JSON-LD 取 headline/description/date/url/section（NewsArticle/Article）。"""
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
                if t not in ("NewsArticle", "Article", "BlogPosting"):
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

def make_item(url: str, html_text: str, hint_section: str | None = None, source_hint: str = "ABC News"):
    # 1) URL 優先 section
    canon = canonical_abc_url(url)
    section = category_from_url(canon) or hint_section

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

    # ✅ 本地時間欄位（AEST/AEDT）：以 publishedAt（UTC）為基礎；再加 fetchedAtLocal
    pub_utc_dt = ensure_utc_from_iso(pub)
    pub_local_dt = as_sydney(pub_utc_dt)
    fetched_utc_dt = datetime.now(timezone.utc)
    fetched_local_dt = as_sydney(fetched_utc_dt)
    
    return {
        "id": hashlib.md5(canon.encode()).hexdigest(),
        "title": title or url,
        "link": url,
        "summary": desc,
        "publishedAt": pub,
        "source": source_hint,
        "fetchedAt": to_iso(fetched_utc_dt),
        # 👇 新增：本地顯示時間（悉尼）
        "publishedAtLocal": (to_iso(pub_local_dt) if pub_local_dt else None),
        "fetchedAtLocal": to_iso(fetched_local_dt),
        "localTimezone": "Australia/Sydney",
        # 分類（字串）+ 兼容多值（單一就包成陣列，無就 None）
        "sourceCategory": section,
        "sourceCategories": ([section] if section else None),
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
            if loc.text:
                urls.append(loc.text.strip())
        for loc in root.findall(".//sm:sitemap/sm:loc", ns):
            if loc.text:
                urls.append(loc.text.strip())
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
                # 只收 /news/ 文章（日期/或 article/stories 段）
                if "/news/" in u and (
                    re.search(r"/news/\d{4}-\d{2}-\d{2}/", u)
                    or "/news/" in u and "/article/" in u
                    or "/news/" in u and "/stories/" in u
                ):
                    out.append(u)
        except Exception as e:
            print(f"[WARN] sitemap fail {sm}: {e}", file=sys.stderr)
            continue
        if len(out) >= 15 * MAX_ITEMS:
            break
    # 去重
    seen = set(); uniq = []
    for u in out:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ---------------- B) 入口頁抽 link（含 script/JSON） ----------------
ARTICLE_HREF_RE = re.compile(
    r'https?://www\.abc\.net\.au/news/[A-Za-z0-9\-/_.]+'
)
REL_ARTICLE_RE = re.compile(
    r'/news/[A-Za-z0-9\-/_.]+'
)

def links_from_html_anywhere(html_text: str, base: str) -> list[str]:
    links = set()
    soup = BeautifulSoup(html_text, "html.parser")
    # 1) <a>
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            href = urljoin(base, href)
        if "/news/" in href:
            links.add(href)
    # 2) script/JSON 文字內的 URL
    for m in ARTICLE_HREF_RE.finditer(html_text):
        links.add(m.group(0))
    for m in REL_ARTICLE_RE.finditer(html_text):
        links.add(urljoin(base, m.group(0)))
    # 盡量限制只係文章頁（含日期或 article 段）
    filtered = []
    for u in links:
        if re.search(r"/news/\d{4}-\d{2}-\d{2}/", u) or ("/news/" in u and "/article/" in u):
            filtered.append(u)
    return filtered

def pagination_candidates(base_url: str, pages_each: int) -> list[str]:
    """生成常見分頁：?page=N、/page/N/；第 1 頁係 base 本身。"""
    out = [base_url.rstrip("/")]
    b = base_url.rstrip("/")
    for n in range(2, pages_each + 1):
        out.append(f"{b}?page={n}")
        out.append(f"{b}/page/{n}/")
    # 去重保持順序
    seen = set(); uniq = []
    for u in out:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

def collect_from_entrypages() -> dict[str, str | None]:
    """
    對每個入口首頁抓連結，並帶上入口分類 hint。
    回傳：{ article_url: category_hint_or_None }
    """
    out: dict[str, str | None] = {}
    for base in ENTRY_BASES:
        hint = category_from_entry_base(base)
        try:
            html_text = fetch(base).text
            for u in links_from_html_anywhere(html_text, base=base):
                cu = canonical_abc_url(u)
                out.setdefault(cu, hint)
        except Exception as e:
            print(f"[WARN] entry scrape fail {base}: {e}", file=sys.stderr)
            continue
        time.sleep(0.2)
    return out

# ---------------- C) /news/ 區淺層 BFS 爬（擴大覆蓋） ----------------
def should_visit(url: str) -> bool:
    if not url.startswith(SECTION_ALLOWED_PREFIXES):
        return False
    if any(x in url for x in [".mp3", ".mp4", ".jpg", ".jpeg", ".png", ".gif", ".pdf"]):
        return False
    return True

def crawl_news_section(seeds: list[str], max_pages: int = 80) -> list[str]:
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
        # 1) 抽文章 link
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

# ---------------- D) RSS 補位（官方多 feed） ----------------
def collect_from_rss() -> list[dict]:
    items = []
    for feed in ABC_FEEDS:
        try:
            xml = fetch(feed).text
        except Exception as e:
            print(f"[WARN] rss fetch fail {feed}: {e}", file=sys.stderr)
            continue
        try:
            root = ET.fromstring(xml)
            for it in root.findall(".//item"):
                link = (it.findtext("link") or "").strip()
                title = clean(it.findtext("title") or "")
                desc = clean(it.findtext("description") or "")
                if not link:
                    link = (it.findtext("guid") or "").strip()
                if not link:
                    continue
                items.append({
                    "title": title or link,
                    "link": link,
                    "summary": desc,
                    "source": "ABC News (RSS)",
                })
        except Exception as e:
            print(f"[WARN] parse rss fail {feed}: {e}", file=sys.stderr)
            continue
        time.sleep(0.2)
    # 去重 by link
    seen = set(); uniq = []
    for it in items:
        if it["link"] not in seen:
            seen.add(it["link"]); uniq.append(it)
    return uniq

# ---------------- E) Google News 補位（解 redirect） ----------------
def extract_abc_url_from_text(text: str) -> str | None:
    if not text:
        return None
    text = html.unescape(text)
    for m in re.finditer(r'https?://[^\s\'">]+', text):
        u = m.group(0)
        if ABC_HOST in u:
            return u
    return None

def decode_gn_item_to_article_url(link_text: str, guid_text: str | None, desc_html: str | None) -> str | None:
    if link_text and ABC_HOST in link_text:
        return link_text.strip()
    if guid_text and ABC_HOST in guid_text:
        return guid_text.strip()
    u = extract_abc_url_from_text(desc_html or "")
    if u:
        return u
    if link_text and "news.google.com" in link_text:
        try:
            p = urlparse(link_text); qs = parse_qs(p.query)
            for key in ("u", "url", "q"):
                if key in qs and qs[key]:
                    cand = unquote(qs[key][0])
                    if ABC_HOST in cand:
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
            if "/news/" in real:
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
        "source": "ABC News (EN) Aggregate",
        "generatedAt": to_iso(now_utc),
        "generatedAtLocal": to_iso(as_sydney(now_utc)),
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
        print("[WARN] feedgen not available, skip XML:", e, file=sys.stderr)
        return
    fg = FeedGenerator()
    fg.title("ABC News (EN) – Aggregated (Unofficial)")
    fg.link(href="https://www.abc.net.au/news/", rel='alternate')
    fg.description("Auto-generated (headings & summaries only).")
    fg.language("en")
    for it in items:
        fe = fg.add_entry()
        fe.id(it["id"])
        fe.title(it["title"])
        fe.link(href=it["link"])
        fe.description(it.get("summary") or it["title"])
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

    # C) /news/ 區淺層 BFS（擴大覆蓋；以入口首頁作為種子）
    urls_crawl = crawl_news_section(seeds=seed_pages, max_pages=80)
    print(f"[INFO] crawl urls: {len(urls_crawl)}", file=sys.stderr)

    # 合併 URL 去重（保留入口分類 hint）
    # 以正規化 URL 作 key
    hint_map = {canonical_abc_url(u): h for u, h in url_to_hint.items()}
    seen, merged_urls = set(), []
    for u in urls_a + urls_b + urls_crawl:
        cu = canonical_abc_url(u)
        if cu not in seen:
            seen.add(cu); merged_urls.append(cu)

    # 逐篇抓
    articles = []
    fetched = set()  # 避免同一篇抓兩次（http/https、帶參數等）
    for u in merged_urls:
        try:
            cu = canonical_abc_url(u)
            if cu in fetched:
                continue
            html_text = fetch(cu).text
            hint = hint_map.get(cu)
            item = make_item(cu, html_text, hint_section=hint, source_hint="ABC News")
            # 去重 by link
            if any(x["link"] == item["link"] for x in articles):
                continue
            articles.append(item)
            fetched.add(cu)
            time.sleep(FETCH_SLEEP)
        except Exception as e:
            print(f"[WARN] fetch article fail {u}: {e}", file=sys.stderr)

    # D) 如果仍然未夠 → RSS 補位
    if len(articles) < MAX_ITEMS // 2:
        print("[INFO] few items; fallback ABC RSS", file=sys.stderr)
        rss_items = collect_from_rss()
        for it in rss_items:
            link = it["link"]
            try:
                cu = canonical_abc_url(link)
                if cu in fetched:
                    continue
                html_text = fetch(cu).text
                item = make_item(cu, html_text, source_hint=it.get("source") or "ABC News (RSS)")
                if any(x["link"] == item["link"] for x in articles):
                    continue
                articles.append(item)
                fetched.add(cu)
                if len(articles) >= MAX_ITEMS:
                    break
                time.sleep(FETCH_SLEEP)
            except Exception as e:
                print(f"[WARN] ABC RSS article fetch fail {link}: {e}", file=sys.stderr)

    # E) 仍然唔夠 → Google News 補位
    if len(articles) < MAX_ITEMS // 2:
        print("[INFO] still few; fallback Google News", file=sys.stderr)
        urls_gn = collect_from_google_news()
        print(f"[INFO] google news urls: {len(urls_gn)}", file=sys.stderr)
        for u in urls_gn:
            try:
                cu = canonical_abc_url(u)
                if cu in fetched:
                    continue
                html_text = fetch(cu).text
                item = make_item(cu, html_text, source_hint="ABC via Google News")
                if any(x["link"] == item["link"] for x in articles):
                    continue
                articles.append(item)
                fetched.add(cu)
                if len(articles) >= MAX_ITEMS:
                    break
                time.sleep(FETCH_SLEEP)
            except Exception as e:
                print(f"[WARN] GN article fetch fail {u}: {e}", file=sys.stderr)

    # 以 publishedAt 排序（desc）；無日期放最後，最後截 MAX_ITEMS
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
    json_out(latest, "abc_en.json")
    rss_out(latest, "abc_en.xml")

    print(f"[DONE] output {len(latest)} items", file=sys.stderr)
