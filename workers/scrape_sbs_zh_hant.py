# workers/scrape_sbs_zh_hant.py
import json, re, sys, html, hashlib, time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

# ---------------- 基本設定 ----------------
HEADERS = {
    "User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"
}
TIMEOUT = 25
MAX_ITEMS = 30
FETCH_SLEEP = 0.6  # 抓單篇之間小睡，對站方友善

SBS_HOST = "www.sbs.com.au"
ROBOTS_URL = "https://www.sbs.com.au/robots.txt"
ENTRY_PAGES = [
    "https://www.sbs.com.au/language/chinese/zh-hant/",
    "https://www.sbs.com.au/language/chinese/",
]

GN_URL = (
    "https://news.google.com/rss/search"
    "?q=site:sbs.com.au/language/chinese"
    "&hl=zh-HK&gl=AU&ceid=AU:zh-Hant"
)

# ---------------- 小工具 ----------------
def iso_now(): return datetime.now(timezone.utc).isoformat()
def clean(s: str) -> str: return re.sub(r"\s+", " ", (s or "")).strip()

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r

def is_zh_hant_page(html_text: str) -> bool:
    # 盡量涵蓋 zh-Hant / zh-hant / zh_Hant 變體
    patterns = [
        r'<html[^>]+lang=["\']zh-?Hant(?:-[A-Z]{2})?["\']',
        r'property=["\']og:locale["\']\s+content=["\']zh_?Hant',
        r'name=["\']language["\']\s+content=["\']zh-?Hant',
    ]
    for pat in patterns:
        if re.search(pat, html_text, re.I):
            return True
    return False

def extract_meta_from_html(html_text: str):
    soup = BeautifulSoup(html_text, "html.parser")
    title = (soup.find("meta", property="og:title") or {}).get("content") \
        or (soup.title.string if soup.title else "")
    desc = (soup.find("meta", property="og:description") or {}).get("content") \
        or (soup.find("meta", attrs={"name": "description"}) or {}).get("content") \
        or ""
    pub = (
        (soup.find("meta", property="article:published_time") or {}).get("content")
        or (soup.find("meta", attrs={"name": "date"}) or {}).get("content")
        or None
    )
    return clean(title), clean(desc), pub

def make_item(url: str, html_text: str):
    title, summary, pub = extract_meta_from_html(html_text)
    return {
        "id": hashlib.md5(url.encode()).hexdigest(),
        "title": title or url,
        "link": url,
        "summary": summary,
        "publishedAt": pub,  # 可能為 ISO8601 或缺失
        "source": "SBS 中文（繁體）",
        "fetchedAt": iso_now(),
    }

# ---------------- A) 由 robots.txt 拎所有 sitemap，再篩中文文章 ----------------
SITEMAP_RE = re.compile(r"(?im)^\s*Sitemap:\s*(https?://\S+)\s*$")

def sitemaps_from_robots() -> list[str]:
    try:
        txt = fetch(ROBOTS_URL).text
    except Exception as e:
        print(f"[WARN] robots fetch fail: {e}", file=sys.stderr)
        return []
    return SITEMAP_RE.findall(txt)

def parse_sitemap_urls(xml_text: str) -> list[str]:
    # 支援 urlset / sitemapindex；若 namespace 搞唔掂，fallback 正則
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
                # 只要中文路徑的「文章」；排除 podcast
                if "/language/chinese/" in u and "/article/" in u and "/podcast-episode/" not in u:
                    out.append(u)
        except Exception as e:
            print(f"[WARN] sitemap fail {sm}: {e}", file=sys.stderr)
            continue
        # 避免無限膨脹
        if len(out) >= 10 * MAX_ITEMS:
            break
    # 去重
    seen = set(); uniq = []
    for u in out:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ---------------- B) 入口頁直抓（連 script/json 內的 link 都抽） ----------------
ARTICLE_HREF_RE = re.compile(
    r'https?://www\.sbs\.com\.au(?:/language/chinese(?:/zh-hant)?)?/article/[A-Za-z0-9\-/_]+'
)
REL_ARTICLE_RE = re.compile(
    r'/(?:language/chinese(?:/zh-hant)?)/article/[A-Za-z0-9\-/_]+'
)

def links_from_html_anywhere(html_text: str, base: str) -> list[str]:
    links = set()
    # 1) 普通 <a>
    soup = BeautifulSoup(html_text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            href = urljoin(base, href)
        if "/language/chinese/" in href and "/article/" in href:
            links.add(href)
    # 2) 原始文本 / script / JSON 內的 URL
    for m in ARTICLE_HREF_RE.finditer(html_text):
        links.add(m.group(0))
    for m in REL_ARTICLE_RE.finditer(html_text):
        links.add(urljoin(base, m.group(0)))
    # 排除 podcast
    return [u for u in links if "/podcast-episode/" not in u]

def collect_from_entrypages() -> list[str]:
    out = []
    for page in ENTRY_PAGES:
        try:
            html_text = fetch(page).text
            out += links_from_html_anywhere(html_text, base=page)
        except Exception as e:
            print(f"[WARN] entry scrape fail {page}: {e}", file=sys.stderr)
            continue
    # 去重
    seen = set(); uniq = []
    for u in out:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ---------------- C) Google News fallback（解 redirect → 只收文字文章） ----------------
def extract_sbs_url_from_text(text: str) -> str | None:
    if not text: return None
    text = html.unescape(text)
    for m in re.finditer(r'https?://[^\s\'">]+', text):
        u = m.group(0)
        if SBS_HOST in u:
            return u
    return None

def decode_gn_item_to_article_url(link_text: str, guid_text: str | None, desc_html: str | None) -> str | None:
    # 1) 直接係 SBS
    if link_text and SBS_HOST in link_text:
        return link_text.strip()
    if guid_text and SBS_HOST in guid_text:
        return guid_text.strip()
    # 2) description 裏面抽
    u = extract_sbs_url_from_text(desc_html or "")
    if u: return u
    # 3) 解析 news.google.com 的 redirect 參數
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
            if ("/language/chinese/" in real and "/article/" in real
                and "/podcast-episode/" not in real):
                urls.append(real)
    except Exception as e:
        print(f"[WARN] parse google news rss fail: {e}", file=sys.stderr)
        return []
    # 去重
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
    fg.title("SBS 中文（繁體）新聞（非官方聚合）")
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

    # B) 入口頁直抓（含 script/JSON 內 link）
    urls_b = collect_from_entrypages()
    print(f"[INFO] entry page urls: {len(urls_b)}", file=sys.stderr)

    # 合併去重
    seen, merged = set(), []
    for u in urls_a + urls_b:
        if u not in seen:
            seen.add(u); merged.append(u)

    # 逐篇抓，**只保留繁體頁面**
    articles = []
    for u in merged:
        try:
            html_text = fetch(u).text
            if not is_zh_hant_page(html_text):
                continue
            item = make_item(u, html_text)
            # 避免個別頁面係 podcast（保險檢查一次）
            if "/podcast-episode/" in item["link"]:
                continue
            articles.append(item)
            if len(articles) >= MAX_ITEMS:
                break
            time.sleep(FETCH_SLEEP)
        except Exception as e:
            print(f"[WARN] fetch article fail {u}: {e}", file=sys.stderr)

    # C) 如果仍然空 → Google News 補位
    if not articles:
        print("[INFO] empty after sitemap/entry; fallback Google News", file=sys.stderr)
        urls_c = collect_from_google_news()
        print(f"[INFO] google news urls: {len(urls_c)}", file=sys.stderr)
        for u in urls_c:
            try:
                html_text = fetch(u).text
                if not is_zh_hant_page(html_text):
                    continue
                item = make_item(u, html_text)
                if "/podcast-episode/" in item["link"]:
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

