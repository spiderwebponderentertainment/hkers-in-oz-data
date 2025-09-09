# workers/scrape_sbs_zh_hant.py
import json, re, hashlib, sys, html
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote
import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

# ---- 設定 ----
HEADERS = {"User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"}
TIMEOUT = 25
MAX_ITEMS = 30

SBS_HOST = "www.sbs.com.au"
SITEMAP_INDEX = "https://www.sbs.com.au/sitemap.xml"
ENTRY_PAGES = [
    "https://www.sbs.com.au/language/chinese/zh-hant/",
    "https://www.sbs.com.au/language/chinese/",
]
# Google News（繁體 + AU）
GN_URL = (
    "https://news.google.com/rss/search"
    "?q=site:sbs.com.au/language/chinese"
    "&hl=zh-HK&gl=AU&ceid=AU:zh-Hant"
)

# ---- 小工具 ----
def iso_now(): return datetime.now(timezone.utc).isoformat()
def clean(s: str) -> str: return re.sub(r"\s+", " ", (s or "")).strip()

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r

def is_zh_hant_page(html_text: str) -> bool:
    # 以 <html lang="zh-Hant"> 或 og:locale 判斷繁體
    if re.search(r'<html[^>]+lang=["\']zh-Hant["\']', html_text, re.I):
        return True
    if re.search(r'property=["\']og:locale["\']\s+content=["\']zh_Hant', html_text, re.I):
        return True
    return False

def extract_meta_from_html(html_text: str):
    soup = BeautifulSoup(html_text, "html.parser")
    title = (soup.find("meta", property="og:title") or {}).get("content") \
        or (soup.title.string if soup.title else "")
    desc = (soup.find("meta", property="og:description") or {}).get("content") \
        or (soup.find("meta", attrs={"name": "description"}) or {}).get("content") \
        or ""
    pub = (soup.find("meta", property="article:published_time") or {}).get("content")
    if not pub:
        t = soup.find("time")
        if t and t.has_attr("datetime"):
            pub = t["datetime"]
    return clean(title), clean(desc), pub

def make_item(url: str, html_text: str):
    title, summary, pub = extract_meta_from_html(html_text)
    return {
        "id": hashlib.md5(url.encode()).hexdigest(),
        "title": title or url,
        "link": url,
        "summary": summary,
        "publishedAt": pub,            # 可能是 ISO8601 或缺失
        "source": "SBS 中文（繁體）",
        "fetchedAt": iso_now(),
    }

# ---- A) Sitemap：放寬到 /language/chinese/，再用 zh-Hant 檢查 ----
def parse_sitemap_urls(xml_text: str) -> list:
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    try:
        root = ET.fromstring(xml_text)
        for loc in root.findall(".//sm:url/sm:loc", ns):
            urls.append(loc.text.strip())
        for loc in root.findall(".//sm:sitemap/sm:loc", ns):
            urls.append(loc.text.strip())
    except ET.ParseError:
        urls = [m.group(1) for m in re.finditer(r"<loc>\s*(.*?)\s*</loc>", xml_text)]
    return urls

def collect_from_sitemap() -> list:
    try:
        idx = fetch(SITEMAP_INDEX).text
    except Exception as e:
        print(f"[WARN] fetch sitemap index fail: {e}", file=sys.stderr)
        return []

    first = parse_sitemap_urls(idx)
    out = []
    for sm in first:
        if not sm.lower().endswith(".xml"):
            continue
        try:
            xml = fetch(sm).text
            urls = parse_sitemap_urls(xml)
            for u in urls:
                if "/language/chinese/" in u and "/article/" in u:
                    out.append(u)
        except Exception as e:
            print(f"[WARN] sitemap fail {sm}: {e}", file=sys.stderr)
            continue
        if len(out) >= 5 * MAX_ITEMS:
            break

    # 去重
    seen = set(); uniq = []
    for u in out:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ---- B) 入口頁直抓：抽 /article/ link ----
def collect_from_entrypages() -> list:
    links = []
    for page in ENTRY_PAGES:
        try:
            html_text = fetch(page).text
            soup = BeautifulSoup(html_text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("/"):
                    href = "https://www.sbs.com.au" + href
                if "/language/chinese/" in href and "/article/" in href:
                    links.append(href)
        except Exception as e:
            print(f"[WARN] entry scrape fail {page}: {e}", file=sys.stderr)
            continue
    # 去重
    seen = set(); uniq = []
    for u in links:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ---- C) Google News fallback：解 redirect → 取原文 URL → 檢查 zh-Hant ----
def extract_sbs_url_from_text(text: str) -> str or None:
    if not text:
        return None
    text = html.unescape(text)
    for m in re.finditer(r'https?://[^\s\'">]+', text):
        u = m.group(0)
        if SBS_HOST in u:
            return u
    return None

def decode_gn_item_to_article_url(link_text: str, guid_text: str or None, desc_html: str or None) -> str or None:
    # 1) <link> 直接係 SBS
    if link_text and SBS_HOST in link_text:
        return link_text.strip()
    # 2) <guid>
    if guid_text and SBS_HOST in guid_text:
        return guid_text.strip()
    # 3) <description> 內的真 URL
    url_from_desc = extract_sbs_url_from_text(desc_html or "")
    if url_from_desc:
        return url_from_desc
    # 4) news.google.com redirect：從 query(u/url/q) 解
    if link_text and "news.google.com" in link_text:
        try:
            p = urlparse(link_text)
            qs = parse_qs(p.query)
            for key in ("u", "url", "q"):
                if key in qs and qs[key]:
                    candidate = unquote(qs[key][0])
                    if SBS_HOST in candidate:
                        return candidate
        except Exception:
            pass
    return None

def collect_from_google_news() -> list:
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
            # 只收中文「文字文章」；排除 podcast
            if ("/language/chinese/" in real and
                "/article/" in real and
                "/podcast-episode/" not in real):
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

# ---- 輸出 ----
def json_out(items, path):
    payload = {
        "source": "SBS 中文（繁體）",
        "generatedAt": iso_now(),
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
    fg.title("SBS 中文（繁體）新聞（非官方聚合）")
    fg.link(href="https://www.sbs.com.au/language/chinese/zh-hant/", rel='alternate')
    fg.description("Auto-generated (headings & summaries only).")
    fg.language("zh-hant")
    for it in items:
        fe = fg.add_entry()
        fe.id(it["id"])
        fe.title(it["title"])
        fe.link(href=it["link"])
        fe.description(it["summary"] or it["title"])
        # pubDate 可能是 ISO8601 或 RFC822，feedgen處理不了就跳過
    fg.rss_file(path)

# ---- 主程式 ----
if __name__ == "__main__":
    articles = []

    # 1) Sitemap
    urls = []
    try:
        urls = collect_from_sitemap()
        print(f"[INFO] sitemap urls: {len(urls)}", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] sitemap step fail: {e}", file=sys.stderr)

    # 2) 入口頁直抓（補充）
    entry_urls = []
    try:
        entry_urls = collect_from_entrypages()
        print(f"[INFO] entry page urls: {len(entry_urls)}", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] entry step fail: {e}", file=sys.stderr)

    # 合併去重
    merged = []
    seen = set()
    for u in urls + entry_urls:
        if u not in seen:
            seen.add(u); merged.append(u)

    # 逐篇抓 HTML，保留 zh-Hant
    for u in merged:
        try:
            html_text = fetch(u).text
            if not is_zh_hant_page(html_text):
                continue
            articles.append(make_item(u, html_text))
            if len(articles) >= MAX_ITEMS:
                break
        except Exception as e:
            print(f"[WARN] fetch article fail {u}: {e}", file=sys.stderr)

    # 3) 若仍空 → Google News fallback（解 redirect → 檢查 zh-Hant → 抽 meta）
    if not articles:
        print("[INFO] no items from sitemap/entry; fallback Google News", file=sys.stderr)
        gn_urls = collect_from_google_news()
        for u in gn_urls:
            try:
                html_text = fetch(u).text
                if not is_zh_hant_page(html_text):
                    continue
                articles.append(make_item(u, html_text))
                if len(articles) >= MAX_ITEMS:
                    break
            except Exception as e:
                print(f"[WARN] GN article fetch fail {u}: {e}", file=sys.stderr)

    # 4) 輸出到 repo root（配合 Pages: root）
    json_out(articles, "sbs_zh_hant.json")
    rss_out(articles,  "sbs_zh_hant.xml")
    print(f"[DONE] output {len(articles)} items", file=sys.stderr)
