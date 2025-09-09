# workers/scrape_sbs_zh_hant.py
import json, re, hashlib, sys, time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote
import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

HEADERS = {"User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"}

# --- 來源 ---
SITEMAP_INDEX = "https://www.sbs.com.au/sitemap.xml"
ENTRY_PAGES = [
    "https://www.sbs.com.au/language/chinese/zh-hant/",
    "https://www.sbs.com.au/language/chinese/",
]
GN_URL = (
    "https://news.google.com/rss/search"
    "?q=site:sbs.com.au/language/chinese"
    "&hl=zh-HK&gl=AU&ceid=AU:zh-Hant"
)

MAX_ITEMS = 30
TIMEOUT = 25

# -------- 共用工具 --------
def iso_now(): return datetime.now(timezone.utc).isoformat()
def clean(s: str) -> str: return re.sub(r"\s+", " ", (s or "")).strip()

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r

def is_zh_hant_page(html: str) -> bool:
    # 以 <html lang="zh-Hant"> 或 meta 標示判斷繁體
    if re.search(r'<html[^>]+lang=["\']zh-Hant["\']', html, re.I):
        return True
    if re.search(r'property=["\']og:locale["\']\s+content=["\']zh_Hant', html, re.I):
        return True
    return False

def extract_meta_from_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
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

def make_item(url: str, html: str):
    title, summary, pub = extract_meta_from_html(html)
    return {
        "id": hashlib.md5(url.encode()).hexdigest(),
        "title": title or url,
        "link": url,
        "summary": summary,
        "publishedAt": pub,
        "source": "SBS 中文（繁體）",
        "fetchedAt": iso_now(),
    }

# -------- A) Sitemap 抓取（寬鬆 chinese，之後用 zh-Hant 檢查） --------
def parse_sitemap_urls(xml_text: str) -> list[str]:
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    try:
        root = ET.fromstring(xml_text)
        # urlset
        for loc in root.findall(".//sm:url/sm:loc", ns):
            urls.append(loc.text.strip())
        # sitemapindex
        for loc in root.findall(".//sm:sitemap/sm:loc", ns):
            urls.append(loc.text.strip())
    except ET.ParseError:
        urls = [m.group(1) for m in re.finditer(r"<loc>\s*(.*?)\s*</loc>", xml_text)]
    return urls

def collect_from_sitemap() -> list[str]:
    try:
        idx = fetch(SITEMAP_INDEX).text
    except Exception as e:
        print(f"[WARN] fetch sitemap index fail: {e}", file=sys.stderr)
        return []
    first = parse_sitemap_urls(idx)
    # 展開全部子 sitemap（會比較多，但最穩陣）
    out = []
    for sm in first:
        if not sm.lower().endswith(".xml"):  # 非 XML 忽略
            continue
        try:
            xml = fetch(sm).text
            urls = parse_sitemap_urls(xml)
            for u in urls:
                # 放寬到 /language/chinese/；再要求是文章頁
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

# -------- B) 入口頁直抓（拉 /article/ link） --------
def collect_from_entrypages() -> list[str]:
    links = []
    for page in ENTRY_PAGES:
        try:
            html = fetch(page).text
            soup = BeautifulSoup(html, "html.parser")
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

# -------- C) Google News fallback（解 redirect ➜ 追原文 ➜ 檢查繁體） --------
def decode_gn_link(gn_link: str) -> str:
    """
    Google News RSS 的 <link> 有時會係 redirect。
    嘗試從 URL 參數、或直接 requests 跟隨，取最終 sbs.com.au 文章 URL。
    """
    try:
        # 先嘗試直接請求，requests 會跟 redirect
        r = fetch(gn_link)
        final = r.url
        if "sbs.com.au" in final:
            return final
    except Exception:
        pass
    # 從參數解（有時會把原文放在 u= 或 url=）
    try:
        parsed = urlparse(gn_link)
        qs = parse_qs(parsed.query)
        for key in ("u", "url", "q"):
            if key in qs and qs[key]:
                candidate = unquote(qs[key][0])
                if "sbs.com.au" in candidate:
                    return candidate
    except Exception:
        pass
    return gn_link  # 退一步：返回原鏈

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
            link = (it.findtext("link") or "").strip()
            if not link:
                continue
            real = decode_gn_link(link)
            if "sbs.com.au" in real and "/language/chinese/" in real and "/article/" in real:
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

# -------- 輸出 --------
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
        # pubDate 可能係 ISO8601 / RFC822，不強轉，feedgen處理唔到就跳過
    fg.rss_file(path)

# -------- 主程式 --------
if __name__ == "__main__":
    # 1) sitemap
    urls = collect_from_sitemap()
    print(f"[INFO] sitemap urls: {len(urls)}", file=sys.stderr)

    # 2) 入口頁直抓（補充）
    entry_urls = collect_from_entrypages()
    print(f"[INFO] entry page urls: {len(entry_urls)}", file=sys.stderr)

    # 合併去重
    merged = []
    seen = set()
    for u in urls + entry_urls:
        if u not in seen:
            seen.add(u); merged.append(u)

    # 取前若干，逐篇抓 HTML，**只保留 zh-Hant 頁面**
    articles = []
    for u in merged:
        try:
            html = fetch(u).text
            if not is_zh_hant_page(html):
                continue
            articles.append(make_item(u, html))
            if len(articles) >= MAX_ITEMS:
                break
        except Exception as e:
            print(f"[WARN] fetch article fail {u}: {e}", file=sys.stderr)
            continue

    # 3) 如果仲係空，fallback Google News（解 redirect ➜ 檢查 zh-Hant）
    if not articles:
        print("[INFO] no articles from sitemap/entry; fallback Google News", file=sys.stderr)
        gn_urls = collect_from_google_news()
        for u in gn_urls:
            try:
                html = fetch(u).text
                if not is_zh_hant_page(html):
                    continue
                articles.append(make_item(u, html))
                if len(articles) >= MAX_ITEMS:
                    break
            except Exception as e:
                print(f"[WARN] GN article fetch fail {u}: {e}", file=sys.stderr)
                continue

    # 輸出到 repo root（你用 root 作 GitHub Pages）
    json_out(articles, "sbs_zh_hant.json")
    rss_out(articles,  "sbs_zh_hant.xml")
    print(f"[DONE] output {len(articles)} items", file=sys.stderr)
