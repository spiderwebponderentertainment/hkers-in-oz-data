# workers/scrape_sbs_zh_hant.py
import json, re, hashlib, sys
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

HEADERS = {"User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"}

SITEMAP_INDEX = "https://www.sbs.com.au/sitemap.xml"
# 放寬成中文語言路徑；有些文章可能是 zh-hant，也可能是其他中文路徑再帶 hreflang
LANG_MATCH = "/language/chinese/"
MAX_ITEMS = 30

def iso_now(): return datetime.now(timezone.utc).isoformat()
def clean(s: str) -> str: return re.sub(r"\s+", " ", (s or "")).strip()

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r

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
    # 只展開 content 類 sitemap（名稱可能含有 news/article/content）
    candidates = [u for u in first if "sitemap" in u]
    out = []
    for sm in candidates:
        try:
            xml = fetch(sm).text
            urls = parse_sitemap_urls(xml)
            for u in urls:
                # 盡量濾到繁中文章（含 chinese 路徑；並優先含 /article/）
                if LANG_MATCH in u and ("/article/" in u or "/news/" in u):
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

# 後備：Google News（繁體 + AU），以 site: 限定只收 sbs 中文文章
GN_URL = (
    "https://news.google.com/rss/search"
    "?q=site:sbs.com.au/language/chinese/zh-hant"
    "&hl=zh-HK&gl=AU&ceid=AU:zh-Hant"
)

def collect_from_google_news() -> list[dict]:
    try:
        xml = fetch(GN_URL).text
    except Exception as e:
        print(f"[WARN] google news fetch fail: {e}", file=sys.stderr)
        return []

    items = []
    try:
        root = ET.fromstring(xml)
        for it in root.findall(".//item"):
            title = clean((it.findtext("title") or ""))
            link = (it.findtext("link") or "").strip()
            desc = clean((it.findtext("description") or ""))
            pub = it.findtext("pubDate") or None
            if not link:
                continue
            items.append({
                "id": hashlib.md5(link.encode()).hexdigest(),
                "title": title or link,
                "link": link,
                "summary": desc,
                "publishedAt": pub,  # 這裡是 RFC822；App 端可顯示原字串，或自行轉換
                "source": "SBS 中文（繁體）",
                "fetchedAt": iso_now(),
            })
    except Exception as e:
        print(f"[WARN] parse google news rss fail: {e}", file=sys.stderr)
        return []
    return items

def extract_meta(url: str):
    try:
        html = fetch(url).text
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
    except Exception as e:
        print(f"[WARN] fetch meta fail {url}: {e}", file=sys.stderr)
        return "", "", None

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
    fg.link(href="https://www.sbs.com.au/language/chinese/zh-hant/topic/news", rel='alternate')
    fg.description("Auto-generated (headings & summaries only).")
    fg.language("zh-hant")
    for it in items:
        fe = fg.add_entry()
        fe.id(it["id"])
        fe.title(it["title"])
        fe.link(href=it["link"])
        fe.description(it["summary"] or it["title"])
        # pubDate 可為 ISO8601 或 RFC822，這裡不強轉，讓 feedgen處理不了時跳過
    fg.rss_file(path)

if __name__ == "__main__":
    # A) 先試 sitemap
    urls = collect_from_sitemap()
    articles = []
    if urls:
        print(f"[INFO] sitemap got {len(urls)} urls, fetching metas…", file=sys.stderr)
        for u in urls:
            title, summary, pub = extract_meta(u)
            articles.append({
                "id": hashlib.md5(u.encode()).hexdigest(),
                "title": title or u,
                "link": u,
                "summary": summary,
                "publishedAt": pub,
                "source": "SBS 中文（繁體）",
                "fetchedAt": iso_now(),
            })
            if len(articles) >= MAX_ITEMS:
                break
    # B) 如果 sitemap 仍然空，fallback 到 Google News RSS
    if not articles:
        print("[INFO] sitemap empty → fallback Google News RSS", file=sys.stderr)
        articles = collect_from_google_news()[:MAX_ITEMS]

    # 輸出到 repo root（Pages: root）
    json_out(articles, "sbs_zh_hant.json")
    rss_out(articles,  "sbs_zh_hant.xml")
    print(f"[DONE] output {len(articles)} items", file=sys.stderr)

