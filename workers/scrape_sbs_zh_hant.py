# workers/scrape_sbs_zh_hant.py
import json, re, hashlib, sys, time
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

HEADERS = {
    "User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"
}

SITEMAP_INDEX = "https://www.sbs.com.au/sitemap.xml"
LANG_SUBSTR = "/language/chinese/zh-hant/"

MAX_URLS = 60     # 從sitemap拉幾多條去處理
MAX_ITEMS = 30    # 最終輸出幾多條

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r

def parse_sitemap_urls(xml_text: str) -> list[str]:
    # 支援 sitemap index 及 urlset
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        # 某些伺服器可能冇 namespace，fallback 粗暴匹配
        return re.findall(r"<loc>(.*?)</loc>", xml_text)
    urls = []
    # urlset
    for loc in root.findall(".//sm:url/sm:loc", ns):
        urls.append(loc.text.strip())
    # sitemapindex
    for loc in root.findall(".//sm:sitemap/sm:loc", ns):
        urls.append(loc.text.strip())
    if not urls:
        # 無namespace fallback
        urls = [m.group(1) for m in re.finditer(r"<loc>\s*(.*?)\s*</loc>", xml_text)]
    return urls

def collect_zh_hant_urls() -> list[str]:
    # 1) 打開總 sitemap
    idx = fetch(SITEMAP_INDEX).text
    first_level = parse_sitemap_urls(idx)

    # 2) 展開子 sitemap（只揀可能含 content 的）
    candidate_sitemaps = [u for u in first_level if "sitemap" in u]
    all_urls = []

    for sm_url in candidate_sitemaps:
        try:
            xml = fetch(sm_url).text
            urls = parse_sitemap_urls(xml)
            # 只保留含繁中路徑的文章 URL
            for u in urls:
                if LANG_SUBSTR in u and "/article/" in u:
                    all_urls.append(u)
        except Exception as e:
            print(f"[WARN] sitemap fail {sm_url}: {e}", file=sys.stderr)
            continue
        if len(all_urls) >= MAX_URLS:
            break

    # 去重、由新到舊（簡單以字串/出現順序）
    uniq = list(dict.fromkeys(all_urls))
    return uniq[:MAX_URLS]

def extract_meta(url: str):
    """抓單篇文章：title / description / pubDate（盡量用SSR meta）"""
    try:
        html = fetch(url).text
        soup = BeautifulSoup(html, "html.parser")

        # title：優先 og:title
        title = (soup.find("meta", property="og:title") or {}).get("content") \
            or (soup.title.string if soup.title else "")
        title = clean(title)

        # description：優先 og:description → name=description
        desc = (soup.find("meta", property="og:description") or {}).get("content") \
            or (soup.find("meta", attrs={"name": "description"}) or {}).get("content") \
            or ""
        desc = clean(desc)

        # 發佈時間：article:published_time / time[datetime]
        pub = (soup.find("meta", property="article:published_time") or {}).get("content")
        if not pub:
            t = soup.find("time")
            if t and t.has_attr("datetime"):
                pub = t["datetime"]

        return title, desc, pub
    except Exception as e:
        print(f"[WARN] extract fail {url}: {e}", file=sys.stderr)
        return "", "", None

def make_item(url: str):
    title, summary, pub = extract_meta(url)
    return {
        "id": hashlib.md5(url.encode()).hexdigest(),
        "title": title or url,
        "link": url,
        "summary": summary,
        "publishedAt": pub,
        "source": "SBS 中文（繁體）",
        "fetchedAt": iso_now(),
    }

def save_json(items, path):
    payload = {
        "source": "SBS 中文（繁體）",
        "generatedAt": iso_now(),
        "count": len(items),
        "items": items
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def save_rss(items, path):
    try:
        from feedgen.feed import FeedGenerator
    except Exception as e:
        print("[WARN] feedgen not available, skip XML:", e, file=sys.stderr)
        return

    fg = FeedGenerator()
    fg.title("SBS 中文（繁體）新聞（非官方聚合）")
    fg.link(href="https://www.sbs.com.au/language/chinese/zh-hant/topic/news", rel='alternate')
    fg.description("Auto-generated from sitemap (headings & summaries only).")
    fg.language("zh-hant")

    for it in items:
        fe = fg.add_entry()
        fe.id(it["id"])
        fe.title(it["title"])
        fe.link(href=it["link"])
        fe.description(it["summary"] or it["title"])
        if it.get("publishedAt"):
            try:
                iso = it["publishedAt"].replace("Z", "+00:00")
                fe.pubDate(datetime.fromisoformat(iso))
            except Exception:
                pass

    fg.rss_file(path)

if __name__ == "__main__":
    urls = collect_zh_hant_urls()
    print(f"[OK] collected {len(urls)} zh-hant article urls from sitemap", file=sys.stderr)
    items = []
    for u in urls:
        items.append(make_item(u))
        if len(items) >= MAX_ITEMS:
            break

    # 輸出到 repo root（你用 root 作 GitHub Pages）
    save_json(items, "sbs_zh_hant.json")
    save_rss(items,  "sbs_zh_hant.xml")
    print(f"[DONE] wrote sbs_zh_hant.json / sbs_zh_hant.xml", file=sys.stderr)
