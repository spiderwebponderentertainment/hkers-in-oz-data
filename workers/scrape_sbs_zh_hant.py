import json, re, hashlib, requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from feedgen.feed import FeedGenerator

CANDIDATE_LIST_URLS = [
    "https://www.sbs.com.au/language/chinese/zh-hant/topic/news",  # 優先用：新聞專頁
    "https://www.sbs.com.au/language/chinese/zh-hant",             # 後備：入口頁
]
HEADERS = {"User-Agent": "HKersInOZBot/1.0"}

def clean(s): return re.sub(r"\s+", " ", s or "").strip()

def iso_now(): return datetime.now(timezone.utc).isoformat()

def fetch_latest():
    r = requests.get(LIST_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    items = []
    for a in soup.select("article"):
        title_tag = a.select_one("h2 a, h3 a")
        if not title_tag: continue
        title = clean(title_tag.get_text())
        href = title_tag.get("href", "")
        if href.startswith("/"):
            href = "https://www.sbs.com.au" + href

        summary_tag = a.select_one("p")
        summary = clean(summary_tag.get_text()) if summary_tag else ""

        time_tag = a.find("time")
        pub = time_tag.get("datetime") if time_tag else None

        items.append({
            "id": hashlib.md5(href.encode()).hexdigest(),
            "title": title,
            "link": href,
            "summary": summary,
            "publishedAt": pub,
            "source": "SBS 中文（繁體）",
            "fetchedAt": iso_now()
        })
    return items[:30]

def save_json(items, path):
    payload = {"source": "SBS 中文（繁體）", "generatedAt": iso_now(), "count": len(items), "items": items}
    with open(path, "w", encoding="utf-8") as f: json.dump(payload, f, ensure_ascii=False, indent=2)

def save_rss(items, path):
    fg = FeedGenerator()
    fg.title("SBS 中文（繁體）新聞（非官方聚合）")
    fg.link(href=LIST_URL, rel='alternate')
    fg.description("Auto-generated feed")
    fg.language("zh-hant")
    for it in items:
        fe = fg.add_entry()
        fe.id(it["id"])
        fe.title(it["title"])
        fe.link(href=it["link"])
        fe.description(it["summary"])
        if it.get("publishedAt"):
            try: fe.pubDate(datetime.fromisoformat(it["publishedAt"].replace("Z","+00:00")))
            except: pass
    fg.rss_file(path)

if __name__ == "__main__":
    items = fetch_latest()
    save_json(items, "../public/sbs_zh_hant.json")
    save_rss(items, "../public/sbs_zh_hant.xml")
