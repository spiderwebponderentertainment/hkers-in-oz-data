# workers/scrape_sbs_zh_hant.py
import json, re, hashlib, requests, sys
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from feedgen.feed import FeedGenerator

# 1) 主要 & 後備列表頁（SBS 可能會改路徑）
CANDIDATE_LIST_URLS = [
    "https://www.sbs.com.au/language/chinese/zh-hant/topic/news",  # 新聞列表（首選）
    "https://www.sbs.com.au/language/chinese/zh-hant",             # 中文入口（後備）
]

HEADERS = {
    "User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"
}

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def fetch_latest():
    last_err = None
    for url in CANDIDATE_LIST_URLS:
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            # 2) 兼容多種卡片樣式
            cards = soup.select("article") or soup.select("[data-testid='Card']") or []
            items = []

            for a in cards:
                # 連結 / 標題
                link_tag = (
                    a.select_one("h2 a, h3 a, a[data-testid='CardLink']")
                    or a.find("a", href=True)
                )
                if not link_tag:
                    continue

                title = clean(link_tag.get_text())
                href = link_tag.get("href", "")
                if not href:
                    continue
                if href.startswith("/"):
                    href = "https://www.sbs.com.au" + href

                # 摘要（可缺）
                summary_tag = a.select_one("p, div[data-testid='CardDescription']") or a.find("p")
                summary = clean(summary_tag.get_text()) if summary_tag else ""

                # 發佈時間（可缺）
                time_tag = a.find("time")
                pub = time_tag.get("datetime") if (time_tag and time_tag.has_attr("datetime")) else None

                items.append({
                    "id": hashlib.md5(href.encode()).hexdigest(),
                    "title": title,
                    "link": href,
                    "summary": summary,
                    "publishedAt": pub,
                    "source": "SBS 中文（繁體）",
                    "fetchedAt": iso_now(),
                })

            if items:
                print(f"[OK] {url} → {len(items)} items", file=sys.stderr)
                return items[:30]

            print(f"[WARN] {url} 解析到 0 條，轉試後備 URL", file=sys.stderr)

        except Exception as e:
            last_err = e
            print(f"[ERR] {url}: {e}", file=sys.stderr)
            continue

    raise last_err or RuntimeError("兩個候選 URL 都失敗，無法取得新聞")

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
    fg = FeedGenerator()
    fg.title("SBS 中文（繁體）新聞（非官方聚合）")
    fg.link(href=CANDIDATE_LIST_URLS[0], rel='alternate')
    fg.description("Auto-generated headings & summaries only.")
    fg.language("zh-hant")

    for it in items:
        fe = fg.add_entry()
        fe.id(it["id"])
        fe.title(it["title"])
        fe.link(href=it["link"])
        fe.description(it["summary"] or it["title"])
        # pubDate 可能為 ISO8601，亦可能缺失
        if it.get("publishedAt"):
            try:
                iso = it["publishedAt"].replace("Z", "+00:00")
                fe.pubDate(datetime.fromisoformat(iso))
            except Exception:
                pass

    fg.rss_file(path)

if __name__ == "__main__":
    items = fetch_latest()
    # 3) 輸出到「repo 根目錄」（因你用 root 做 GitHub Pages）
    save_json(items, "sbs_zh_hant.json")
    save_rss(items,  "sbs_zh_hant.xml")
    print(f"[DONE] wrote sbs_zh_hant.json / sbs_zh_hant.xml at repo root", file=sys.stderr)

