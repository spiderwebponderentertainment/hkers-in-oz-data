import json, re, hashlib, requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from feedgen.feed import FeedGenerator

CANDIDATE_LIST_URLS = [
    "https://www.sbs.com.au/language/chinese/zh-hant/topic/news",  # 專門新聞
    "https://www.sbs.com.au/language/chinese/zh-hant",             # 後備入口
]

def fetch_latest():
    last_err = None
    for url in CANDIDATE_LIST_URLS:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            cards = soup.select("article") or soup.select("[data-testid='Card']")
            items = []
            for a in cards:
                link_tag = (a.select_one("h2 a, h3 a, a[data-testid='CardLink']")
                            or a.find("a", href=True))
                if not link_tag:
                    continue
                title = clean(link_tag.get_text())
                href = link_tag.get("href", "")
                if href.startswith("/"):
                    href = "https://www.sbs.com.au" + href

                summary_tag = (a.select_one("p, div[data-testid='CardDescription']")
                               or a.find("p"))
                summary = clean(summary_tag.get_text()) if summary_tag else ""

                time_tag = a.find("time")
                pub = time_tag.get("datetime") if time_tag and time_tag.has_attr("datetime") else None

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
                print(f"✅ 成功用 {url} 取得 {len(items)} 條新聞")
                return items[:30]
        except Exception as e:
            print(f"⚠️ 失敗 {url}: {e}")
            last_err = e
            continue
    raise last_err or RuntimeError("兩個 URL 都唔得，無法取得新聞")
