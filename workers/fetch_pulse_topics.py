import requests
import feedparser
import json
import sys
import os
import re
import html
from datetime import datetime, timezone
import time

# ---------------- 設定 ----------------
# 準備多條備用 Feed 路線，防止 WordPress 唔認得 /c/
TOPICS = {
    "london_eto": {
        "rss_urls": [
            "https://pulsehknews.com/c/londonetotrial/feed/",
            "https://pulsehknews.com/category/londonetotrial/feed/",
            "https://pulsehknews.com/feed/?category_name=londonetotrial"
        ],
        "output_file": "topic_london_eto.json",
        "display_name": "倫敦經貿辦案"
    },
    "taipo_fire": {
        "rss_urls": [
            "https://pulsehknews.com/c/taipofire/feed/",
            "https://pulsehknews.com/category/taipofire/feed/",
            "https://pulsehknews.com/feed/?category_name=taipofire"
        ],
        "output_file": "topic_taipo_fire.json",
        "display_name": "宏福苑大火"
    }
}

HEADERS = {
    "User-Agent": "FeedFetcher-Google; (+http://www.google.com/feedfetcher.html)",
    "Accept": "application/rss+xml, application/xml, text/xml"
}

# ---------------- 工具函數 ----------------
def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    clean_text = re.sub(r'<[^>]+>', '', raw_html)
    return html.unescape(clean_text).strip()

def fetch_rss_pages(urls_to_try: list) -> list:
    all_items = []
    
    for url in urls_to_try:
        print(f"  正在嘗試連線: {url}")
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                print(f"    [失敗] 伺服器回傳 {r.status_code}")
                continue
                
            # 使用 feedparser 解析 (自動支援 RSS, Atom 等所有格式)
            feed = feedparser.parse(r.text)
            
            if not feed.entries:
                print(f"    [空檔案] 成功連線，但 XML 入面無文章。")
                # 印出頭 200 個字俾我哋 Debug，睇下伺服器俾咗咩鬼嘢我哋
                print(f"    偵錯資訊: {r.text[:200]}")
                continue
                
            print(f"    [成功] 喺呢條 Link 搵到 {len(feed.entries)} 篇文章！")
            
            for entry in feed.entries:
                title = getattr(entry, "title", "")
                link = getattr(entry, "link", "")
                desc = getattr(entry, "summary", getattr(entry, "description", ""))
                
                # 處理時間格式
                try:
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        dt = datetime.fromtimestamp(time.mktime(entry.published_parsed), timezone.utc)
                    else:
                        dt = datetime.now(timezone.utc)
                    pub_date_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                except:
                    pub_date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                
                all_items.append({
                    "id": link,
                    "title": clean_html(title),
                    "link": link,
                    "summary": clean_html(desc),
                    "publishedAt": pub_date_iso,
                    "source": "追光者",
                    "sourceCategory": "專題報導"
                })
            
            # 只要有一條 Link 成功攞到嘢，就收工，唔洗再試下一條備用 Link
            break 
            
        except Exception as e:
            print(f"    [警告] 讀取時發生錯誤: {e}")
            
    return all_items

def load_existing_json(file_path: str) -> list:
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("items", [])
        except Exception:
            pass
    return []

# ---------------- 主程式 ----------------
def main():
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    for topic_key, config in TOPICS.items():
        print(f"\n[INFO] 開始處理專題: {config['display_name']}")
        try:
            # 1. 抓取最新 RSS 新聞 (會自動試 3 條 Link)
            new_items = fetch_rss_pages(config["rss_urls"])
            
            if not new_items:
                print(f"  [放棄] 試盡所有 Link 都無文章。")
                continue
                
            # 2. 讀取舊有歷史新聞
            existing_items = load_existing_json(config["output_file"])
            print(f"  從本地資料庫載入 {len(existing_items)} 篇歷史紀錄")
            
            # 3. 合併並去除重複 (以 Link 為 Key)
            merged_dict = {item["link"]: item for item in existing_items}
            for item in new_items:
                merged_dict[item["link"]] = item
                
            final_items = list(merged_dict.values())
            
            # 4. 根據日期由新到舊排序
            final_items.sort(key=lambda x: x["publishedAt"], reverse=True)
            print(f"  合併後總共有 {len(final_items)} 篇新聞")
            
            # 5. 輸出存檔
            payload = {
                "topic": config["display_name"],
                "generatedAt": now_utc,
                "count": len(final_items),
                "items": final_items
            }
            
            with open(config["output_file"], "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                
            print(f"[DONE] 成功輸出 {config['output_file']}")
            
        except Exception as e:
            print(f"[ERROR] 處理專題 {config['display_name']} 時發生錯誤: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
