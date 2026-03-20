import requests
import xml.etree.ElementTree as ET
import json
import sys
import os
import re
import html
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# ---------------- 設定 ----------------
# 追新聞的專題 RSS Feed 網址 (WordPress 預設結構)
TOPICS = {
    "london_eto": {
        "rss_url": "https://pulsehknews.com/c/londonetotrial/feed/",
        "output_file": "topic_london_eto.json",
        "display_name": "倫敦經貿辦案"
    },
    "taipo_fire": {
        "rss_url": "https://pulsehknews.com/c/taipofire/feed/",
        "output_file": "topic_taipo_fire.json",
        "display_name": "宏福苑大火"
    }
}

# ✅ 偽裝成 Google 官方的 RSS 抓取機械人，令 Cloudflare 放行
HEADERS = {
    "User-Agent": "FeedFetcher-Google; (+http://www.google.com/feedfetcher.html)",
    "Accept": "application/rss+xml, application/xml, text/xml"
}

# ---------------- 工具函數 ----------------
def clean_html(raw_html: str) -> str:
    """清除 HTML 標籤並解碼 Entities"""
    if not raw_html:
        return ""
    clean_text = re.sub(r'<[^>]+>', '', raw_html)
    return html.unescape(clean_text).strip()

def fetch_rss_pages(base_rss_url: str) -> list:
    """抓取 RSS Feed，並嘗試自動翻頁獲取舊文章"""
    all_items = []
    page = 1
    
    while True:
        # WordPress RSS 分頁語法: ?paged=2
        url = f"{base_rss_url}?paged={page}" if page > 1 else base_rss_url
        print(f"  正在讀取 RSS 第 {page} 頁...")
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            # 如果去到無文章嘅頁面，會回傳 404
            if r.status_code != 200:
                break
                
            # 解析 XML
            root = ET.fromstring(r.text)
            items = root.findall(".//item")
            
            if not items:
                break
                
            for item in items:
                title = item.findtext("title") or ""
                link = item.findtext("link") or ""
                desc = item.findtext("description") or ""
                pub_date_str = item.findtext("pubDate") or ""
                
                # 將 RSS 的日期 (RFC 822) 轉為 ISO 8601 (UTC)
                try:
                    dt = parsedate_to_datetime(pub_date_str)
                    pub_date_iso = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                except:
                    pub_date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                
                all_items.append({
                    "id": link,  # 使用網址作為唯一 ID
                    "title": clean_html(title),
                    "link": link,
                    "summary": clean_html(desc),
                    "publishedAt": pub_date_iso,
                    "source": "追新聞",
                    "sourceCategory": "專題報導"
                })
                
            page += 1
            
        except Exception as e:
            print(f"    [警告] 讀取 RSS 時發生錯誤: {e}")
            break
            
    return all_items

def load_existing_json(file_path: str) -> list:
    """讀取 Repo 內現有的 JSON，作為歷史資料庫"""
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
            # 1. 抓取最新 RSS 新聞
            new_items = fetch_rss_pages(config["rss_url"])
            print(f"  成功從網站抓取了 {len(new_items)} 篇新聞")
            
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
