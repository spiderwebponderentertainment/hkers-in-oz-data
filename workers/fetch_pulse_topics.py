import requests
import json
import sys
import os
import re
import html
from datetime import datetime, timezone

# ---------------- 設定 ----------------
# 追新聞的專題 RSS Feed 網址
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

# ---------------- 工具函數 ----------------
def clean_html(raw_html: str) -> str:
    """清除 HTML 標籤並解碼 Entities"""
    if not raw_html:
        return ""
    clean_text = re.sub(r'<[^>]+>', '', raw_html)
    return html.unescape(clean_text).strip()

def fetch_via_rss2json(rss_url: str) -> list:
    """透過合法的 rss2json 服務，繞過 Cloudflare 直接獲取 JSON"""
    # rss2json 嘅免費 API
    api_url = f"https://api.rss2json.com/v1/api.json?rss_url={rss_url}"
    print(f"  呼叫 rss2json 服務中...")
    
    try:
        r = requests.get(api_url, timeout=20)
        data = r.json()
        
        if data.get("status") != "ok":
            print(f"    [失敗] rss2json 無法讀取: {data.get('message')}")
            return []
            
        items = []
        for item in data.get("items", []):
            pub_date_str = item.get("pubDate", "") # 格式: "YYYY-MM-DD HH:MM:SS"
            
            # 將日期轉為標準的 ISO 8601 格式
            try:
                dt = datetime.strptime(pub_date_str, "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
                pub_date_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except:
                pub_date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            items.append({
                "id": item.get("link"),
                "title": clean_html(item.get("title", "")),
                "link": item.get("link"),
                "summary": clean_html(item.get("description", "")),
                "publishedAt": pub_date_iso,
                "source": "追新聞",
                "sourceCategory": "專題報導"
            })
            
        return items
        
    except Exception as e:
        print(f"    [警告] 呼叫 API 時發生錯誤: {e}")
        return []

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
            # 1. 透過 rss2json 獲取最新新聞
            new_items = fetch_via_rss2json(config["rss_url"])
            print(f"  成功從網站抓取了 {len(new_items)} 篇新聞")
            
            # 如果無新聞，跳過儲存步驟
            if not new_items:
                print("  [放棄] 無法取得新文章，保留原有檔案。")
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
