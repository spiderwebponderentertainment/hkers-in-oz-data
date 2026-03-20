import cloudscraper
from bs4 import BeautifulSoup
import json
import sys
import os
import time
from datetime import datetime, timezone

# ---------------- 設定 ----------------
# 今次我哋直接讀取你喺瀏覽器見到嘅網頁，唔再依賴 API 或 RSS
TOPICS = {
    "london_eto": {
        "url": "https://pulsehknews.com/c/londonetotrial/",
        "output_file": "topic_london_eto.json",
        "display_name": "倫敦經貿辦案"
    },
    "taipo_fire": {
        "url": "https://pulsehknews.com/c/taipofire/",
        "output_file": "topic_taipo_fire.json",
        "display_name": "宏福苑大火"
    }
}

# ---------------- 工具函數 ----------------
def fetch_html_and_parse(url: str) -> list:
    """使用 cloudscraper 突破 Cloudflare，並用 BeautifulSoup 拆解 HTML"""
    print(f"  正在模擬人類瀏覽器載入網頁: {url}")
    
    # 建立一個突破 Cloudflare 嘅 Scraper (模擬 Chrome)
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
    
    try:
        r = scraper.get(url, timeout=20)
        if r.status_code != 200:
            print(f"    [失敗] 網頁回傳代碼: {r.status_code}")
            return []
            
        # 開始用 BeautifulSoup 拆解網頁 HTML
        soup = BeautifulSoup(r.text, 'html.parser')
        items = []
        
        # 尋找所有文章區塊 (支援大部分 WordPress Theme 嘅寫法)
        articles = soup.find_all('article')
        if not articles:
            # 如果無 <article> tag，就搵帶有 post 類別嘅 div
            articles = soup.find_all('div', class_=lambda c: c and ('post' in c or 'item' in c))
            
        print(f"    [分析] 喺網頁表面搵到 {len(articles)} 個文章區塊。")
        
        for art in articles:
            # 1. 搵標題同 Link (通常喺 h2, h3 入面嘅 <a>)
            title_elem = art.find(['h2', 'h3', 'h4'])
            if not title_elem:
                continue
                
            a_tag = title_elem.find('a')
            if not a_tag:
                a_tag = title_elem if title_elem.name == 'a' else None
                if not a_tag:
                    continue
                    
            title = a_tag.get_text(strip=True)
            link = a_tag.get('href')
            
            # 2. 搵時間
            time_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            time_elem = art.find('time')
            if time_elem and time_elem.get('datetime'):
                time_iso = time_elem.get('datetime')
                
            # 3. 搵簡介
            summary = ""
            excerpt_elem = art.find(class_=lambda c: c and ('excerpt' in c or 'summary' in c))
            if excerpt_elem:
                summary = excerpt_elem.get_text(strip=True)
                
            # 確認有標題同 Link 先加入清單
            if title and link:
                items.append({
                    "id": link,
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "publishedAt": time_iso,
                    "source": "追光者",
                    "sourceCategory": "專題報導"
                })
                
        return items
        
    except Exception as e:
        print(f"    [錯誤] 抓取 HTML 時發生錯誤: {e}")
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
            # 1. 視覺化刮網頁
            new_items = fetch_html_and_parse(config["url"])
            print(f"  成功從網頁刮到 {len(new_items)} 篇新聞")
            
            if not new_items:
                print("  [放棄] 無法取得新文章，保留原有檔案。")
                continue
                
            # 2. 讀取舊有歷史新聞
            existing_items = load_existing_json(config["output_file"])
            print(f"  從本地資料庫載入 {len(existing_items)} 篇歷史紀錄")
            
            # 3. 合併並去除重複
            merged_dict = {item["link"]: item for item in existing_items}
            for item in new_items:
                merged_dict[item["link"]] = item
                
            final_items = list(merged_dict.values())
            
            # 4. 排序
            final_items.sort(key=lambda x: x["publishedAt"], reverse=True)
            print(f"  合併後總共有 {len(final_items)} 篇新聞")
            
            # 5. 輸出
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
            
        # 專題之間停 3 秒，免得太密俾人 Block
        time.sleep(3)

if __name__ == "__main__":
    main()
