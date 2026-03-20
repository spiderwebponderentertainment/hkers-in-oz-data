from curl_cffi import requests
import json
import sys
import re
from datetime import datetime, timezone
import html
import time

# ---------------- 設定 ----------------
WP_API_BASE = "https://pulsehknews.com/wp-json/wp/v2"

TOPICS = {
    "london_eto": {
        "slug": "londonetotrial",
        "output_file": "topic_london_eto.json",
        "display_name": "倫敦經貿辦案"
    },
    "taipo_fire": {
        "slug": "taipofire",
        "output_file": "topic_taipo_fire.json",
        "display_name": "宏福苑大火"
    }
}

# ---------------- 工具函數 ----------------
def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    clean_text = re.sub(r'<[^>]+>', '', raw_html)
    return html.unescape(clean_text).strip()

def fetch_category_id(session, slug: str) -> int:
    url = f"{WP_API_BASE}/categories?slug={slug}"
    r = session.get(url, timeout=15)
    
    if r.status_code != 200:
        raise Exception(f"API 拒絕連線，Status Code: {r.status_code}")
        
    data = r.json()
    if not data:
        raise ValueError(f"找不到 slug 為 '{slug}' 的分類！")
    return data[0]["id"]

def fetch_all_posts_by_category(session, category_id: int) -> list:
    all_posts = []
    page = 1
    while True:
        url = f"{WP_API_BASE}/posts?categories={category_id}&per_page=100&page={page}&_embed=1"
        print(f"  Fetching page {page} for category {category_id}...")
        
        # 加入重試機制
        max_retries = 3
        for attempt in range(max_retries):
            r = session.get(url, timeout=20)
            if r.status_code == 200:
                break
            elif r.status_code == 400 and "rest_post_invalid_page_number" in r.text:
                return all_posts # 已經揭到最尾一頁
            else:
                print(f"    [警告] 頁面 {page} 讀取失敗 (Status {r.status_code})，準備重試...")
                time.sleep(3)
        else:
            raise Exception(f"無法讀取第 {page} 頁，已放棄。")
            
        posts = r.json()
        if not posts:
            break
            
        all_posts.extend(posts)
        
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
            
        page += 1
        time.sleep(2) # 翻頁之間停 2 秒
        
    return all_posts

def process_post(post: dict, source_name: str) -> dict:
    title = clean_html(post.get("title", {}).get("rendered", ""))
    excerpt = clean_html(post.get("excerpt", {}).get("rendered", ""))
    link = post.get("link", "")
    
    pub_date = post.get("date_gmt", "")
    if pub_date:
        pub_date = pub_date + "Z"

    return {
        "id": str(post.get("id")),
        "title": title,
        "link": link,
        "summary": excerpt,
        "publishedAt": pub_date,
        "source": source_name,
        "sourceCategory": "專題報導"
    }

# ---------------- 主程式 ----------------
def main():
    now_utc = datetime.now(timezone.utc).isoformat()
    
    # ✅ 關鍵：啟動完美偽裝 Chrome 120 的 Session
    session = requests.Session(impersonate="chrome120")
    
    for topic_key, config in TOPICS.items():
        print(f"\n[INFO] 開始處理專題: {config['display_name']}")
        try:
            cat_id = fetch_category_id(session, config["slug"])
            print(f"  取得 Category ID: {cat_id}")
            
            time.sleep(2)
            
            raw_posts = fetch_all_posts_by_category(session, cat_id)
            print(f"  共抓取了 {len(raw_posts)} 篇文章")
            
            processed_items = [process_post(p, "追新聞") for p in raw_posts]
            processed_items.sort(key=lambda x: x["publishedAt"], reverse=True)
            
            payload = {
                "topic": config["display_name"],
                "generatedAt": now_utc,
                "count": len(processed_items),
                "items": processed_items
            }
            
            with open(config["output_file"], "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                
            print(f"[DONE] 成功輸出 {config['output_file']}")
            
        except Exception as e:
            print(f"[ERROR] 處理專題 {config['display_name']} 時發生錯誤: {e}", file=sys.stderr)
            
        time.sleep(3)

if __name__ == "__main__":
    main()
