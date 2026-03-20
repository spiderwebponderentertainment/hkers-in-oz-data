import requests
import json
import sys
import re
from datetime import datetime, timezone
import html

# ---------------- 設定 ----------------
HEADERS = {
    "User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"
}

# 追新聞的 WordPress API 基底
WP_API_BASE = "https://pulsehknews.com/wp-json/wp/v2"

# 定義專題與對應的 Category Slug 或 ID
# 經過測試，Pulse HK News 使用 Categories 黎做專題
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
    """清除 HTML 標籤並解碼 Entities"""
    if not raw_html:
        return ""
    clean_text = re.sub(r'<[^>]+>', '', raw_html)
    return html.unescape(clean_text).strip()

def fetch_category_id(slug: str) -> int:
    """透過 slug 取得對應的 Category ID"""
    url = f"{WP_API_BASE}/categories?slug={slug}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError(f"找不到 slug 為 '{slug}' 的分類！")
    return data[0]["id"]

def fetch_all_posts_by_category(category_id: int) -> list:
    """利用 Pagination 抓取該分類下的所有文章"""
    all_posts = []
    page = 1
    while True:
        url = f"{WP_API_BASE}/posts?categories={category_id}&per_page=100&page={page}&_embed=1"
        print(f"  Fetching page {page} for category {category_id}...")
        r = requests.get(url, headers=HEADERS)
        
        # 處理超出頁數的情況
        if r.status_code == 400 and "rest_post_invalid_page_number" in r.text:
            break
        r.raise_for_status()
        
        posts = r.json()
        if not posts:
            break
            
        all_posts.extend(posts)
        
        # 檢查 Header 中的總頁數
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
        page += 1
        
    return all_posts

def process_post(post: dict, source_name: str) -> dict:
    """將 WordPress Post 格式轉換為 App 所需的 JSON 格式"""
    title = clean_html(post.get("title", {}).get("rendered", ""))
    excerpt = clean_html(post.get("excerpt", {}).get("rendered", ""))
    link = post.get("link", "")
    
    # 處理時間 (WordPress 預設提供 ISO 格式字串，但沒有 Z)
    pub_date = post.get("date_gmt", "")
    if pub_date:
        pub_date = pub_date + "Z" # 強制轉為 UTC 標準

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
    
    for topic_key, config in TOPICS.items():
        print(f"\n[INFO] 開始處理專題: {config['display_name']}")
        try:
            # 1. 取得 Category ID
            cat_id = fetch_category_id(config["slug"])
            print(f"  取得 Category ID: {cat_id}")
            
            # 2. 抓取所有文章
            raw_posts = fetch_all_posts_by_category(cat_id)
            print(f"  共抓取了 {len(raw_posts)} 篇文章")
            
            # 3. 格式化資料
            processed_items = []
            for post in raw_posts:
                processed_items.append(process_post(post, "追新聞"))
                
            # 4. 按日期排序 (舊到新，適合 Timeline，或新到舊)
            # 這裡預設為新到舊 (reverse=True)
            processed_items.sort(key=lambda x: x["publishedAt"], reverse=True)
            
            # 5. 封裝並輸出
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

if __name__ == "__main__":
    main()
