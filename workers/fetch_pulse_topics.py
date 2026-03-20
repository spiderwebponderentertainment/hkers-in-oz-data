import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import sys
import re
from datetime import datetime, timezone
import html
import time

# ---------------- 設定 ----------------
# ✅ 1. 偽裝成真人用嘅 Chrome 瀏覽器
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-HK,zh-TW;q=0.9,zh;q=0.8,en;q=0.7",
    "Referer": "https://pulsehknews.com/"
}

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

# ✅ 2. 建立一個「識得自動重試」的 Session
def get_session():
    session = requests.Session()
    # 如果撞到 429 (Too Many Requests) 或者 50X 錯誤，自動重試 5 次，每次等待時間加倍
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update(HEADERS)
    return session

# ---------------- 工具函數 ----------------
def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    clean_text = re.sub(r'<[^>]+>', '', raw_html)
    return html.unescape(clean_text).strip()

def fetch_category_id(session, slug: str) -> int:
    url = f"{WP_API_BASE}/categories?slug={slug}"
    r = session.get(url, timeout=15)
    r.raise_for_status()
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
        
        r = session.get(url, timeout=20)
        if r.status_code == 400 and "rest_post_invalid_page_number" in r.text:
            break
        r.raise_for_status()
        
        posts = r.json()
        if not posts:
            break
            
        all_posts.extend(posts)
        
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
            
        page += 1
        # ✅ 3. 翻頁之間加少少 Delay，扮真人
        time.sleep(1.5)
        
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
    session = get_session() # 啟動強效 Session
    
    for topic_key, config in TOPICS.items():
        print(f"\n[INFO] 開始處理專題: {config['display_name']}")
        try:
            cat_id = fetch_category_id(session, config["slug"])
            print(f"  取得 Category ID: {cat_id}")
            
            # 專題之間停多陣
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
            
        # 處理完一個專題，抖長啲時間先搞下一個
        time.sleep(3)

if __name__ == "__main__":
    main()
