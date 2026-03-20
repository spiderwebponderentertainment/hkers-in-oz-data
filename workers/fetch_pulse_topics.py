import requests
import json
import sys
import re
from datetime import datetime, timezone
import html
import time
from urllib.parse import quote

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

# ---------------- 核心：Proxy 隱身工具 ----------------
def fetch_via_proxy(target_url: str):
    """利用免費的中間人 Proxy 去繞過 GitHub IP 封鎖"""
    
    # 準備兩條免費嘅 Proxy 路線，一條死咗就自動用另一條
    proxies = [
        f"https://api.allorigins.win/raw?url={quote(target_url)}",
        f"https://api.codetabs.com/v1/proxy/?quest={quote(target_url)}"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    for proxy_url in proxies:
        try:
            r = requests.get(proxy_url, headers=headers, timeout=20)
            # 只要成功攞到料，就即刻回傳
            if r.status_code == 200:
                return r
        except Exception:
            pass
        # 轉路線前停 1 秒
        time.sleep(1)
        
    raise Exception(f"所有 Proxy 皆無法突破防線。目標: {target_url}")

# ---------------- 工具函數 ----------------
def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    clean_text = re.sub(r'<[^>]+>', '', raw_html)
    return html.unescape(clean_text).strip()

def fetch_category_id(slug: str) -> int:
    url = f"{WP_API_BASE}/categories?slug={slug}"
    # ✅ 改用 Proxy 去敲門
    r = fetch_via_proxy(url)
    data = r.json()
    
    if not data or not isinstance(data, list):
        raise ValueError(f"找不到 slug 為 '{slug}' 的分類！")
    return data[0]["id"]

def fetch_all_posts_by_category(category_id: int) -> list:
    all_posts = []
    page = 1
    while True:
        url = f"{WP_API_BASE}/posts?categories={category_id}&per_page=100&page={page}&_embed=1"
        print(f"  Fetching page {page} for category {category_id}...")
        
        try:
            # ✅ 改用 Proxy 去敲門
            r = fetch_via_proxy(url)
            posts = r.json()
        except Exception as e:
            print(f"    [警告] 停止翻頁: {e}")
            break
            
        if not posts or not isinstance(posts, list):
            break
            
        all_posts.extend(posts)
        
        # 如果攞到嘅文章少過 100 篇，代表已經係最後一頁
        if len(posts) < 100:
            break
            
        page += 1
        time.sleep(2)
        
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
    
    for topic_key, config in TOPICS.items():
        print(f"\n[INFO] 開始處理專題: {config['display_name']}")
        try:
            cat_id = fetch_category_id(config["slug"])
            print(f"  成功取得 Category ID: {cat_id}")
            
            time.sleep(2)
            
            raw_posts = fetch_all_posts_by_category(cat_id)
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
