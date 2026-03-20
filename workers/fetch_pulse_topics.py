import cloudscraper
from bs4 import BeautifulSoup
import json
import sys
import os
import time
import random
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

# ---------------- 設定 ----------------
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

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "zh-HK,zh-TW;q=0.9,zh;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}

SOURCE_NAME = "追光者"
SOURCE_CATEGORY = "專題報導"

# ---------------- 工具函數 ----------------
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_url(url: str) -> str:
    """標準化 URL，減少重複"""
    if not url:
        return url

    try:
        parsed = urlparse(url.strip())
        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            fragment="",
            query=""
        )
        clean_url = urlunparse(normalized)

        # 去除尾斜線（保留根目錄例外）
        if clean_url.endswith("/") and len(clean_url) > len(f"{parsed.scheme}://{parsed.netloc}/"):
            clean_url = clean_url.rstrip("/")

        return clean_url
    except Exception:
        return url.strip()


def safe_text(elem) -> str:
    if not elem:
        return ""
    return elem.get_text(" ", strip=True)


def load_existing_json(file_path: str) -> list:
    """讀取現有 JSON，作為歷史資料庫"""
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("items", [])
        except Exception as e:
            print(f"  [警告] 讀取現有 JSON 失敗: {e}")
    return []


def dedup_items(items: list) -> list:
    """按 link 去重"""
    merged = {}
    for item in items:
        link = normalize_url(item.get("link", ""))
        if not link:
            continue
        item["link"] = link
        item["id"] = link
        merged[link] = item
    return list(merged.values())


def parse_article_block(art) -> dict | None:
    """從單個文章區塊提取資料"""
    # 1. 搵標題 + link
    a_tag = None

    # 常見寫法優先
    selectors = [
        "h1 a", "h2 a", "h3 a", "h4 a",
        ".entry-title a",
        ".post-title a",
        ".jeg_post_title a",
        ".td-module-title a",
        "a[rel='bookmark']"
    ]

    for sel in selectors:
        a_tag = art.select_one(sel)
        if a_tag and a_tag.get("href") and safe_text(a_tag):
            break

    if not a_tag:
        # 最後 fallback：搵第一個有 href 嘅 a
        for candidate in art.find_all("a", href=True):
            text = safe_text(candidate)
            href = candidate.get("href", "")
            if text and href and not href.startswith("#"):
                a_tag = candidate
                break

    if not a_tag:
        return None

    title = safe_text(a_tag)
    link = normalize_url(a_tag.get("href", ""))

    if not title or not link:
        return None

    # 過濾分類頁 / 無效 link
    bad_patterns = [
        "/c/", "/category/", "/tag/", "/author/"
    ]
    # 但如果 link 同文章頁一樣是 /c/... 唔一定代表無效，所以唔直接全擋
    # 只擋到明顯係 topic archive 自己
    if link.endswith("/c") or link.endswith("/category"):
        return None

    # 2. 搵時間
    published_at = now_iso()

    time_elem = art.find("time")
    if time_elem:
        if time_elem.get("datetime"):
            published_at = time_elem.get("datetime").strip()
        else:
            maybe_time_text = safe_text(time_elem)
            if maybe_time_text:
                published_at = maybe_time_text

    # 3. 搵 summary
    summary_selectors = [
        ".excerpt",
        ".summary",
        ".entry-summary",
        ".post-excerpt",
        ".jeg_post_excerpt",
        ".td-excerpt"
    ]

    summary = ""
    for sel in summary_selectors:
        excerpt_elem = art.select_one(sel)
        if excerpt_elem:
            summary = safe_text(excerpt_elem)
            if summary:
                break

    return {
        "id": link,
        "title": title,
        "link": link,
        "summary": summary,
        "publishedAt": published_at,
        "source": SOURCE_NAME,
        "sourceCategory": SOURCE_CATEGORY
    }


def parse_topic_page(html: str) -> list:
    """用 BeautifulSoup 拆解 HTML，盡量支援多款結構"""
    soup = BeautifulSoup(html, "html.parser")
    items = []

    # 優先用常見文章容器
    article_blocks = soup.select("article")

    # 如無 article，就試其他常見 class
    if not article_blocks:
        fallback_selectors = [
            "main .post",
            "main .item",
            "main .archive-post",
            "main .jeg_posts article",
            "main .jeg_post",
            "main .td_module_wrap",
            "main .entry",
            ".content-area article",
            ".site-main article"
        ]

        seen_blocks = []
        for sel in fallback_selectors:
            found = soup.select(sel)
            for block in found:
                if block not in seen_blocks:
                    seen_blocks.append(block)

        article_blocks = seen_blocks

    print(f"    [分析] 找到 {len(article_blocks)} 個文章區塊候選。")

    for art in article_blocks:
        item = parse_article_block(art)
        if item:
            items.append(item)

    # 如果都搵唔到，就直接由 heading link fallback
    if not items:
        print("    [分析] 標準文章區塊無結果，改用 heading link fallback。")
        for a_tag in soup.select("h1 a, h2 a, h3 a, h4 a"):
            title = safe_text(a_tag)
            link = normalize_url(a_tag.get("href", ""))

            if not title or not link:
                continue

            # 盡量排除分類 / 導航
            if "/c/" in link or "/tag/" in link or "/author/" in link:
                continue

            items.append({
                "id": link,
                "title": title,
                "link": link,
                "summary": "",
                "publishedAt": now_iso(),
                "source": SOURCE_NAME,
                "sourceCategory": SOURCE_CATEGORY
            })

    items = dedup_items(items)
    return items


def fetch_html_with_retry(scraper, url: str, max_attempts: int = 5) -> str | None:
    """抓 HTML，支援 429 重試"""
    print(f"  正在模擬人類瀏覽器載入網頁: {url}")

    for attempt in range(1, max_attempts + 1):
        try:
            headers = dict(REQUEST_HEADERS)
            headers["Referer"] = "https://pulsehknews.com/"

            response = scraper.get(url, headers=headers, timeout=25)

            if response.status_code == 200:
                print(f"    [成功] HTTP 200")
                return response.text

            if response.status_code == 429:
                wait_seconds = min(15 * (2 ** (attempt - 1)), 180) + random.uniform(1, 3)
                print(f"    [限流] HTTP 429，第 {attempt}/{max_attempts} 次，{wait_seconds:.1f} 秒後重試...")
                time.sleep(wait_seconds)
                continue

            print(f"    [失敗] 網頁回傳代碼: {response.status_code}")
            return None

        except Exception as e:
            wait_seconds = min(10 * (2 ** (attempt - 1)), 120) + random.uniform(1, 3)
            print(f"    [錯誤] 第 {attempt}/{max_attempts} 次請求失敗: {e}")
            if attempt < max_attempts:
                print(f"    [重試] {wait_seconds:.1f} 秒後再試...")
                time.sleep(wait_seconds)

    return None


def fetch_html_and_parse(scraper, url: str) -> list:
    """抓取 HTML 並解析"""
    html = fetch_html_with_retry(scraper, url)
    if not html:
        return []

    items = parse_topic_page(html)
    return items


def create_shared_scraper():
    """建立共用 scraper session"""
    scraper = cloudscraper.create_scraper(
        browser={
            "browser": "chrome",
            "platform": "windows",
            "desktop": True
        }
    )
    return scraper


# ---------------- 主程式 ----------------
def main():
    now_utc = now_iso()

    # 共用同一個 session，cookies 會比較似正常瀏覽
    scraper = create_shared_scraper()

    for idx, (topic_key, config) in enumerate(TOPICS.items(), start=1):
        print(f"\n[INFO] ({idx}/{len(TOPICS)}) 開始處理專題: {config['display_name']}")

        try:
            # 1. 抓取最新專題頁
            new_items = fetch_html_and_parse(scraper, config["url"])
            print(f"  成功從網頁刮到 {len(new_items)} 篇新聞")

            if not new_items:
                print("  [放棄] 無法取得新文章，保留原有檔案。")
                continue

            # 2. 讀取舊有歷史新聞
            existing_items = load_existing_json(config["output_file"])
            print(f"  從本地資料庫載入 {len(existing_items)} 篇歷史紀錄")

            # 3. 合併 + 去重
            final_items = dedup_items(existing_items + new_items)

            # 4. 排序
            # 盡量用 publishedAt 排序，排唔到就當字串比
            def sort_key(x):
                return str(x.get("publishedAt", ""))

            final_items.sort(key=sort_key, reverse=True)
            print(f"  合併後總共有 {len(final_items)} 篇新聞")

            # 5. 輸出
            payload = {
                "topicKey": topic_key,
                "topic": config["display_name"],
                "generatedAt": now_utc,
                "count": len(final_items),
                "items": final_items
            }

            with open(config["output_file"], "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            print(f"[DONE] 成功輸出 {config['output_file']}")

        except Exception as e:
            print(
                f"[ERROR] 處理專題 {config['display_name']} 時發生錯誤: {e}",
                file=sys.stderr
            )

        # 專題之間停耐少少，減低被 block 機會
        if idx < len(TOPICS):
            sleep_seconds = random.uniform(8, 15)
            print(f"  [等待] 專題之間暫停 {sleep_seconds:.1f} 秒...")
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
