# workers/scrape_abc_en.py
# -*- coding: utf-8 -*-
import re, sys, json, time, hashlib, html
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

# ---------------- 基本設定 ----------------
HEADERS = {
    # 預設用 Desktop Chrome（比較少被擋）
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.google.com/",
}
REQ_TIMEOUT = (8, 20)  # (connect, read)
MAX_CRAWL = 250        # 最多嘗試抓取的文章 URL 數
MAX_OUTPUT = 150       # 輸出數量上限（最新優先）

session = requests.Session()
session.headers.update(HEADERS)

# 只允許「正文」頁面：/news/YYYY-MM-DD/.../<numeric-id>
GOOD_URL_RE = re.compile(
    r"^https?://www\.abc\.net\.au/news/\d{4}-\d{2}-\d{2}/[^?#]+/\d+/?$"
)

# 已知不需要或容易 403/404 的前綴
BAD_PREFIXES = (
    "https://www.abc.net.au/news/topic/",
    "https://www.abc.net.au/news/subscribe",
    "https://www.abc.net.au/news/emergency",
    "https://www.abc.net.au/news/chinese",
    "https://www.abc.net.au/news/indonesian",
    "https://www.abc.net.au/news/tok-pisin",
)

# 丟棄含有以下關鍵字的鏈接
BAD_KEYWORDS = ("live-blog", "/page/", "#")


# ---------------- HTTP Helper ----------------
def _normalize_https(u: str) -> str:
    # 強制 https，去除多餘 fragment
    u = u.replace("http://", "https://").split("#", 1)[0]
    return u

def get(url, **kw):
    """帶 fallback 的 GET：403/406 會嘗試 AMP/不同 UA。"""
    url = _normalize_https(url)
    variants = [url]
    # 可能存在 AMP 版
    if not url.endswith("?amp"):
        variants.append(url + ("&amp" if "?" in url else "?amp"))
    if not url.rstrip("/").endswith("/output=amp"):
        variants.append(url.rstrip("/") + "/?output=amp")

    # 兩套 UA：Desktop（預設）→ Mobile Safari
    ua_desktop = HEADERS["User-Agent"]
    ua_mobile = ("Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
                 "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 "
                 "Mobile/15E148 Safari/604.1")

    last_exc = None
    for ua in (ua_desktop, ua_mobile):
        session.headers["User-Agent"] = ua
        for v in variants:
            for attempt in range(2):
                try:
                    r = session.get(v, timeout=REQ_TIMEOUT, **kw)
                    # 403/406 再試下一個變體
                    if r.status_code in (403, 406):
                        time.sleep(0.3)
                        continue
                    r.raise_for_status()
                    return r
                except requests.RequestException as e:
                    last_exc = e
                    time.sleep(0.4 * (attempt + 1))
    if last_exc:
        raise last_exc
    raise requests.RequestException("Unknown fetch error")

# ---------------- 種子頁（不做分頁） ----------------
SEED_PAGES = [
    "https://www.abc.net.au/news/",
    "https://www.abc.net.au/news/justin/",
    "https://www.abc.net.au/news/politics/",
    "https://www.abc.net.au/news/world/",
    "https://www.abc.net.au/news/business/",
    "https://www.abc.net.au/news/sport/",
    "https://www.abc.net.au/news/health/",
    "https://www.abc.net.au/news/science/",
    "https://www.abc.net.au/news/environment/",
]


# ---------------- Link 抽取與篩選 ----------------
def _looks_like_article(u: str) -> bool:
    if any(k in u for k in BAD_KEYWORDS):
        return False
    if u.startswith(BAD_PREFIXES):
        return False
    return bool(GOOD_URL_RE.match(u))


def extract_links(html_text, base):
    soup = BeautifulSoup(html_text, "lxml")
    urls = set()
    for a in soup.select("a[href]"):
        href = a["href"].strip()
        u = urljoin(base, href)
        # 清理 fragment
        u = _normalize_https(u)
        if _looks_like_article(u):
            urls.add(u)
    return urls

# 由 ABC 文章 URL 取尾段數字 ID（愈大愈新）
def _id_from_url(u: str) -> int:
    m = re.search(r"/(\d+)/?$", u)
    return int(m.group(1)) if m else -1

# 由 URL 補回 YYYY-MM-DD 日期
def _date_from_url(u: str):
    m = re.search(r"/news/(\d{4}-\d{2}-\d{2})/", u)
    if not m:
        return None
    try:
        dt = datetime.fromisoformat(m.group(1) + "T00:00:00+00:00")
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

# ---------------- 文章解析 ----------------
def _text(x):
    if not x:
        return ""
    return " ".join(x.get_text(" ", strip=True).split())


def _first(soup, selectors):
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return el
    return None


def _parse_datetime(s: str):
    # 嘗試 ISO8601
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    # 兜底：抽出數字時間戳（ABC 不常用）
    return None


def parse_article(url: str, html_text: str):
    """
    回傳統一結構：
    {
      "url": str,
      "title": str,
      "published_at": iso8601 (UTC) | None,
      "content": str,
      "source": "abc_en",
      "id": str  # 穩定ID
    }
    """
    soup = BeautifulSoup(html_text, "lxml")

    # 標題：優先 og:title / <meta name="title"> / <h1>
    title = ""
    ogt = soup.select_one('meta[property="og:title"]')
    if ogt and ogt.get("content"):
        title = ogt["content"].strip()
    if not title:
        mt = soup.select_one('meta[name="title"]')
        if mt and mt.get("content"):
            title = mt["content"].strip()
    if not title:
        h1 = _first(soup, ["h1", "header h1", "article h1"])
        title = _text(h1) if h1 else ""

    # 發佈時間：og:article:published_time / article:published_time / time[datetime]
    published = None
    for sel in [
        'meta[property="article:published_time"]',
        'meta[name="article:published_time"]',
        'meta[property="og:article:published_time"]',
    ]:
        m = soup.select_one(sel)
        if m and m.get("content"):
            published = _parse_datetime(m["content"].strip())
            if published:
                break
    if not published:
        t = soup.select_one("time[datetime]")
        if t and t.get("datetime"):
            published = _parse_datetime(t["datetime"].strip())

    # 冇 meta 就用 URL 日期兜底
    if not published:
        published = _date_from_url(url)

    # 內容：盡量由正文容器抽 <p>
    body = ""
    body_root = _first(
        soup,
        [
            "article",
            '[data-component="article-body"]',
            ".article",
            ".story",
            ".article-body",
            "main",
        ],
    )
    if body_root:
        paras = [p for p in body_root.select("p") if _text(p)]
        body = "\n".join(_text(p) for p in paras)
    if not body:
        # 兜底：全頁 p
        paras = [p for p in soup.select("p") if _text(p)]
        body = "\n".join(_text(p) for p in paras[:50])

    # 需要最少有標題或內容先算有效
    if not title and not body:
        return None

    # 穩定 id：用 URL / 或最後數字ID
    m = re.search(r"/(\d+)/?$", url)
    stable_id = m.group(1) if m else hashlib.md5(url.encode("utf-8")).hexdigest()

    item = {
        "url": url,
        "title": title,
        "published_at": published.isoformat().replace("+00:00", "Z") if published else None,
        "content": body,
        "source": "abc_en",
        "id": stable_id,
    }
    return item


# ---------------- 抓取主流程 ----------------
def fetch_article(url):
    resp = get(url)
    resp.raise_for_status()
    return parse_article(url, resp.text)


def crawl():
    print(f"[INFO] entry page urls: {len(SEED_PAGES)}")
    entry_urls = set()
    for seed in SEED_PAGES:
        try:
            r = get(seed)
            r.raise_for_status()
            links = extract_links(r.text, seed)
            entry_urls |= links
        except Exception as e:
            print(f"[WARN] entry scrape fail {seed}: {e}")
    
    # 用尾段數字 ID 倒序（最新優先），再裁 MAX_CRAWL
    entry_urls = sorted(entry_urls, key=_id_from_url, reverse=True)
    print(f"[INFO] entry page urls: {len(entry_urls)}")

    # 限制爬取數量（避免跑太耐）
    entry_urls = entry_urls[:MAX_CRAWL]

    print(f"[INFO] crawl urls: {len(entry_urls)}")
    out = []
    for u in entry_urls:
        try:
            art = fetch_article(u)
            if art:
                out.append(art)
        except Exception as e:
            print(f"[WARN] fetch article fail {u}: {e}")
    return out


def _published_key(x):
    """
    排序鍵：優先用 published_at；冇嘅話用 URL 裏面嘅 YYYY-MM-DD 作回退（設 12:00Z）。
    """
    # 1) 正常路：用 published_at
    ts = x.get("published_at")
    if ts:
        try:
            return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
        except Exception:
            pass
    # 2) 回退：由 URL 取日期
    url = x.get("url", "")
    m = re.search(r"/(\d{4})-(\d{2})-(\d{2})/", url)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            # 中午 12:00Z，避免凌晨邊界；只用嚟比大小
            dt = datetime(y, mo, d, 12, 0, 0, tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            pass
    # 3) 冇辦法就最尾
    return 0


def main():
    items = crawl()

    # 依發佈時間降序
    # 先用 id 去重，避免同一篇多個變體
    uniq = {}
    for it in items:
        iid = it.get("id") or hashlib.md5(it["url"].encode("utf-8")).hexdigest()
        if iid not in uniq:
            uniq[iid] = it
    items = list(uniq.values())
    items.sort(key=_published_key, reverse=True)

    # 只輸出最新 150 條
    items = items[:MAX_OUTPUT]

    print(f"[DONE] output {len(items)} items")

    # 如果你需要把 JSON 輸出到檔案，可取消以下兩行註解：
    # with open("abc_en_out.json", "w", encoding="utf-8") as f:
    #     json.dump(items, f, ensure_ascii=False, indent=2)

    # 有啲 pipeline 會從 stdout 讀，呢度一拼輸出 JSON
    # （如果你唔想經 stdout，註解呢兩行）
    print(json.dumps(items, ensure_ascii=False))


if __name__ == "__main__":
    main()
