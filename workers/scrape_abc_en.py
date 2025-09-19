# workers/scrape_abc_en.py
# -*- coding: utf-8 -*-
import re, sys, json, time, hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

# ---------------- 基本設定 ----------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.google.com/",
}
REQ_TIMEOUT = (8, 20)  # (connect, read)
MAX_CRAWL = 250        # 最多嘗試抓取的文章 URL 數
MAX_OUTPUT = 150       # 輸出數量上限（最新優先）

session = requests.Session()
session.headers.update(HEADERS)

# 允許的正文頁面：/news/YYYY-MM-DD/.../<numeric-id>
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

# 種子頁（把 Just In 放第 1 位）
SEED_PAGES = [
    "https://www.abc.net.au/news/justin/",
    "https://www.abc.net.au/news/",
    "https://www.abc.net.au/news/politics/",
    "https://www.abc.net.au/news/world/",
    "https://www.abc.net.au/news/business/",
    "https://www.abc.net.au/news/sport/",
    "https://www.abc.net.au/news/health/",
    "https://www.abc.net.au/news/science/",
    "https://www.abc.net.au/news/environment/",
]

# ---------------- HTTP Helper ----------------
def _normalize_https(u: str) -> str:
    # 強制 https，去除 fragment + query（utm 等）
    u = u.replace("http://", "https://")
    u = u.split("#", 1)[0]
    u = u.split("?", 1)[0]
    return u

def get(url, **kw):
    """帶 fallback 的 GET：403/406 會嘗試 AMP/不同 UA。"""
    url = _normalize_https(url)
    variants = [url]
    if not url.endswith("?amp"):
        variants.append(url + ("&amp" if "?" in url else "?amp"))
    if not url.rstrip("/").endswith("/output=amp"):
        variants.append(url.rstrip("/") + "/?output=amp")

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

# ---------------- Link 抽取與篩選 ----------------
def _looks_like_article(u: str) -> bool:
    if any(k in u for k in BAD_KEYWORDS):
        return False
    if u.startswith(BAD_PREFIXES):
        return False
    return bool(GOOD_URL_RE.match(u))

def _parse_dt(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def extract_links_with_times(html_text, base) -> list[tuple[str, datetime | None]]:
    """
    回傳 [(url, dt_or_None)]，保留頁面上的出現順序。
    會嘗試在 <a>、其父層或兄弟節點尋找 <time datetime> 作為排序線索。
    """
    soup = BeautifulSoup(html_text, "lxml")
    results = []
    for a in soup.select("a[href]"):
        u = urljoin(base, a["href"].strip())
        u = _normalize_https(u)
        if not _looks_like_article(u):
            continue
        # 嘗試取時間
        dt = None
        cand = a.find("time", attrs={"datetime": True})
        if not cand and a.parent:
            cand = a.parent.find("time", attrs={"datetime": True})
        if not cand:
            # 再試兄弟
            sib = a.find_next_sibling("time")
            if sib and sib.has_attr("datetime"):
                cand = sib
        if cand and cand.has_attr("datetime"):
            dt = _parse_dt(cand["datetime"].strip())
        results.append((u, dt))
    return results

# 由 ABC 文章 URL 取尾段數字 ID（愈大愈新；僅作次序參考）
def _id_from_url(u: str) -> int:
    m = re.search(r"/(\d+)/?$", u)
    return int(m.group(1)) if m else -1

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

def parse_article(url: str, html_text: str):
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

    # 發佈時間：meta 或 <time>
    published = None
    for sel in [
        'meta[property="article:published_time"]',
        'meta[name="article:published_time"]',
        'meta[property="og:article:published_time"]',
    ]:
        m = soup.select_one(sel)
        if m and m.get("content"):
            published = _parse_dt(m["content"].strip())
            if published:
                break
    if not published:
        t = soup.select_one("time[datetime]")
        if t and t.get("datetime"):
            published = _parse_dt(t["datetime"].strip())

    # 正文
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
        paras = [p for p in soup.select("p") if _text(p)]
        body = "\n".join(_text(p) for p in paras[:50])

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
    print(f"[INFO] entry bases: {len(SEED_PAGES)}")

    # 1) 先抓 Just In（第一個 seed），保留頁面順序 + <time> 作排序
    justin_links = []
    try:
        r = get(SEED_PAGES[0])
        r.raise_for_status()
        justin_links = extract_links_with_times(r.text, SEED_PAGES[0])
    except Exception as e:
        print(f"[WARN] entry scrape fail {SEED_PAGES[0]}: {e}")

    # 以 <time> 倒序；冇 <time> 放最後但仍保留原出現順序
    def _justin_key(item):
        u, dt = item
        return (1 if dt is None else 0, 0 if dt is None else -int(dt.timestamp()))
    justin_links.sort(key=_justin_key)

    # 2) 其他 seed 頁（不看 <time>，純收集 → 稍後用 ID 倒序）
    other_urls = set()
    for seed in SEED_PAGES[1:]:
        try:
            r = get(seed)
            r.raise_for_status()
            links = extract_links_with_times(r.text, seed)  # 我哋只要 URL
            for u, _ in links:
                other_urls.add(u)
        except Exception as e:
            print(f"[WARN] entry scrape fail {seed}: {e}")

    other_urls = list(other_urls)
    other_urls.sort(key=lambda u: _id_from_url(u), reverse=True)

    # 3) 合併：Just In（已按時間）→ 其他（ID 倒序）
    ordered = [u for (u, _) in justin_links] + other_urls

    # 4) 去重（保留第一個出現者），裁剪 MAX_CRAWL
    seen = set()
    final_list = []
    for u in ordered:
        if u not in seen:
            seen.add(u)
            final_list.append(u)
        if len(final_list) >= MAX_CRAWL:
            break

    print(f"[INFO] crawl urls: {len(final_list)}")

    # 5) 抓文
    out = []
    for u in final_list:
        try:
            art = fetch_article(u)
            if art:
                out.append(art)
        except Exception as e:
            print(f"[WARN] fetch article fail {u}: {e}")
    return out

def _published_key(x):
    # 以 published_at 為準；冇就放後面（但因為我們先抓 Just In，通常都會有）
    ts = x.get("published_at")
    if ts:
        try:
            return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
        except Exception:
            pass
    # 後備：用 URL 中的日期（置 12:00Z）
    url = x.get("url", "")
    m = re.search(r"/(\d{4})-(\d{2})-(\d{2})/", url)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            dt = datetime(y, mo, d, 12, 0, 0, tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            pass
    return 0

def main():
    items = crawl()

    # 去重（以 id 為鍵），再按發佈時間降序
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
    print(json.dumps(items, ensure_ascii=False))

if __name__ == "__main__":
    main()
