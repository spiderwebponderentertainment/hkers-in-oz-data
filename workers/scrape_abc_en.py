# workers/scrape_abc_en.py
"""
ABC English scraper (lean)
- Crawl up to MAX_CRAWL = 250 candidates (latest-first frontier)
- Output newest MAX_OUTPUT = 150
- No paging brute-force. Seeds are the "latest" hubs + RSS (best-effort).
- Strong dedupe by canonical/content-id + url-normalization.
"""

import json, re, sys, html, hashlib, time
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin, urlunparse, parse_qs
from collections import deque
import heapq

import requests
from bs4 import BeautifulSoup

# ---------------- 基本設定 ----------------
HEADERS = {
    "User-Agent": "HKersInOZBot/1.0 (+https://example.com/hkersinoz; scraping latest index only)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
TIMEOUT = 15
SESSION = requests.Session()

MAX_CRAWL  = 250
MAX_OUTPUT = 150

# 可選：時間截停（小時）。0 = 不設
SINCE_HOURS = 0  # e.g. 72

# 入口（避免 page/2..）：僅最新匯總與主要欄目首頁
SEEDS = [
    "https://www.abc.net.au/news/",
    "https://www.abc.net.au/news/justin/",
    "https://www.abc.net.au/news/world/",
    "https://www.abc.net.au/news/politics/",
    "https://www.abc.net.au/news/business/",
    "https://www.abc.net.au/news/science/",
    "https://www.abc.net.au/news/health/",
    "https://www.abc.net.au/news/sport/",
    "https://www.abc.net.au/news/environment/",
]
# 一條 Top stories RSS（若失效亦不影響主流程）
RSS_TRY = [
    "https://www.abc.net.au/news/feed/51120/rss",  # Top Stories (commonly used id)
]

ABC_HOSTS = {"www.abc.net.au", "abc.net.au"}

# ---------------- 通用工具 ----------------
def now_ts() -> float:
    return time.time()

def to_iso8601(dt: datetime | None) -> str | None:
    if not dt:
        return None
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_date_str(s: str) -> datetime | None:
    s = s.strip()
    # 常見格式：2025-09-19T06:18:00Z / 2025-09-19T06:18:00+00:00
    fmts = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
    return None

def norm_url(u: str, base: str | None = None) -> str | None:
    if not u:
        return None
    if base:
        u = urljoin(base, u)
    try:
        p = urlparse(u)
        if p.scheme not in ("http", "https"):
            return None
        # 只保留 ABC 網域
        if p.netloc not in ABC_HOSTS:
            return None
        # 去 query / fragment（ABC 文章 URL 不依賴 query）
        clean = p._replace(query="", fragment="")
        return urlunparse(clean)
    except Exception:
        return None

def content_id_from_url(u: str) -> str | None:
    """
    ABC 文章常見尾段數字，如 .../105792346
    """
    m = re.search(r"/(\d{6,})/?$", u)
    return m.group(1) if m else None

def get(url: str) -> requests.Response | None:
    try:
        r = SESSION.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code >= 400:
            print(f"[WARN] GET {url} -> {r.status_code}", file=sys.stderr)
            return None
        return r
    except Exception as e:
        print(f"[WARN] GET fail {url}: {e}", file=sys.stderr)
        return None

# ---------------- 列表頁抽連結 + 列表級時間 ----------------
def extract_time_on_card(el) -> datetime | None:
    # 1) <time datetime="...">
    t = el.find("time")
    if t and t.has_attr("datetime"):
        dt = parse_date_str(t["datetime"])
        if dt:
            return dt
    # 2) data-* 里頭可能藏時間（保守抓）
    for attr in ("data-timestamp", "data-published", "data-updated"):
        if el.has_attr(attr):
            dt = parse_date_str(el[attr])
            if dt:
                return dt
    # 3) meta in card
    meta = el.find("meta", attrs={"itemprop": "datePublished"})
    if meta and meta.has_attr("content"):
        dt = parse_date_str(meta["content"])
        if dt:
            return dt
    return None

def parse_list(url: str, html_text: str) -> list[tuple[str, datetime | None]]:
    soup = BeautifulSoup(html_text, "html.parser")
    out: list[tuple[str, datetime | None]] = []

    # ABC 列表卡片常用 a[href]，先廣義取，再在父級找時間
    for a in soup.find_all("a", href=True):
        href = norm_url(a["href"], base=url)
        if not href:
            continue
        # 粗略篩：只接受看似文章的路徑（有數字 ID）
        if not content_id_from_url(href):
            continue

        # 嘗試從就近容器抽列表級時間
        dt = None
        candidates = [a]
        # 逐層向上找最多 3 層
        parent = a.parent
        hops = 0
        while parent is not None and hops < 3 and dt is None:
            dt = extract_time_on_card(parent)
            parent = parent.parent
            hops += 1

        out.append((href, dt))
    return dedupe_links(out)

def dedupe_links(pairs: list[tuple[str, datetime | None]]):
    seen = set()
    out = []
    for href, dt in pairs:
        key = href
        if key in seen:
            continue
        seen.add(key)
        out.append((href, dt))
    return out

# ---------------- 文章頁解析 ----------------
def parse_article(url: str, html_text: str) -> dict | None:
    soup = BeautifulSoup(html_text, "html.parser")

    # canonical 作為主鍵優先
    canon = soup.find("link", rel="canonical")
    if canon and canon.has_attr("href"):
        cu = norm_url(canon["href"], base=url)
        if cu:
            url = cu

    title = None
    ogt = soup.find("meta", property="og:title")
    if ogt and ogt.has_attr("content"):
        title = ogt["content"]
    if not title:
        t = soup.find("title")
        title = t.get_text(strip=True) if t else None

    # description
    desc = None
    ogd = soup.find("meta", property="og:description")
    if ogd and ogd.has_attr("content"):
        desc = ogd["content"]
    if not desc:
        md = soup.find("meta", attrs={"name": "description"})
        if md and md.has_attr("content"):
            desc = md["content"]

    # published time
    pub = None
    for sel in [
        ("meta", {"property": "article:published_time"}, "content"),
        ("meta", {"itemprop": "datePublished"}, "content"),
        ("time", {"itemprop": "datePublished"}, "datetime"),
        ("time", {}, "datetime"),
    ]:
        tag, attrs, attr_name = sel
        el = soup.find(tag, attrs=attrs)
        if el and el.has_attr(attr_name):
            pub = parse_date_str(el[attr_name])
            if pub:
                break

    if not title:
        # 非標準頁或特殊專題頁就跳過
        return None

    id_hint = content_id_from_url(url) or hashlib.md5(url.encode()).hexdigest()

    return {
        "id": id_hint,
        "title": title,
        "link": url,
        "summary": desc or "",
        "publishedAt": to_iso8601(pub),
        "source": "ABC News (EN)",
    }

# ---------------- RSS 輔助（可沒有亦不影響） ----------------
def try_rss_seed() -> list[tuple[str, datetime | None]]:
    out = []
    for u in RSS_TRY:
        r = get(u)
        if not r:
            continue
        try:
            soup = BeautifulSoup(r.text, "xml")
            for item in soup.find_all("item"):
                link = item.find("link")
                pubd = item.find("pubDate")
                href = norm_url(link.get_text(strip=True)) if link else None
                if not href:
                    continue
                dt = None
                if pubd:
                    try:
                        dt = datetime.strptime(pubd.get_text(strip=True), "%a, %d %b %Y %H:%M:%S %z")
                    except Exception:
                        dt = None
                out.append((href, dt))
        except Exception:
            pass
    return dedupe_links(out)

# ---------------- 主流程 ----------------
def main():
    start = now_ts()

    # 時間截停
    cutoff_dt = None
    if SINCE_HOURS and SINCE_HOURS > 0:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=SINCE_HOURS)

    # 建立「最新優先」frontier：(-epoch, seq, url)
    seq = 0
    frontier = []
    pushed = set()

    def push_link(href: str, hint_dt: datetime | None):
        nonlocal seq
        if href in pushed:
            return
        if cutoff_dt and hint_dt and hint_dt < cutoff_dt:
            return
        score = -(hint_dt.timestamp() if hint_dt else (now_ts() - 3600 - seq))  # 沒時間就按發現序
        heapq.heappush(frontier, (score, seq, href))
        pushed.add(href)
        seq += 1

    # 先放 RSS（最新）
    for (href, dt) in try_rss_seed():
        push_link(href, dt)

    # 再放各 hub 首頁（從中抽連結 + 列表級時間再入 frontier）
    for s in SEEDS:
        r = get(s)
        if not r:
            continue
        pairs = parse_list(s, r.text)
        for href, dt in pairs:
            push_link(href, dt)

    visited = set()
    results: dict[str, dict] = {}

    crawled = 0
    while frontier and crawled < MAX_CRAWL:
        _, _, url = heapq.heappop(frontier)
        if url in visited:
            continue
        visited.add(url)

        r = get(url)
        if not r:
            continue
        art = parse_article(url, r.text)
        crawled += 1

        if art:
            # canonical 去重（以 link 作 key）
            key = art["link"]
            results[key] = art

        # 從文章頁再嘗試挖少量「相關連結」（同站且含 ID），但不過度擴散
        if crawled < MAX_CRAWL:
            try:
                soup = BeautifulSoup(r.text, "html.parser")
                # 只抽最多 10 條相關
                picked = 0
                for a in soup.find_all("a", href=True):
                    if picked >= 10:
                        break
                    href = norm_url(a["href"], base=url)
                    if not href:
                        continue
                    if not content_id_from_url(href):
                        continue
                    # 用列表順序近似時間（冇卡片時間）
                    push_link(href, None)
                    picked += 1
            except Exception:
                pass

    # 輸出前整理：去重 → 按 publishedAt DESC → 取前 MAX_OUTPUT
    items = list(results.values())

    # 再以 id 去重（保險）
    seen_ids = set()
    uniq = []
    for it in items:
        iid = it.get("id") or hashlib.md5(it["link"].encode()).hexdigest()
        if iid in seen_ids:
            continue
        seen_ids.add(iid)
        uniq.append(it)

    def sort_key(it):
        pa = it.get("publishedAt")
        try:
            return datetime.strptime(pa, "%Y-%m-%dT%H:%M:%SZ") if pa else datetime.min.replace(tzinfo=timezone.utc)
        except Exception:
            try:
                # 容忍 +00:00 形式
                return datetime.fromisoformat(pa.replace("Z", "+00:00")) if pa else datetime.min.replace(tzinfo=timezone.utc)
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)

    uniq.sort(key=sort_key, reverse=True)
    output = uniq[:MAX_OUTPUT]

    took = now_ts() - start
    print(f"[INFO] frontier seeds={len(pushed)} crawled={crawled} kept={len(output)} took={took:.1f}s", file=sys.stderr)

    print(json.dumps(output, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    try:
        from datetime import timedelta  # lazy import for cutoff option
    except Exception:
        pass
    main()
