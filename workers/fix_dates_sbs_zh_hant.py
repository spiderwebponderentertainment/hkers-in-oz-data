# workers/fix_dates_sbs_zh_hant.py
# 用途：讀取 repo root 的 sbs_zh_hant.json，對缺失 publishedAt 的項目逐條補日期，然後覆寫輸出。
import json, sys, re, html
from pathlib import Path
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "HKersInOZBot/1.0 (+news-aggregator; contact: you@example.com)"}
TIMEOUT = 20
INPUT_JSON = Path("sbs_zh_hant.json")   # 由 workers/scrape_sbs_zh_hant.py 產出
OUTPUT_JSON = Path("sbs_zh_hant.json")  # 覆寫返同一份

def iso_now(): return datetime.now(timezone.utc).isoformat()

def clean(s):
    return re.sub(r"\s+", " ", (s or "")).strip()

def normalize_date(raw: str | None) -> str | None:
    if not raw: return None
    raw = clean(raw)
    # 先試 ISO
    try:
        if raw.endswith("Z"):
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).isoformat()
        dt = datetime.fromisoformat(raw)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    # 再試 RFC822
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def fetch_date_from_page(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # 1) meta: article:published_time / og:article:published_time
    for key in ("article:published_time", "og:article:published_time"):
        tag = soup.find("meta", {"property": key})
        if tag and tag.get("content"):
            dt = normalize_date(tag["content"])
            if dt: return dt

    # 2) JSON-LD 中嘅 datePublished / dateCreated
    for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        txt = tag.string or tag.get_text() or ""
        for candidate in _iter_json_candidates(txt):
            # @graph 內的 NewsArticle
            if isinstance(candidate, dict) and "@graph" in candidate:
                for g in candidate["@graph"]:
                    if isinstance(g, dict) and g.get("@type") in ("NewsArticle","Article"):
                        dt = normalize_date(g.get("datePublished") or g.get("dateCreated"))
                        if dt: return dt
            # 直屬 NewsArticle
            if isinstance(candidate, dict) and candidate.get("@type") in ("NewsArticle","Article"):
                dt = normalize_date(candidate.get("datePublished") or candidate.get("dateCreated"))
                if dt: return dt

    # 3) 某些頁面會用 <meta name="date">
    tag = soup.find("meta", attrs={"name":"date"})
    if tag and tag.get("content"):
        dt = normalize_date(tag["content"])
        if dt: return dt

    return None

def _iter_json_candidates(txt: str):
    import json
    # 先嘗試整段
    try:
        data = json.loads(txt)
        if isinstance(data, list):
            for x in data: yield x
        else:
            yield data
        return
    except Exception:
        pass
    # 兜底：逐個 {...} 嘗試
    for m in re.finditer(r"\{.*?\}", txt, re.S):
        frag = m.group(0)
        try:
            yield json.loads(frag)
        except Exception:
            continue

def main():
    if not INPUT_JSON.exists():
        print(f"[ERR] {INPUT_JSON} not found. Run workers/scrape_sbs_zh_hant.py first.", file=sys.stderr)
        sys.exit(1)

    payload = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    items = payload.get("items", [])
    changed = 0

    for it in items:
        pub = it.get("publishedAt")
        if pub:  # 已有日期，跳過
            continue
        link = it.get("link")
        if not link:
            continue
        dt = fetch_date_from_page(link)
        if dt:
            it["publishedAt"] = dt
            changed += 1

    payload["generatedAt"] = iso_now()
    payload["count"] = len(items)
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] fixed {changed} items with missing dates (total {len(items)})")

if __name__ == "__main__":
    main()

