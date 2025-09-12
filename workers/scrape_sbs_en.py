# workers/scrape_sbs_en.py
import json, sys, time, hashlib
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

HEADERS = {"User-Agent": "HKersInOZBot/1.0"}
TIMEOUT = 20
MAX_ITEMS = 50
FETCH_SLEEP = 0.3

def iso_now(): return datetime.now(timezone.utc).isoformat()

def fetch(url): 
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status(); return r

def make_item(url, html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    title = (soup.find("meta", property="og:title") or {}).get("content") or (soup.title.string if soup.title else url)
    desc = (soup.find("meta", property="og:description") or {}).get("content") or ""
    pub = (soup.find("meta", property="article:published_time") or {}).get("content") or None
    return {
        "id": hashlib.md5(url.encode()).hexdigest(),
        "title": title.strip(),
        "link": url,
        "summary": desc.strip(),
        "publishedAt": pub,
        "source": "SBS English",
        "fetchedAt": iso_now(),
    }

if __name__ == "__main__":
    FEED = "https://www.sbs.com.au/news/feed"  # SBS 英文官方 RSS
    try:
        xml = fetch(FEED).text
        root = ET.fromstring(xml)
        ns = {"dc": "http://purl.org/dc/elements/1.1/"}
        items = []
        for it in root.findall(".//item"):
            link = it.findtext("link")
            if not link: continue
            try:
                html_text = fetch(link).text
                items.append(make_item(link, html_text))
                if len(items) >= MAX_ITEMS: break
                time.sleep(FETCH_SLEEP)
            except Exception as e:
                print(f"[WARN] fail {link}: {e}", file=sys.stderr)
                continue
        payload = {"source": "SBS English", "generatedAt": iso_now(), "count": len(items), "items": items}
        with open("sbs_en.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[DONE] {len(items)} items", file=sys.stderr)
    except Exception as e:
        print(f"[FATAL] feed fetch fail: {e}", file=sys.stderr)
        sys.exit(1)

    json_out(articles, "sbs_en.json")
    rss_out(articles,  "sbs_en.xml")
