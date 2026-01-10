import csv
import os
import sys
from datetime import datetime
from urllib.parse import urlparse

import feedparser
import requests
from dateutil import parser as dateparser

from notion_client_util import get_database_id, get_notion_client, upsert_page


def read_sources_csv(path: str):
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            agency = (r.get("agency") or "").strip()
            typ = (r.get("type") or "").strip()
            url = (r.get("url") or "").strip()
            if agency and typ and url:
                rows.append({"agency": agency, "type": typ, "url": url})
        return rows


def to_iso_date(dt_str: str | None):
    if not dt_str:
        return None
    try:
        dt = dateparser.parse(dt_str)
        if not dt:
            return None
        # Notion date accepts ISO-8601. Keep timezone if exists.
        return dt.isoformat()
    except Exception:
        return None


def normalize_title(title: str | None, fallback_url: str):
    t = (title or "").strip()
    if t:
        return t
    # Fallback: use path tail
    p = urlparse(fallback_url).path.rstrip("/")
    tail = p.split("/")[-1] if p else fallback_url
    return tail[:200]


def fetch_rss_items(rss_url: str, limit: int = 30):
    """
    Returns list of dict: {title, link, published}
    """
    d = feedparser.parse(rss_url)
    items = []
    for e in d.entries[:limit]:
        link = getattr(e, "link", None) or getattr(e, "id", None)
        if not link:
            continue
        title = getattr(e, "title", None)
        published = getattr(e, "published", None) or getattr(e, "updated", None)
        items.append(
            {
                "title": title,
                "link": link,
                "published": published,
            }
        )
    return items


def main():
    # Basic env checks (Actions already validated, but keep guardrails)
    notion = get_notion_client()
    database_id = get_database_id()

    sources_path = os.getenv("SOURCES_PATH", "data/sources.csv")
    sources = read_sources_csv(sources_path)
    if not sources:
        raise RuntimeError(f"No sources found in {sources_path}")

    created = 0
    updated = 0
    errors = 0

    for s in sources:
        agency = s["agency"]
        typ = s["type"].upper()
        url = s["url"]

        if typ != "RSS":
            # 将来HTML版にも拡張可能。今はRSSのみ対応。
            print(f"[SKIP] {agency} type={typ} url={url}")
            continue

        print(f"[FETCH] {agency} {url}")
        try:
            items = fetch_rss_items(url, limit=50)
        except Exception as e:
            errors += 1
            print(f"[ERROR] RSS parse failed: {agency} {url} {e}", file=sys.stderr)
            continue

        for it in items:
            link = it["link"]
            title = normalize_title(it.get("title"), link)
            published_iso = to_iso_date(it.get("published"))

            try:
                res = upsert_page(
                    notion,
                    database_id,
                    title=title,
                    url=link,
                    agency=agency,
                    published_at_iso=published_iso,
                    item_type="RSS",
                )
                if res == "created":
                    created += 
                else:
                    updated += 1
            except Exception as e:
                errors += 1
                print(f"[ERROR] Notion upsert failed: {agency} {link} {e}", file=sys.stderr)

    print(f"Done. created={created} updated={updated} errors={errors}")


if __name__ == "__main__":
    main()
