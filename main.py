import csv
import os
import sys
from urllib.parse import urlparse

import feedparser
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
        return dt.isoformat() if dt else None
    except Exception:
        return None


def normalize_title(title: str | None, fallback_url: str):
    t = (title or "").strip()
    if t:
        return t
    p = urlparse(fallback_url).path.rstrip("/")
    tail = p.split("/")[-1] if p else fallback_url
    return tail[:200]


def fetch_rss_items(rss_url: str, limit: int = 30):
    d = feedparser.parse(rss_url)
    items = []
    for e in d.entries[:limit]:
        link = getattr(e, "link", None) or getattr(e, "id", None)
        if not link:
            continue
        title = getattr(e, "title", None)
        published = getattr(e, "published", None) or getattr(e, "updated", None)
        items.append({"title": title, "link": link, "published": published})
    return items


def main():
    notion = get_notion_client()
    database_id = get_database_id()

    # ===== テスト：Notionに1行書けるか（ここだけ実行）=====
    upsert_page(
        notion,
        database_id,
        title="【テスト】Notion書き込み確認",
        url="https://example.com/test-notion-write",
        agency="テスト省庁",
        published_at_iso=None,
    )
    print("TEST WRITE DONE")
    return
    # =======================================================

    # ※ return を消したら、以下のRSS処理が動きます
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
                )
                if res == "created":
                    created += 1
                else:
                    updated += 1
            except Exception as e:
                errors += 1
                print(f"[ERROR] Notion upsert failed: {agency} {link} {e}", file=sys.stderr)

    print(f"Done. created={created} updated={updated} errors={errors}")


if __name__ == "__main__":
    main()
