import csv
import os
import sys
from urllib.parse import urlparse

import feedparser
from dateutil import parser as dateparser

from notion_client_util import get_database_id, get_notion_client, upsert_page


SEEN_PATH = os.getenv("SEEN_PATH", "data/seen_urls.txt")


def load_seen(path: str) -> set[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()


def append_seen(path: str, urls: list[str]):
    if not urls:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for u in urls:
            f.write(u + "\n")


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

    # sources
    sources_path = os.getenv("SOURCES_PATH", "data/sources.csv")
    sources = read_sources_csv(sources_path)
    if not sources:
        raise RuntimeError(f"No sources found in {sources_path}")

    # seen set
    seen = load_seen(SEEN_PATH)

    created = 0
    skipped_seen = 0
    errors = 0
    newly_seen: list[str] = []

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
            print(f"[INFO] items={len(items)}")
        except Exception as e:
            errors += 1
            print(f"[ERROR] RSS parse failed: {agency} {url} {e}", file=sys.stderr)
            continue

        for it in items:
            link = it["link"]
            if link in seen:
                skipped_seen += 1
                continue

            title = normalize_title(it.get("title"), link)
            published_iso = to_iso_date(it.get("published"))

            try:
                upsert_page(
                    notion,
                    database_id,
                    title=title,
                    url=link,
                    agency=agency,
                    published_at_iso=published_iso,
                )
                created += 1
                seen.add(link)
                newly_seen.append(link)
            except Exception as e:
                errors += 1
                print(f"[ERROR] Notion create failed: {agency} {link} {e}", file=sys.stderr)

    append_seen(SEEN_PATH, newly_seen)

    print(f"Done. created={created} skipped_seen={skipped_seen} errors={errors}")
    print(f"[INFO] seen_file={SEEN_PATH} newly_seen={len(newly_seen)}")


if __name__ == "__main__":
    main()
