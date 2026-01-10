import csv
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import feedparser
from dateutil import parser as dateparser

from notion_client_util import get_database_id, get_notion_client, upsert_page

JST = timezone(timedelta(hours=9))


def read_sources_csv(path: str):
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [
            {
                "agency": (r.get("agency") or "").strip(),
                "type": (r.get("type") or "").strip(),
                "url": (r.get("url") or "").strip(),
            }
            for r in reader
            if r.get("agency") and r.get("type") and r.get("url")
        ]


def normalize_title(title: str | None, fallback_url: str):
    if title and title.strip():
        return title.strip()
    p = urlparse(fallback_url).path.rstrip("/")
    return (p.split("/")[-1] if p else fallback_url)[:200]


def parse_datetime_jst(dt_str: str | None):
    if not dt_str:
        return None
    try:
        dt = dateparser.parse(dt_str)
        if not dt:
            return None
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(JST)
    except Exception:
        return None


def fetch_rss_items(rss_url: str, limit: int = 50):
    d = feedparser.parse(rss_url)
    items = []
    for e in d.entries[:limit]:
        link = getattr(e, "link", None) or getattr(e, "id", None)
        if not link:
            continue
        items.append(
            {
                "title": getattr(e, "title", None),
                "link": link,
                "published": getattr(e, "published", None)
                or getattr(e, "updated", None),
            }
        )
    return items


def main():
    notion = get_notion_client()
    database_id = get_database_id()

    # ===== 時間窓（JST）=====
    today_0 = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_0 = today_0 - timedelta(days=1)
    print(f"[WINDOW] {yesterday_0.isoformat()} -> {today_0.isoformat()}")

    sources = read_sources_csv(os.getenv("SOURCES_PATH", "data/sources.csv"))

    created = 0
    updated = 0
    skipped_time = 0
    errors = 0

    for s in sources:
        if s["type"].upper() != "RSS":
            continue

        print(f"[FETCH] {s['agency']} {s['url']}")

        try:
            items = fetch_rss_items(s["url"])
        except Exception as e:
            print(f"[ERROR] RSS parse failed: {e}", file=sys.stderr)
            errors += 1
            continue

        for it in items:
            published_jst = parse_datetime_jst(it["published"])
            if not published_jst or not (yesterday_0 <= published_jst < today_0):
                skipped_time += 1
                continue

            try:
                res = upsert_page(
                    notion,
                    database_id,
                    title=normalize_title(it["title"], it["link"]),
                    url=it["link"],
                    agency=s["agency"],
                    published_at_iso=published_jst.isoformat(),
                )
                if res == "created":
                    created += 1
                else:
                    updated += 1
            except Exception as e:
                errors += 1
                print(f"[ERROR] Notion upsert failed: {e}", file=sys.stderr)

    print(
        f"Done. created={created} updated={updated} "
        f"skipped_time={skipped_time} errors={errors}"
    )


if __name__ == "__main__":
    main()
