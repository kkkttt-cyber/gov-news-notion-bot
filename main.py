import csv
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from notion_client_util import get_database_id, get_notion_client, upsert_page

JST = timezone(timedelta(hours=9))


# ---------- 共通ユーティリティ ----------

def parse_datetime_jst(text: str | None):
    if not text:
        return None
    try:
        dt = dateparser.parse(text)
        if not dt:
            return None
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(JST)
    except Exception:
        return None


def normalize_title(title: str | None, fallback_url: str):
    if title and title.strip():
        return title.strip()[:200]
    p = urlparse(fallback_url).path.rstrip("/")
    return (p.split("/")[-1] if p else fallback_url)[:200]


# ---------- sources.csv ----------

def read_sources_csv(path: str):
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [
            {
                "muni": (r.get("muni") or "").strip(),
                "url": (r.get("url") or "").strip(),
            }
            for r in reader
            if r.get("muni") and r.get("url")
        ]


# ---------- RSS ----------

def fetch_rss_items(url: str, limit: int = 50):
    d = feedparser.parse(url)
    if not d.entries:
        return []

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


# ---------- HTML（汎用新着ページ） ----------

DATE_REGEX = re.compile(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})")


def fetch_html_items(url: str, limit: int = 50):
    headers = {"User-Agent": "GovNewsBot/1.0"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    items = []

    for a in soup.find_all("a", href=True):
        if len(items) >= limit:
            break

        href = a.get("href")
        link = urljoin(url, href)

        text = a.get_text(" ", strip=True)
        around = text
        if a.parent:
            around = a.parent.get_text(" ", strip=True)

        date_match = DATE_REGEX.search(around)
        published = date_match.group(1) if date_match else None

        items.append(
            {
                "title": text,
                "link": link,
                "published": published,
            }
        )

    return items


# ---------- main ----------

def main():
    notion = get_notion_client()
    database_id = get_database_id()

    today_0 = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_0 = today_0 - timedelta(days=1)

    print(f"[WINDOW] {yesterday_0} -> {today_0}")

    sources = read_sources_csv(os.getenv("SOURCES_PATH", "data/sources.csv"))

    created = 0
    updated = 0
    skipped_time = 0
    errors = 0

    for s in sources:
        muni = s["muni"]
        url = s["url"]

        print(f"[SOURCE] {muni} {url}")

        # 1) RSS を試す
        try:
            items = fetch_rss_items(url)
            source_type = "RSS"
        except Exception:
            items = []

        # 2) RSSで取れなければHTML
        if not items:
            try:
                items = fetch_html_items(url)
                source_type = "HTML"
            except Exception as e:
                errors += 1
                print(f"[ERROR] fetch failed: {e}", file=sys.stderr)
                continue

        print(f"[INFO] type={source_type} items={len(items)}")

        for it in items:
            published_jst = parse_datetime_jst(it.get("published"))
            if not published_jst or not (yesterday_0 <= published_jst < today_0):
                skipped_time += 1
                continue

            try:
                res = upsert_page(
                    notion,
                    database_id,
                    title=normalize_title(it.get("title"), it["link"]),
                    url=it["link"],
                    agency=muni,
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
