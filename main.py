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

# いろんな表記の日付を拾う（例：2026/1/9, 2026-01-09, 令和8年1月9日, 1月9日 など）
DATE_PATTERNS = [
    re.compile(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})"),
    re.compile(r"(\d{4}年\d{1,2}月\d{1,2}日)"),
    re.compile(r"(令和\d{1,2}年\d{1,2}月\d{1,2}日)"),
    re.compile(r"(\d{1,2}月\d{1,2}日)"),          # 月日だけ
    re.compile(r"(\d{1,2}[/-]\d{1,2})"),          # 01/09 等
]


def read_sources_csv(path: str):
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [
            {"muni": (r.get("muni") or "").strip(), "url": (r.get("url") or "").strip()}
            for r in reader
            if r.get("muni") and r.get("url")
        ]


def normalize_title(title: str | None, fallback_url: str):
    if title and title.strip():
        return title.strip()[:200]
    p = urlparse(fallback_url).path.rstrip("/")
    return (p.split("/")[-1] if p else fallback_url)[:200]


def normalize_date_text(text: str, base_dt: datetime | None = None) -> str:
    """
    日付文字列を dateutil で解釈しやすい形に正規化する。
    対応：
    ① 和暦（令和）
    ② 西暦（YYYY年MM月DD日 / YYYY-MM-DD / YYYY/MM/DD）
    ③ 月日のみ（MM月DD日 / MM/DD）→ 年は base_dt（通常は今日JST）から補完
    """
    if not text:
        return ""

    t = text.strip()

    # ① 和暦（令和）: 令和1年=2019年 → 2018 + 年数
    m = re.search(r"令和(\d{1,2})年\s*(\d{1,2})月\s*(\d{1,2})日", t)
    if m:
        year = 2018 + int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year}-{month:02d}-{day:02d}"

    # ② 西暦（YYYY年MM月DD日）
    m = re.search(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日", t)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year}-{month:02d}-{day:02d}"

    # ② 西暦（YYYY/MM/DD or YYYY-MM-DD）
    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", t)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year}-{month:02d}-{day:02d}"

    # ③ 月日のみ（MM月DD日 / MM/DD）
    m = re.search(r"(\d{1,2})月\s*(\d{1,2})日", t)
    if not m:
        m = re.search(r"(\d{1,2})[/-](\d{1,2})", t)

    if m:
        month = int(m.group(1))
        day = int(m.group(2))

        base = base_dt or datetime.now(JST)
        year = base.year

        # 年跨ぎ補正：1月に12月の記事を拾ったら前年扱い
        if month > base.month:
            year -= 1

        return f"{year}-{month:02d}-{day:02d}"

    # それ以外はそのまま（dateutilに賭ける）
    return t


def parse_datetime_jst(text: str | None):
    """
    文字列日付をJST datetimeに変換。
    - 和暦/西暦/月日のみ を normalize_date_text で正規化してから dateutil でparse
    """
    if not text:
        return None
    try:
        norm = normalize_date_text(text, base_dt=datetime.now(JST))
        dt = dateparser.parse(norm)
        if not dt:
            return None
        if not dt.tzinfo:
            # timezoneが無い場合はUTC扱いにしてJSTへ
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(JST)
    except Exception:
        return None


def looks_like_rss_url(url: str) -> bool:
    u = url.lower()
    return any(u.endswith(s) for s in [".rss", ".rdf", ".xml"]) or "rss" in u


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


def extract_date_text(text: str) -> str | None:
    if not text:
        return None
    for pat in DATE_PATTERNS:
        m = pat.search(text)
        if m:
            return m.group(1)
    return None


def pick_best_candidates(soup: BeautifulSoup, base_url: str):
    """
    新着ページの「それっぽい」リンク集合を絞る。
    - nav/footer 由来の大量リンクを避けるため
    """
    for sel in ["main", "article", "section", "#main", ".main", ".contents"]:
        node = soup.select_one(sel)
        if node:
            return node.find_all("a", href=True)
    return soup.find_all("a", href=True)


def fetch_detail_date(link: str) -> str | None:
    """
    詳細ページから日付を拾う（一覧で拾えない場合の救済）
    """
    headers = {"User-Agent": "GovNewsBot/1.0 (+GitHub Actions)"}
    try:
        r = requests.get(link, headers=headers, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # timeタグ優先
        time_el = soup.find("time")
        if time_el:
            dt_raw = time_el.get("datetime") or time_el.get_text(" ", strip=True)
            if dt_raw:
                return dt_raw

        # meta property
        for meta_name in ["article:published_time", "article:modified_time"]:
            m = soup.find("meta", attrs={"property": meta_name})
            if m and m.get("content"):
                return m.get("content")

        # ページ全体から保険で拾う
        text = soup.get_text(" ", strip=True)
        return extract_date_text(text)

    except Exception:
        return None


def fetch_html_items(url: str, limit: int = 50, detail_date_budget: int = 10):
    """
    HTML新着ページから候補を拾う
    - 日付が取れないものは、上限(detail_date_budget)まで詳細ページを見て救済
    """
    headers = {"User-Agent": "GovNewsBot/1.0 (+GitHub Actions)"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    candidates = pick_best_candidates(soup, url)

    items = []
    seen = set()
    detail_used = 0

    for a in candidates:
        if len(items) >= limit:
            break

        href = a.get("href")
        if not href:
            continue
        link = urljoin(url, href)

        # 同一ページアンカー・同URLは除外
        if link.startswith(url + "#") or link == url:
            continue
        if link in seen:
            continue
        seen.add(link)

        title = a.get_text(" ", strip=True)

        # タイトルが空/短すぎるリンクは除外
        if not title or len(title) < 2:
            continue

        # 日付（周辺テキストから拾う）
        around = ""
        if a.parent:
            around = a.parent.get_text(" ", strip=True) or ""
        around = (around + " " + title).strip()

        date_text = extract_date_text(around)

        # timeタグ
        if not date_text:
            time_el = None
            if a.parent:
                time_el = a.parent.find("time")
            if not time_el:
                time_el = a.find("time")
            if time_el:
                dt_raw = time_el.get("datetime") or time_el.get_text(" ", strip=True)
                date_text = dt_raw

        # meta property（一覧ページ側にある場合）
        if not date_text:
            for meta_name in ["article:modified_time", "article:published_time"]:
                m = soup.find("meta", attrs={"property": meta_name})
                if m and m.get("content"):
                    date_text = m.get("content")
                    break

        # 最終救済：詳細ページ
        if not date_text and detail_used < detail_date_budget:
            date_text = fetch_detail_date(link)
            detail_used += 1

        items.append({"title": title, "link": link, "published": date_text})

    return items


def main():
    notion = get_notion_client()
    database_id = get_database_id()

    # ===== 時間窓（JST）：前日0:00〜当日0:00 =====
    today_0 = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_0 = today_0 - timedelta(days=1)
    print(f"[WINDOW] {yesterday_0.isoformat()} -> {today_0.isoformat()}")

    sources = read_sources_csv(os.getenv("SOURCES_PATH", "data/sources.csv"))
    if not sources:
        raise RuntimeError("sources.csv is empty")

    created = 0
    updated = 0
    skipped_time = 0
    skipped_nodate = 0
    errors = 0

    for s in sources:
        muni = s["muni"]
        url = s["url"]

        items = []
        source_type = "HTML"

        # 1) URL的にRSSっぽければRSS優先、ダメならHTMLへ
        if looks_like_rss_url(url):
            try:
                items = fetch_rss_items(url, limit=50)
                source_type = "RSS" if items else "HTML"
            except Exception:
                items = []
                source_type = "HTML"

        if not items:
            try:
                items = fetch_html_items(url, limit=50, detail_date_budget=10)
                source_type = "HTML"
            except Exception as e:
                errors += 1
                print(f"[ERROR] fetch failed: {muni} {url} {e}", file=sys.stderr)
                continue

        print(f"[SOURCE] {muni} type={source_type} items={len(items)}")

        for it in items:
            published_jst = parse_datetime_jst(it.get("published"))
            if not published_jst:
                skipped_nodate += 1
                continue

            if not (yesterday_0 <= published_jst < today_0):
                skipped_time += 1
                continue

            link = it["link"]
            title = normalize_title(it.get("title"), link)

            try:
                res = upsert_page(
                    notion,
                    database_id,
                    title=title,
                    url=link,
                    agency=muni,
                    published_at_iso=published_jst.isoformat(),
                )
                if res == "created":
                    created += 1
                else:
                    updated += 1
            except Exception as e:
                errors += 1
                print(f"[ERROR] Notion upsert failed: {muni} {link} {e}", file=sys.stderr)

    print(
        f"Done. created={created} updated={updated} "
        f"skipped_nodate={skipped_nodate} skipped_time={skipped_time} errors={errors}"
    )


if __name__ == "__main__":
    main()
