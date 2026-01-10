import os
import re
from datetime import datetime, timezone, timedelta
from notion_client import Client

JST = timezone(timedelta(hours=9))


def _normalize_database_id(db_id: str) -> str:
    return re.sub(r"[^0-9a-fA-F]", "", db_id)


def get_notion_client() -> Client:
    token = os.getenv("NOTION_TOKEN")
    if not token:
        raise RuntimeError("NOTION_TOKEN is missing (set in GitHub Secrets).")
    return Client(auth=token)


def get_database_id() -> str:
    db_id = os.getenv("NOTION_DATABASE_ID")
    if not db_id:
        raise RuntimeError("NOTION_DATABASE_ID is missing (set in GitHub Secrets).")
    return _normalize_database_id(db_id)


def find_page_by_dup_key(notion: Client, database_id: str, dup_key: str):
    res = notion.databases.query(
        database_id=database_id,
        filter={"property": "重複キー", "rich_text": {"equals": dup_key}},
        page_size=1,
    )
    results = res.get("results", [])
    return results[0] if results else None


def build_properties(
    *,
    title: str,
    url: str,
    agency: str,
    published_at_iso: str | None,
    fetched_at_iso: str,
    dup_key: str,
):
    props = {
        "タイトル": {"title": [{"text": {"content": title[:200]}}]},
        "URL": {"url": url},
        "省庁": {"select": {"name": agency}},
        "取得日時": {"date": {"start": fetched_at_iso}},
        "重複キー": {"rich_text": [{"text": {"content": dup_key[:2000]}}]},
    }

    if published_at_iso:
        props["公開日"] = {"date": {"start": published_at_iso}}

    return props


def upsert_page(
    notion: Client,
    database_id: str,
    *,
    title: str,
    url: str,
    agency: str,
    published_at_iso: str | None,
):
    dup_key = url
    fetched_at_iso = datetime.now(JST).isoformat()

    existing = find_page_by_dup_key(notion, database_id, dup_key)

    props = build_properties(
        title=title,
        url=url,
        agency=agency,
        published_at_iso=published_at_iso,
        fetched_at_iso=fetched_at_iso,
        dup_key=dup_key,
    )

    if existing:
        notion.pages.update(page_id=existing["id"], properties=props)
        return "updated"
    else:
        notion.pages.create(parent={"database_id": database_id}, properties=props)
        return "created"
