import os
import re
from datetime import datetime, timezone, timedelta
from notion_client import Client

JST = timezone(timedelta(hours=9))


def get_notion_client():
    token = os.getenv("NOTION_TOKEN")
    if not token:
        raise RuntimeError("NOTION_TOKEN is missing")
    return Client(auth=token)


def get_database_id():
    raw = os.getenv("NOTION_DATABASE_ID")
    if not raw:
        raise RuntimeError("NOTION_DATABASE_ID is missing")
    return re.sub(r"[^0-9a-fA-F]", "", raw)


def find_page_by_dup_key(notion, database_id, dup_key):
    res = notion.databases.query(
        database_id=database_id,
        filter={
            "property": "重複キー",
            "rich_text": {"equals": dup_key},
        },
        page_size=1,
    )
    results = res.get("results", [])
    return results[0] if results else None


def upsert_page(
    notion,
    database_id,
    *,
    title,
    url,
    agency,
    published_at_iso,
):
    dup_key = url
    fetched_at = datetime.now(JST).isoformat()

    props = {
        "タイトル": {
            "title": [{"text": {"content": title[:200]}}],
        },
        "URL": {"url": url},
        "省庁": {
            "rich_text": [{"text": {"content": agency}}],
        },
        "取得日時": {"date": {"start": fetched_at}},
        "重複キー": {
            "rich_text": [{"text": {"content": dup_key}}],
        },
    }

    if published_at_iso:
        props["公開日"] = {"date": {"start": published_at_iso}}

    existing = find_page_by_dup_key(notion, database_id, dup_key)

    if existing:
        notion.pages.update(
            page_id=existing["id"],
            properties=props,
        )
        return "updated"

    notion.pages.create(
        parent={"database_id": database_id},
        properties=props,
    )
    return "created"
