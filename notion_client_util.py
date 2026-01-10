import os
import re
from datetime import datetime, timezone, timedelta
from notion_client import Client

JST = timezone(timedelta(hours=9))


def _normalize_database_id(db_id: str) -> str:
    """
    Accepts database id with/without hyphens.
    """
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


def find_page_by_key(notion: Client, database_id: str, key: str):
    """
    Search a page in DB where property 'Key' equals key.
    Returns page object or None.
    """
    res = notion.databases.query(
        database_id=database_id,
        filter={"property": "Key", "rich_text": {"equals": key}},
        page_size=1,
    )
    results = res.get("results", [])
    return results[0] if results else None


def build_properties(*, title: str, url: str, agency: str, published_at_iso: str | None, fetched_at_iso: str,
                     item_type: str = "RSS", key: str | None = None):
    """
    Build Notion properties. Assumes DB has:
      - Title (title)
      - URL (url)
      - Agency (select)
      - PublishedAt (date) [optional]
      - FetchedAt (date) [optional]
      - Type (select) [optional]
      - Key (rich_text)
    If some properties don't exist in DB, Notion API will error.
    """
    key = key or url

    props = {
        "Title": {"title": [{"text": {"content": title[:200]}}]},
        "URL": {"url": url},
        "Agency": {"select": {"name": agency}},
        "Key": {"rich_text": [{"text": {"content": key[:2000]}}]},
    }

    # Optional fields (only include if provided)
    if published_at_iso:
        props["PublishedAt"] = {"date": {"start": published_at_iso}}
    props["FetchedAt"] = {"date": {"start": fetched_at_iso}}
    props["Type"] = {"select": {"name": item_type}}

    return props


def upsert_page(
    notion: Client,
    database_id: str,
    *,
    title: str,
    url: str,
    agency: str,
    published_at_iso: str | None,
    item_type: str = "RSS",
):
    key = url
    fetched_at_iso = datetime.now(JST).isoformat()

    existing = find_page_by_key(notion, database_id, key)

    props = build_properties(
        title=title,
        url=url,
        agency=agency,
        published_at_iso=published_at_iso,
        fetched_at_iso=fetched_at_iso,
        item_type=item_type,
        key=key,
    )

    if existing:
        notion.pages.update(page_id=existing["id"], properties=props)
        return "updated"
    else:
        notion.pages.create(parent={"database_id": database_id}, properties=props)
        return "created"
