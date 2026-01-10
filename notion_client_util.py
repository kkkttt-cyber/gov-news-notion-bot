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
    # UUID 正規化（ハイフン除去）
    return re.sub(r"[^0-9a-fA-F]", "", raw)


def find_page_by_url_via_search(notion: Client, database_id: str, url: str):
    """
    databases.query が使えない環境向け。
    pages.search で DB 内を検索し、URL が一致するページを探す。
    """
    res = notion.search(
        query=url,
        filter={"property": "object", "value": "page"},
        page_size=10,
    )

    for page in res.get("results", []):
        parent = page.get("parent", {})
        if parent.get("type") != "database_id":
            continue
        if parent.get("database_id") != database_id:
            continue

        props = page.get("properties", {})
        url_prop = props.get("URL", {})
        if url_prop.get("type") == "url" and url_prop.get("url") == url:
            return page

    return None


def build_properties(
    *,
    title: str,
    url: str,
    agency: str,
    published_at_iso: str | None,
):
    fetched_at = datetime.now(JST).isoformat()

    props = {
        "タイトル": {"title": [{"text": {"content": title[:200]}}]},
        "URL": {"url": url},
        "省庁": {"rich_text": [{"text": {"content": agency}}]},
        "取得日時": {"date": {"start": fetched_at}},
        "重複キー": {"rich_text": [{"text": {"content": url}}]},
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
    """
    URL をキーにして Upsert：
    - 既存あり → update
    - 無し → create
    """
    existing = find_page_by_url_via_search(notion, database_id, url)
    props = build_properties(
        title=title,
        url=url,
        agency=agency,
        published_at_iso=published_at_iso,
    )

    if existing:
        notion.pages.update(page_id=existing["id"], properties=props)
        return "updated"

    notion.pages.create(
        parent={"database_id": database_id},
        properties=props,
    )
    return "created"
