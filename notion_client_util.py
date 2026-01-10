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


def create_page(
    notion,
    database_id,
    *,
    title,
    url,
    agency,
    published_at_iso,
):
    fetched_at = datetime.now(JST).isoformat()

    props = {
        "タイトル": {"title": [{"text": {"content": (title or "")[:200]}}]},
        "URL": {"url": url},
        "省庁": {"rich_text": [{"text": {"content": (agency or "")[:2000]}}]},
        "取得日時": {"date": {"start": fetched_at}},
        "重複キー": {"rich_text": [{"text": {"content": (url or "")[:2000]}}]},
    }

    if published_at_iso:
        props["公開日"] = {"date": {"start": published_at_iso}}

    notion.pages.create(
        parent={"database_id": database_id},
        properties=props,
    )
    return "created"


# 既存コード互換のため、名前は upsert_page のまま提供（中身は create のみ）
def upsert_page(
    notion,
    database_id,
    *,
    title,
    url,
    agency,
    published_at_iso,
):
    # まず「Notionに書ける」ことを優先。重複排除は次のステップで実装します。
    return create_page(
        notion,
        database_id,
        title=title,
        url=url,
        agency=agency,
        published_at_iso=published_at_iso,
    )
