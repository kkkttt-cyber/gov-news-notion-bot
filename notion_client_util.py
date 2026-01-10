import os
import re
from datetime import datetime, timezone, timedelta
from notion_client import Client

JST = timezone(timedelta(hours=9))


def _normalize_database_id(db_id: str) -> str:
    """
    DB ID はハイフン有無どちらでも受け付け、API用に正規化します。
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


def find_page_by_dup_key(notion: Client, database_id: str, dup_key: str):
    """
    DB内の「重複キー」が一致する行（ページ）を1件探します。
    """
    res = notion.databases.query(
        database_id=database_id,
        filter={"pro
