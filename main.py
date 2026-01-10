import csv
import os
import sys
from urllib.parse import urlparse

import feedparser
from dateutil import parser as dateparser

from notion_client_util import get_database_id, get_notion_client, upsert_page


def read_sources_csv(path: str):
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r
