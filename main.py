import os
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))

def main():
    notion_token = os.getenv("NOTION_TOKEN")
    db_id = os.getenv("NOTION_DATABASE_ID")

    if not notion_token or not db_id:
        raise RuntimeError("NOTION_TOKEN or NOTION_DATABASE_ID is missing")

    print("GitHub Actions OK")
    print("Now JST:", datetime.now(JST))

if __name__ == "__main__":
    main()
