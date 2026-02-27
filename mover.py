import gspread
import json
import os
import logging
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1VAc6d7sfScnLS-LvN7gYg06IeaR6yLgHAFYv4lcTZ3c")

def get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return spreadsheet.worksheet("news"), spreadsheet.worksheet("posts")

def main():
    log.info("=== Mover стартовал ===")
    news_sheet, posts_sheet = get_sheets()

    news_records = news_sheet.get_all_records(default_blank="")
    moved = 0

    for i, row in enumerate(news_records, start=2):
        status = str(row.get("status", "")).strip().lower()

        if status != "ok":
            continue

        title     = row.get("title", "")
        text      = row.get("text", "")
        photo_url = row.get("photo_url", "")
        url       = row.get("url", "")
        comment   = row.get("comment", "")

        # Добавляем в posts
        posts_sheet.append_row([
            "new",                                    # A - status
            "",                                       # B - published_at (пусто, бот заполнит)
            title,                                    # C - title
            text,                                     # D - text
            photo_url,                                # E - photo_url
            url,                                      # F - url
            comment,                                  # G - comment
        ])

        # Меняем статус в news на done
        news_sheet.update_cell(i, 1, "done")

        moved += 1
        log.info(f"Перенесено: {title[:60]}")

    log.info(f"=== Готово. Перенесено: {moved} ===")

if __name__ == "__main__":
    main()
