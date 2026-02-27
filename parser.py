import feedparser
import gspread
import json
import os
import re
import requests
import logging
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1VAc6d7sfScnLS-LvN7gYg06IeaR6yLgHAFYv4lcTZ3c")
SHEET_NAME = "news"

RSS_FEEDS = [
    "https://news.google.com/rss/search?q=%D0%B3%D0%BE%D0%BB%D0%BE%D0%B4%D0%B0%D0%BD%D0%B8%D0%B5%20OR%20%D0%B0%D1%83%D1%82%D0%BE%D1%84%D0%B0%D0%B3%D0%B8%D1%8F&hl=ru&gl=RU&ceid=RU:ru",
    "https://medvestnik.ru/rss",
]

ALLOW_KEYWORDS = [
    "пребиот", "жкт", "интермитент", "интервальн", "клиническ",
    "кортизол", "кетоз", "голодан", "аутофаг", "микробиом",
    "метабол", "долголет", "диет", "инсулин", "fasting", "autophagy"
]

BLOCK_KEYWORDS = [
    "ремонт", "увол", "назнач", "закуп", "финанс", "администрац",
    "главврач", "совещан", "заседан", "актрис", "роман", "скандал",
    "звезд", "знаменит", "светск"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def is_allowed(text: str) -> bool:
    ltext = (text or "").lower()
    return any(kw in ltext for kw in ALLOW_KEYWORDS) and not any(bw in ltext for bw in BLOCK_KEYWORDS)

def resolve_google_url(url: str) -> str:
    try:
        if "news.google.com" in url:
            resp = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
            real_url = resp.url
            if "news.google.com" not in real_url:
                return real_url
            soup = BeautifulSoup(resp.text, "html.parser")
            link = soup.find("a", {"data-n-au": True})
            if link:
                return link["data-n-au"]
        return url
    except Exception as ex:
        log.error(f"❌ Ошибка resolve: {ex}")
        return url

def scrape_article(url: str) -> tuple:
    try:
        real_url = resolve_google_url(url)
        resp = requests.get(real_url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")

        photo_url = ""
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            photo_url = og["content"]
        if not photo_url:
            tw = soup.find("meta", attrs={"name": "twitter:image"})
            if tw and tw.get("content"):
                photo_url = tw["content"]

        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        article = (
            soup.find("article")
            or soup.find("div", class_=re.compile(r"article|content|body|text", re.I))
            or soup.find("main")
        )

        paragraphs = article.find_all("p") if article else soup.find_all("p")
        full_text = "\n\n".join(
            p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 40
        )[:3000]

        log.info(f"✅ фото={'есть' if photo_url else 'нет'} | текст={len(full_text)}")
        return photo_url, full_text.strip(), real_url

    except Exception as ex:
        log.error(f"❌ Ошибка scrape: {url}: {ex}")
        return "", "", url

def get_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

def main():
    log.info("=== Парсер стартовал ===")
    sheet = get_sheet()

    records = sheet.get_all_records(default_blank="")
    existing_urls = {r.get("url", "") for r in records}
    added = 0

    for feed_url in RSS_FEEDS:
        log.info(f"Читаю RSS: {feed_url[:60]}")
        feed = feedparser.parse(feed_url)

        for entry in feed.entries[:10]:
            title = entry.get("title", "").strip()
            url = entry.get("link", "").strip()

            if not url or url in existing_urls:
                continue

            if not is_allowed(title):
                log.info(f"⏭️ Пропуск: {title[:60]}")
                continue

            photo_url, full_text, real_url = scrape_article(url)

            # Если текст пустой — берём summary из RSS
            if not full_text:
                full_text = BeautifulSoup(entry.get("summary", ""), "html.parser").get_text().strip()
                log.info(f"⚠️ Текст из RSS summary: {title[:60]}")

            if not is_allowed(title + " " + full_text):
                log.info(f"⏭️ Фильтр по тексту: {title[:60]}")
                continue

            sheet.append_row([
                "new",      # A - status
                title,      # B - title
                full_text,  # C - text
                photo_url,  # D - photo_url
                real_url,   # E - url
                "",         # F - comment
            ])

            existing_urls.add(url)
            existing_urls.add(real_url)
            added += 1
            log.info(f"✅ Добавлено: {title[:60]}")

    log.info(f"=== Готово. Добавлено: {added} ===")

if __name__ == "__main__":
    main()
