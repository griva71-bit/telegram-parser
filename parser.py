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
SHEET_NAME = "news"  # ← остаётся "news" как было!

RSS_FEEDS = [
    "https://medvestnik.ru/rss",
    "https://nv.ua/rss/health.xml",
    "https://naked-science.ru/rss",
    "https://nauka.tass.ru/rss",
    "https://indicator.ru/rss",
    "https://pcr.news/rss/",
    "https://naukatv.ru/rss",
    "https://sciencenews.ru/rss",
]

ALLOW_KEYWORDS = [
    # Питание и диеты
    "пребиот", "жкт", "интермитент", "интервальн",
    "кетоз", "голодан", "аутофаг", "микробиом",
    "метабол", "диет", "инсулин", "fasting", "autophagy",
    "питани", "пищевар", "кишечник", "ожирен", "похуден",
    "антиоксид", "витамин", "минерал", "жир", "углевод",
    # Общая медицина
    "клиническ", "исследован",
    "учёны", "ученые", "учены",
    "врач", "пациент", "лечен", "терапи",
    "препарат", "лекарств", "таблетк",
    "вакцин", "иммун", "иммунит",
    "болезн", "заболеван", "синдром",
    "диагноз", "симптом",
    "операци", "хирург",
    # Науки о теле
    "биохим", "гормон", "воспален", "кортизол",
    "долголет", "старени", "стареет",
    "мозг", "нейрон", "нервн",
    "ген", "днк", "клетк", "белок", "фермент",
    "антител", "микроб", "вирус", "бактери", "инфекц",
    "рак", "онкол", "опухол",
    "сердц", "сосуд", "давлени",
    "холестер", "сахар", "диабет", "глюкоз",
    "стресс", "тревог", "депресс",
    "сон", "усталост", "энерги",
    "печен", "почк", "лёгк", "легк",
    "кост", "мышц", "сустав",
    "кожа", "кож",
    # Научные действия
    "открыл", "обнаружил", "доказал", "выяснил",
    "установил", "показал", "нашли", "нашел",
    "эксперимент", "испытани", "клинич",
    "исследовател", "учёный", "ученый",
    "антибиотик", "пробиотик",
    "аллерги", "астм",
    "инсульт", "инфаркт",
    "гипертони", "аритми",
]

BLOCK_KEYWORDS = [
    "ремонт", "увол", "назнач", "закуп",
    "администрац", "главврач", "совещан", "заседан",
    "актрис", "роман", "скандал", "звезд", "знаменит",
    "светск", "полиц", "крим", "арест",
    "выбор", "депутат", "партия", "митинг",
    "футбол", "хоккей", "матч",
    "погод", "прогноз погод",
    "мода", "стил", "тренд",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def is_allowed(text: str) -> bool:
    ltext = (text or "").lower()
    blocked = any(bw in ltext for bw in BLOCK_KEYWORDS)
    allowed = any(kw in ltext for kw in ALLOW_KEYWORDS)
    return allowed and not blocked


def scrape_article(url: str) -> tuple:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
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

        log.info(f"фото={'есть' if photo_url else 'нет'} | текст={len(full_text)} | {url[:50]}")
        return photo_url, full_text.strip()

    except Exception as ex:
        log.error(f"Ошибка scrape: {ex} | {url[:50]}")
        return "", ""


def get_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)


def main():
    log.info("=== Парсер стартовал ===")
    sheet = get_sheet()

    records = sheet.get_all_records(default_blank="")
    existing_urls = {r.get("url", "") for r in records}
    log.info(f"Уже в таблице: {len(existing_urls)} записей")
    added = 0

    for feed_url in RSS_FEEDS:
        log.info(f"Читаю RSS: {feed_url}")
        try:
            feed = feedparser.parse(feed_url)
            log.info(f"Найдено статей: {len(feed.entries)}")
        except Exception as ex:
            log.error(f"Ошибка RSS: {ex}")
            continue

        for entry in feed.entries[:20]:
            title = entry.get("title", "").strip()
            url   = entry.get("link",  "").strip()

            if not url or url in existing_urls:
                continue

            if not is_allowed(title):
                log.info(f"Пропуск: {title[:60]}")
                continue

            log.info(f"✅ Подходит: {title[:60]}")
            photo_url, full_text = scrape_article(url)

            if not full_text:
                full_text = BeautifulSoup(
                    entry.get("summary", ""), "html.parser"
                ).get_text().strip()

            if not photo_url:
                for enc in entry.get("enclosures", []):
                    if enc.get("type", "").startswith("image"):
                        photo_url = enc.get("href", "")
                        break
                if not photo_url and entry.get("media_thumbnail"):
                    photo_url = entry["media_thumbnail"][0].get("url", "")

            sheet.append_row([
                "new",
                title,
                full_text,
                photo_url,
                url,
                "",
            ])

            existing_urls.add(url)
            added += 1
            log.info(f"➕ Добавлено: {title[:60]}")

    log.info(f"=== Готово. Добавлено: {added} ===")


if __name__ == "__main__":
    main()
