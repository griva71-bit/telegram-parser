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
    "питани", "диет", "голодан", "интервальн",
    "кетоз", "кетоген", "аутофаг", "калори",
    "белок", "витамин", "минерал",
    "антиоксид", "омега", "пробиотик", "пребиотик",
    # Обмен веществ
    "метабол", "инсулин", "глюкоз",
    "ожирен", "похуден", "холестер",
    # ЖКТ и микробиом
    "кишечник", "микробиом", "микрофлор",
    "пищевар", "жкт", "желудок", "печен",
    # Мозг и психика
    "мозг", "нейрон", "когнитивн", "памят",
    "депресс", "тревог", "стресс", "кортизол",
    "сон ", "бессонниц",
    # Сердце и сосуды
    "сердц", "сосуд", "давлени", "гипертони",
    "инсульт", "инфаркт", "аритми",
    # Иммунитет
    "иммун", "воспален", "аутоиммун",
    "аллерги", "астм",
    # Онкология
    "рак ", "онкол", "опухол",
    # Гормоны
    "гормон", "щитовидн", "тестостерон", "эстроген",
    # Старение
    "старени", "долголет", "продолжительност",
    # Общая медицина
    "болезн", "заболеван", "синдром", "диагноз",
    "лечен", "терапи", "препарат", "лекарств",
    "клиническ", "исследован", "испытани",
    "вакцин", "антибиотик", "пациент",
    # Физическая активность
    "физическ", "тренировк", "упражнен", "мышц",
    # Биология человека
    "ген ", "днк", "клетк", "фермент", "антител",
    "вирус", "бактери", "инфекц",
    "кост", "сустав", "кожа",
]

BLOCK_KEYWORDS = [
    # Космос и астрономия
    "космос", "планет", "астроном", "ракет", "спутник",
    "луна ", "марс ", "юпитер", "галактик", "телескоп",
    "астронавт", "орбит", "невесомост",
    # Древняя история и археология
    "динозавр", "палеонтол", "археолог",
    "древн", "фараон", "египет", "викинг",
    # Физика и математика
    "квантов", "коллайдер", "антиматери",
    # Климат и экология
    "климат", "потеплени", "углекислый",
    "вымирани", "биоразнообрази",
    # Роботы и ИИ
    "робот", "нейросет",
    # Политика
    "политик", "выбор", "депутат", "партия",
    "война", "армия", "оружи",
    "экономик", "рынок",
    # Развлечения
    "актрис", "кино", "музык", "концерт",
    "футбол", "хоккей", "матч",
    "мода", "стил",
    # Административное
    "назнач", "увол", "совещан", "заседан",
    "главврач", "администрац", "закуп",
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
