import re
import logging
import aiohttp
from bs4 import BeautifulSoup
from config import DISTRICTS

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}

async def scrape_district(district_name: str, district_id: int, max_pages: int = 3) -> list[dict]:
    """Парсит объявления с krisha.kz для одного района."""
    all_listings = []

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for page in range(1, max_pages + 1):
            url = f"https://krisha.kz/prodazha/kvartiry/astana/?das[_sys.hasphoto]=1&das[district]={district_id}&page={page}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.warning(f"HTTP {resp.status} for {district_name} page {page}")
                        break
                    html = await resp.text()
            except Exception as e:
                logger.error(f"Error fetching {district_name} page {page}: {e}")
                break

            soup = BeautifulSoup(html, "lxml")
            cards = soup.select(".a-card")

            if not cards:
                break

            for card in cards:
                listing = parse_card(card, district_name)
                if listing:
                    all_listings.append(listing)

            # Проверяем пагинацию
            next_btn = soup.select_one(".paginator__btn--next")
            if not next_btn:
                break

    logger.info(f"{district_name}: собрано {len(all_listings)} объявлений")
    return all_listings


def parse_card(card, district_name: str) -> dict | None:
    """Извлекает данные из одной карточки объявления."""
    title_el = card.select_one(".a-card__title")
    if not title_el:
        return None

    title = title_el.get_text(strip=True)

    # ID из ссылки
    link_el = card.select_one("a[href]")
    href = link_el.get("href", "") if link_el else ""
    id_match = re.search(r"/(\d+)", href)
    if not id_match:
        return None
    ad_id = id_match.group(1)

    # Площадь
    area_match = re.search(r"([\d.]+)\s*м²", title)
    area = float(area_match.group(1)) if area_match else 0

    # Комнаты
    rooms_match = re.search(r"(\d+)-комнатная", title)
    rooms = int(rooms_match.group(1)) if rooms_match else 0

    # Цена
    price_el = card.select_one(".a-card__price")
    price_text = price_el.get_text() if price_el else "0"
    price = int(re.sub(r"\D", "", price_text)) if price_text else 0

    # ЖК из описания
    text_el = card.select_one(".a-card__text-preview")
    text = text_el.get_text(strip=True) if text_el else ""
    zhk_match = re.search(r"жил\.\s*комплекс\s+([^,]+)", text)
    zhk = zhk_match.group(1).strip() if zhk_match else "Не указан"

    # Фото
    img_el = card.select_one("img[src]")
    photo = img_el.get("src", "") if img_el else ""

    # Цена за м²
    price_m2 = round(price / area) if area > 0 else 0

    # Полный URL
    full_url = f"https://krisha.kz{href}" if href.startswith("/") else href

    return {
        "id": ad_id,
        "district": district_name,
        "zhk": zhk,
        "rooms": rooms,
        "area": area,
        "price": price,
        "price_m2": price_m2,
        "phone": "",
        "url": full_url,
        "photo1": photo,
        "photo2": "",
    }


async def scrape_all_districts(max_pages: int = 3) -> list[dict]:
    """Парсит все районы Астаны."""
    all_listings = []
    for name, did in DISTRICTS.items():
        listings = await scrape_district(name, did, max_pages)
        all_listings.extend(listings)
    logger.info(f"Итого собрано: {len(all_listings)} объявлений")
    return all_listings
