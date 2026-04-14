import statistics
import logging
from config import MIN_PRICE_PER_M2, MAX_PRICE_PER_M2, MIN_AREA, MAX_AREA, MIN_PRICE, MAX_PRICE, ANOMALY_RATIO

logger = logging.getLogger(__name__)


def validate_listings(listings: list[dict]) -> list[dict]:
    """Валидация объявлений + проверка аномалий внутри ЖК."""

    # Шаг 1: базовая валидация
    for item in listings:
        item["validation"] = "✅ ОК"
        item["comment"] = ""

        if item["price_m2"] < MIN_PRICE_PER_M2:
            item["validation"] = "⚠️ АНОМАЛИЯ"
            item["comment"] = f"Цена/м² {item['price_m2']:,} < {MIN_PRICE_PER_M2:,}"
        elif item["price_m2"] > MAX_PRICE_PER_M2:
            item["validation"] = "⚠️ АНОМАЛИЯ"
            item["comment"] = f"Цена/м² {item['price_m2']:,} > {MAX_PRICE_PER_M2:,}"
        elif item["area"] < MIN_AREA:
            item["validation"] = "⚠️ АНОМАЛИЯ"
            item["comment"] = f"Площадь {item['area']} < {MIN_AREA}"
        elif item["area"] > MAX_AREA:
            item["validation"] = "⚠️ АНОМАЛИЯ"
            item["comment"] = f"Площадь {item['area']} > {MAX_AREA}"
        elif item["price"] < MIN_PRICE:
            item["validation"] = "⚠️ АНОМАЛИЯ"
            item["comment"] = f"Цена {item['price']:,} < {MIN_PRICE:,}"
        elif item["price"] > MAX_PRICE:
            item["validation"] = "⚠️ АНОМАЛИЯ"
            item["comment"] = f"Цена {item['price']:,} > {MAX_PRICE:,}"

    # Шаг 2: аномалии внутри ЖК
    zhk_groups = {}
    for item in listings:
        if item["validation"] == "✅ ОК" and item["zhk"] != "Не указан":
            zhk_groups.setdefault(item["zhk"], []).append(item)

    for zhk, items in zhk_groups.items():
        if len(items) < 3:
            continue
        prices = [i["price_m2"] for i in items]
        median = statistics.median(prices)
        for item in items:
            if item["price_m2"] < median / ANOMALY_RATIO:
                item["validation"] = "⚠️ АНОМАЛИЯ"
                item["comment"] = f"Ниже медианы ЖК {zhk} ({median:,.0f}) в {ANOMALY_RATIO}x раз"
            elif item["price_m2"] > median * ANOMALY_RATIO:
                item["validation"] = "⚠️ АНОМАЛИЯ"
                item["comment"] = f"Выше медианы ЖК {zhk} ({median:,.0f}) в {ANOMALY_RATIO}x раз"

    ok_count = sum(1 for i in listings if i["validation"] == "✅ ОК")
    anomaly_count = sum(1 for i in listings if i["validation"] == "⚠️ АНОМАЛИЯ")
    logger.info(f"Валидация: {ok_count} ОК, {anomaly_count} аномалий")

    return listings
