import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
KASPI_PHONE = os.getenv("KASPI_PHONE", "+7 (XXX) XXX-XX-XX")
KASPI_NAME = os.getenv("KASPI_NAME", "")
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "7"))

# Районы Астаны: название -> district ID на krisha.kz
DISTRICTS = {
    "Есильский": 8,
    "Алматинский": 4,
    "Байконурский": 6,
    "Сарыаркинский": 2,
    "Нура": 10,
}

# Лимиты тарифов
TARIFF_LIMITS = {
    "Триал":      {"listings": 10, "phones": True,  "districts": 99},
    "Бесплатный": {"listings": 3,  "phones": False, "districts": 1},
    "Стандарт":   {"listings": 10, "phones": True,  "districts": 3},
    "Премиум":    {"listings": 10, "phones": True,  "districts": 99},
}

# Валидация
MIN_PRICE_PER_M2 = 150_000
MAX_PRICE_PER_M2 = 1_500_000
MIN_AREA = 18
MAX_AREA = 500
MIN_PRICE = 3_000_000
MAX_PRICE = 1_000_000_000
ANOMALY_RATIO = 2.0
