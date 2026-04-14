import logging
import asyncio
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
import database as db
from scraper import scrape_all_districts
from validator import validate_listings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

ASTANA_TZ = timezone(timedelta(hours=5))

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# --- Helpers ------------------------------------------------

DISTRICT_MAP = {
    "1": "Есильский", "2": "Алматинский", "3": "Байконурский",
    "4": "Сарыаркинский", "5": "Нура", "6": "Все",
}

ROOMS_MAP = {"1": (1,1), "2": (2,2), "3": (3,3), "4": (4,99), "5": (1,99)}

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="/настроить"), KeyboardButton(text="/топ")],
        [KeyboardButton(text="/тарифы"), KeyboardButton(text="/статус")],
        [KeyboardButton(text="/помощь")],
    ],
    resize_keyboard=True,
)


def format_price(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def days_left(end_date) -> int:
    if end_date is None:
        return 0
    today = datetime.now(ASTANA_TZ).date()
    return max(0, (end_date - today).days)


async def check_and_expire_tariff(chat_id: int) -> dict:
    """Check if tariff has expired, downgrade if needed. Returns user row."""
    user = await db.get_user(chat_id)
    if not user:
        return None
    if user["tariff"] in ("Триал", "Стандарт", "Премиум"):
        if user["sub_end"] and datetime.now(ASTANA_TZ).date() > user["sub_end"]:
            await db.update_user(chat_id, tariff="Бесплатный")
            user = await db.get_user(chat_id)
            await bot.send_message(
                chat_id,
                "⏰ Твой тариф *{}* закончился!\n\n"
                "Теперь у тебя бесплатный доступ (3 объявления, без телефонов).\n"
                "Продлить: /тарифы".format(user["tariff"]),
                parse_mode=ParseMode.MARKDOWN,
            )
    return user


def trial_warning(user: dict) -> str:
    if user["tariff"] == "Триал":
        left = days_left(user["sub_end"])
        if left <= 2:
            return f"\n\n⏰ Триал заканчивается через {left} дн! /тарифы"
    return ""


# --- /start -------------------------------------------------

@router.message(CommandStart())
async def cmd_start(msg: Message):
    user = await db.get_user(msg.from_user.id)
    if not user:
        end = datetime.now(ASTANA_TZ).date() + timedelta(days=config.TRIAL_DAYS)
        await db.create_user(
            chat_id=msg.from_user.id,
            name=msg.from_user.full_name or "",
            tariff="Триал",
            sub_start=datetime.now(ASTANA_TZ).date(),
            sub_end=end,
        )
        await db.add_subscription(
            chat_id=msg.from_user.id, tariff="Триал", amount=0,
            method="Авто", start=datetime.now(ASTANA_TZ).date(), end=end,
            comment="Триал при регистрации",
        )
    await msg.answer(
        "Привет! Я бот-аналитик рынка недвижимости Астаны 🏙\n\n"
        "Нахожу самые выгодные квартиры по цене за м² и отправляю "
        "персональные подборки каждые 5 часов.\n\n"
        "🎁 *У тебя 7 дней бесплатного Премиум-доступа* — полный "
        "функционал без ограничений!\n\n"
        "Начни настройку: /настроить\n"
        "Посмотреть топ: /топ\n"
        "Тарифы: /тарифы",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=MAIN_KB,
    )


# --- /настроить -------------------------------------------------

@router.message(Command("настроить"))
async def cmd_setup(msg: Message):
    await db.update_user(msg.from_user.id, setup_step="район")
    await msg.answer(
        "Настройка подборки. Отвечай номерами через запятую.\n\n"
        "Какие районы Астаны интересуют?\n"
        "1. Есильский (Левый берег, деловой центр)\n"
        "2. Алматинский (Правый берег, центр)\n"
        "3. Байконурский\n"
        "4. Сарыаркинский\n"
        "5. Нура\n"
        "6. Все районы"
    )


# --- Setup flow (stateful) ----------------------------------

@router.message(F.text & ~F.text.startswith("/"))
async def handle_text(msg: Message):
    user = await db.get_user(msg.from_user.id)
    if not user:
        await msg.answer("Напиши /start чтобы начать.")
        return

    step = user.get("setup_step", "готово")
    text = msg.text.strip()

    if step == "район":
        nums = [n.strip() for n in text.replace(" ", ",").split(",") if n.strip()]
        if "6" in nums:
            districts = "Все"
        else:
            districts = ",".join(DISTRICT_MAP.get(n, "") for n in nums if n in DISTRICT_MAP)
        if not districts:
            await msg.answer("Выбери номера от 1 до 6. Пример: 1,2")
            return
        await db.update_user(msg.from_user.id, districts=districts, setup_step="комнаты")
        await msg.answer(
            "Сколько комнат?\n"
            "1. 1-комнатные\n2. 2-комнатные\n3. 3-комнатные\n4. 4+\n5. Любые"
        )

    elif step == "комнаты":
        nums = [n.strip() for n in text.replace(" ", ",").split(",") if n.strip()]
        all_rooms = []
        for n in nums:
            if n in ROOMS_MAP:
                all_rooms.append(ROOMS_MAP[n])
        if not all_rooms:
            await msg.answer("Выбери от 1 до 5.")
            return
        rmin = min(r[0] for r in all_rooms)
        rmax = max(r[1] for r in all_rooms)
        await db.update_user(msg.from_user.id, rooms_min=rmin, rooms_max=rmax, setup_step="бюджет")
        await msg.answer("Укажи бюджет (от-до в млн тенге). Пример: 20-50\nИли напиши 'любой'")

    elif step == "бюджет":
        if text.lower() in ("любой", "любые", "любая", "все"):
            await db.update_user(msg.from_user.id, budget_min=0, budget_max=0, setup_step="цена_м2")
        else:
            parts = text.replace(" ", "").split("-")
            try:
                bmin = int(float(parts[0]) * 1_000_000)
                bmax = int(float(parts[1]) * 1_000_000) if len(parts) > 1 else 0
            except (ValueError, IndexError):
                await msg.answer("Формат: 20-50 (млн тенге) или 'любой'")
                return
            await db.update_user(msg.from_user.id, budget_min=bmin, budget_max=bmax, setup_step="цена_м2")
        await msg.answer("Максимальная цена за м² (тенге)? Пример: 500000\nИли 'любая'")

    elif step == "цена_м2":
        if text.lower() in ("любая", "любой", "все", "0"):
            await db.update_user(msg.from_user.id, max_price_m2=0, setup_step="жк")
        else:
            try:
                val = int(text.replace(" ", "").replace(",", ""))
            except ValueError:
                await msg.answer("Укажи число (пример: 500000) или 'любая'")
                return
            await db.update_user(msg.from_user.id, max_price_m2=val, setup_step="жк")
        await msg.answer("Есть целевые ЖК? Напиши названия через запятую или 'любые'")

    elif step == "жк":
        if text.lower() in ("любые", "любой", "все", "нет"):
            await db.update_user(msg.from_user.id, target_zhk="", setup_step="готово")
        else:
            await db.update_user(msg.from_user.id, target_zhk=text, setup_step="готово")

        user = await db.get_user(msg.from_user.id)
        left = days_left(user["sub_end"])
        await msg.answer(
            "✅ Настройка завершена!\n\n"
            f"📍 Районы: {user['districts'] or 'Все'}\n"
            f"🏠 Комнаты: {user['rooms_min'] or '?'}-{user['rooms_max'] or '?'}\n"
            f"💰 Бюджет: {format_price(user['budget_min'] or 0)}-{format_price(user['budget_max'] or 0)} ₸\n"
            f"📊 Макс цена/м²: {format_price(user['max_price_m2'] or 0) if user['max_price_m2'] else 'любая'} ₸\n"
            f"🏗 ЖК: {user['target_zhk'] or 'любые'}\n\n"
            f"🎁 Триал: осталось {left} дней Премиум-доступа\n\n"
            "Подборки придут в ближайшие 5 часов.\n"
            "Посмотреть сейчас: /топ",
            reply_markup=MAIN_KB,
        )
    else:
        await msg.answer("Не понял команду. Напиши /помощь")


# --- /топ ------------------------------------------------

@router.message(Command("топ", "топ5", "топ10", "top"))
async def cmd_top(msg: Message):
    user = await check_and_expire_tariff(msg.from_user.id)
    if not user:
        await msg.answer("Напиши /start чтобы начать.")
        return

    tariff = user["tariff"]
    limits = config.TARIFF_LIMITS.get(tariff, config.TARIFF_LIMITS["Бесплатный"])

    cmd = msg.text.strip().lower()
    max_n = 5 if "5" in cmd else limits["listings"]

    listings = await db.get_filtered_listings(user)
    if not listings:
        await msg.answer("Пока нет объявлений по твоим фильтрам. Попробуй расширить критерии: /настроить")
        return

    # One best per ZhK
    best_per_zhk = {}
    for l in listings:
        zhk = l["zhk"]
        if zhk not in best_per_zhk or l["price_m2"] < best_per_zhk[zhk]["price_m2"]:
            best_per_zhk[zhk] = l
    sorted_listings = sorted(best_per_zhk.values(), key=lambda x: x["price_m2"])[:max_n]

    header = (
        f"🔄 Топ-{len(sorted_listings)} по твоим фильтрам\n"
        f"📍 Районы: {user['districts'] or 'Все'}\n"
        f"{'\u2500' * 25}\n"
    )
    cards = []
    for i, l in enumerate(sorted_listings, 1):
        phone_line = f"📞 {l['phone']}\n" if limits["phones"] and l.get("phone") else ""
        cards.append(
            f"🏆 *#{i}* | {l['district']}\n"
            f"🏗 ЖК: {l['zhk']}\n"
            f"📐 {l['area']} м² | 🏠 {l['rooms']} комн.\n"
            f"💰 {format_price(l['price'])} ₸\n"
            f"📊 *{format_price(l['price_m2'])} ₸/м²*\n"
            f"{phone_line}"
            f"🔗 [Объявление]({l['url']})"
        )

    footer = f"\n{'\u2500' * 25}\n📊 Диапазон: {format_price(sorted_listings[0]['price_m2'])}–{format_price(sorted_listings[-1]['price_m2'])} ₸/м²"

    if tariff == "Бесплатный":
        footer += "\n\n💡 Полный доступ за 5,000 ₸/мес: /тарифы"

    text = header + "\n\n".join(cards) + footer + trial_warning(user)

    # Split if too long
    if len(text) > 4000:
        await msg.answer(header, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        for card in cards:
            await msg.answer(card, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        await msg.answer(footer + trial_warning(user), parse_mode=ParseMode.MARKDOWN)
    else:
        await msg.answer(text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


# --- /жк -------------------------------------------------

@router.message(Command("жк"))
async def cmd_zhk(msg: Message):
    name = msg.text.replace("/жк", "").strip()
    if not name:
        await msg.answer("Укажи название ЖК. Пример: /жк Хайвилл")
        return
    listings = await db.search_by_zhk(name)
    if not listings:
        await msg.answer(f"Объявления по ЖК \u00ab{name}\u00bb не найдены.")
        return
    lines = [f"🏗 *ЖК {name}* \u2014 {len(listings)} объявлений:\n"]
    for l in listings[:15]:
        lines.append(
            f"\u2022 {l['rooms']} комн. {l['area']}м\u00b2 \u2014 {format_price(l['price'])} ₸ "
            f"(*{format_price(l['price_m2'])} ₸/м\u00b2*)"
        )
    await msg.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


# --- /тарифы ------------------------------------------------

@router.message(Command("тарифы"))
async def cmd_tariffs(msg: Message):
    await msg.answer(
        "📋 *Тарифы:*\n\n"
        "🎁 *Триал* \u2014 7 дней бесплатно\n"
        "\u2022 Полный Премиум-доступ для новых пользователей\n\n"
        "🆓 *Бесплатный* \u2014 0 ₸/мес\n"
        "\u2022 Топ-3 раз в день, без телефонов, 1 район\n\n"
        "💼 *Стандарт* \u2014 5,000 ₸/мес\n"
        "\u2022 Топ-10 каждые 5 часов\n"
        "\u2022 Телефоны продавцов\n"
        "\u2022 До 3 районов, алерты\n\n"
        "👑 *Премиум* \u2014 15,000 ₸/мес\n"
        "\u2022 Все районы Астаны\n"
        "\u2022 Аналитика по ЖК\n"
        "\u2022 Персональные фильтры\n\n"
        "Оплата: /оплата",
        parse_mode=ParseMode.MARKDOWN,
    )


# --- /оплата ------------------------------------------------

@router.message(Command("оплата"))
async def cmd_payment(msg: Message):
    await msg.answer(
        "Для активации подписки:\n"
        f"1. Переведи на Kaspi: *{config.KASPI_PHONE}*\n"
        f"{('   ' + config.KASPI_NAME) if config.KASPI_NAME else ''}\n"
        "2. Отправь скриншот чека в этот чат\n"
        "3. Подписка активируется в течение часа\n\n"
        "💼 Стандарт \u2014 5,000 ₸/мес\n"
        "👑 Премиум \u2014 15,000 ₸/мес",
        parse_mode=ParseMode.MARKDOWN,
    )


# --- /статус ------------------------------------------------

@router.message(Command("статус"))
async def cmd_status(msg: Message):
    user = await check_and_expire_tariff(msg.from_user.id)
    if not user:
        await msg.answer("Напиши /start чтобы начать.")
        return
    left = days_left(user["sub_end"])
    tariff_line = f"📋 Тариф: *{user['tariff']}*"
    if user["tariff"] == "Триал":
        tariff_line += f"\n🎁 Триал: осталось {left} дней"
    elif user["tariff"] in ("Стандарт", "Премиум"):
        tariff_line += f"\n📅 Действует до: {user['sub_end']}"

    await msg.answer(
        f"👤 *Твой профиль:*\n\n"
        f"📍 Районы: {user['districts'] or 'не выбраны'}\n"
        f"🏠 Комнаты: {user['rooms_min'] or '?'}-{user['rooms_max'] or '?'}\n"
        f"💰 Бюджет: {format_price(user['budget_min'] or 0)}-{format_price(user['budget_max'] or 0)} ₸\n"
        f"📊 Макс цена/м\u00b2: {format_price(user['max_price_m2']) if user['max_price_m2'] else 'любая'}\n"
        f"🏗 ЖК: {user['target_zhk'] or 'любые'}\n\n"
        f"{tariff_line}\n\n"
        "Изменить: /настроить",
        parse_mode=ParseMode.MARKDOWN,
    )


# --- /помощь ------------------------------------------------

@router.message(Command("помощь", "help"))
async def cmd_help(msg: Message):
    await msg.answer(
        "📖 *Команды бота:*\n\n"
        "/старт \u2014 регистрация\n"
        "/настроить \u2014 настроить фильтры\n"
        "/топ \u2014 персональный топ-10\n"
        "/топ5 \u2014 топ-5\n"
        "/жк Название \u2014 поиск по ЖК\n"
        "/статус \u2014 профиль и подписка\n"
        "/тарифы \u2014 тарифные планы\n"
        "/оплата \u2014 реквизиты оплаты\n"
        "/помощь \u2014 эта справка",
        parse_mode=ParseMode.MARKDOWN,
    )


# --- Scheduled parsing + broadcast --------------------------

async def scheduled_parse_and_send():
    """Парсинг + валидация + рассылка (каждые 5 часов)."""
    logger.info("⏰ Запуск парсинга...")

    now = datetime.now(ASTANA_TZ)
    is_night = 0 <= now.hour < 6

    # 1. Scrape
    raw = await scrape_all_districts(max_pages=3)
    if not raw:
        logger.warning("Парсинг вернул 0 объявлений!")
        return

    # 2. Validate
    listings = validate_listings(raw)

    # 3. Update DB
    current_ids = set()
    for l in listings:
        current_ids.add(l["id"])
        await db.upsert_listing(l)

    await db.deactivate_missing(current_ids)
    logger.info(f"БД обновлена: {len(listings)} объявлений")

    # 4. Broadcast (skip at night)
    if is_night:
        logger.info("Ночь \u2014 рассылка пропущена")
        return

    users = await db.get_active_users()
    for user in users:
        try:
            # Check tariff expiry
            user = await check_and_expire_tariff(user["chat_id"])
            if not user:
                continue

            tariff = user["tariff"]
            limits = config.TARIFF_LIMITS.get(tariff, config.TARIFF_LIMITS["Бесплатный"])

            # Free users: only 2 times per day (06:00 and 16:00)
            if tariff == "Бесплатный" and now.hour not in (6, 16):
                continue

            filtered = await db.get_filtered_listings(user)
            if not filtered:
                continue

            # Best per ZhK
            best = {}
            for l in filtered:
                zhk = l["zhk"]
                if zhk not in best or l["price_m2"] < best[zhk]["price_m2"]:
                    best[zhk] = l
            top = sorted(best.values(), key=lambda x: x["price_m2"])[:limits["listings"]]

            if not top:
                continue

            header = (
                f"🔄 Обновление \u2014 {now.strftime('%d.%m.%Y %H:%M')}\n"
                f"📍 {user['districts'] or 'Все районы'}\n"
                f"Найдено: {len(filtered)} | Твой топ-{len(top)}:\n"
            )
            cards = []
            for i, l in enumerate(top, 1):
                phone_line = f"📞 {l['phone']}\n" if limits["phones"] and l.get("phone") else ""
                cards.append(
                    f"🏆 #{i} | {l['district']}\n"
                    f"🏗 {l['zhk']} | {l['rooms']} комн. {l['area']}м\u00b2\n"
                    f"💰 {format_price(l['price'])} ₸ | "
                    f"📊 {format_price(l['price_m2'])} ₸/м\u00b2\n"
                    f"{phone_line}"
                    f"🔗 {l['url']}"
                )

            footer = f"\n📊 {format_price(top[0]['price_m2'])}\u2013{format_price(top[-1]['price_m2'])} ₸/м\u00b2"
            if tariff == "Бесплатный":
                footer += "\n💡 Полный доступ: /тарифы"
            footer += trial_warning(user)

            text = header + "\n\n".join(cards) + footer
            await bot.send_message(user["chat_id"], text, disable_web_page_preview=True)
            await asyncio.sleep(0.5)  # Rate limiting

        except Exception as e:
            logger.error(f"Ошибка рассылки для {user['chat_id']}: {e}")


# --- Main ------------------------------------------------

async def main():
    await db.init_db()

    scheduler = AsyncIOScheduler(timezone="Asia/Almaty")
    scheduler.add_job(scheduled_parse_and_send, "cron", hour="2,6,11,16,21", minute=0)
    scheduler.start()
    logger.info("Планировщик запущен: 02:00, 06:00, 11:00, 16:00, 21:00 Астана")

    logger.info("Бот запущен!")
    try:
        await dp.start_polling(bot)
    finally:
        await db.close_db()
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
