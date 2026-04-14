import asyncpg
import logging
from datetime import date
import config

logger = logging.getLogger(__name__)
pool = None


async def init_db():
    global pool
    pool = await asyncpg.create_pool(config.DATABASE_URL, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id BIGINT PRIMARY KEY,
                name TEXT DEFAULT '',
                districts TEXT DEFAULT '',
                rooms_min INT DEFAULT 0,
                rooms_max INT DEFAULT 0,
                budget_min BIGINT DEFAULT 0,
                budget_max BIGINT DEFAULT 0,
                max_price_m2 BIGINT DEFAULT 0,
                target_zhk TEXT DEFAULT '',
                tariff TEXT DEFAULT 'Триал',
                sub_start DATE DEFAULT CURRENT_DATE,
                sub_end DATE DEFAULT (CURRENT_DATE + INTERVAL '7 days'),
                status TEXT DEFAULT 'Активный',
                setup_step TEXT DEFAULT 'start',
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS listings (
                id TEXT PRIMARY KEY,
                district TEXT,
                zhk TEXT,
                rooms INT DEFAULT 0,
                area REAL DEFAULT 0,
                price BIGINT DEFAULT 0,
                price_m2 BIGINT DEFAULT 0,
                phone TEXT DEFAULT '',
                url TEXT DEFAULT '',
                photo1 TEXT DEFAULT '',
                photo2 TEXT DEFAULT '',
                validation TEXT DEFAULT '✅ ОК',
                comment TEXT DEFAULT '',
                is_active BOOLEAN DEFAULT TRUE,
                first_seen DATE DEFAULT CURRENT_DATE,
                last_checked TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT REFERENCES users(chat_id),
                tariff TEXT,
                amount INT DEFAULT 0,
                payment_method TEXT DEFAULT '',
                paid_at TIMESTAMP DEFAULT NOW(),
                period_start DATE,
                period_end DATE,
                status TEXT DEFAULT 'Активна',
                kaspi_ref TEXT DEFAULT '',
                comment TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_listings_active ON listings(is_active);
            CREATE INDEX IF NOT EXISTS idx_listings_district ON listings(district);
        """)
    logger.info("Database initialized")


async def close_db():
    global pool
    if pool:
        await pool.close()


# ─── Users ─────────────────────────────────────────────────────────

async def get_user(chat_id: int) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE chat_id = $1", chat_id)
        return dict(row) if row else None


async def create_user(chat_id: int, name: str, tariff: str, sub_start: date, sub_end: date):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (chat_id, name, tariff, sub_start, sub_end)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (chat_id) DO NOTHING
        """, chat_id, name, tariff, sub_start, sub_end)


async def update_user(chat_id: int, **kwargs):
    if not kwargs:
        return
    sets = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(kwargs.keys()))
    vals = [chat_id] + list(kwargs.values())
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE users SET {sets} WHERE chat_id = $1", *vals)


async def get_active_users() -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users WHERE status = 'Активный'")
        return [dict(r) for r in rows]


# ─── Listings ──────────────────────────────────────────────────────

async def upsert_listing(l: dict):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO listings (id, district, zhk, rooms, area, price, price_m2,
                                  phone, url, photo1, photo2, validation, comment,
                                  is_active, last_checked)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,TRUE,NOW())
            ON CONFLICT (id) DO UPDATE SET
                is_active = TRUE, last_checked = NOW(),
                price = EXCLUDED.price, price_m2 = EXCLUDED.price_m2,
                validation = EXCLUDED.validation, comment = EXCLUDED.comment
        """, l["id"], l["district"], l["zhk"], l["rooms"], l["area"],
             l["price"], l["price_m2"], l.get("phone",""), l["url"],
             l.get("photo1",""), l.get("photo2",""),
             l["validation"], l.get("comment",""))


async def deactivate_missing(current_ids: set):
    if not current_ids:
        return
    async with pool.acquire() as conn:
        placeholders = ",".join(f"${i+1}" for i in range(len(current_ids)))
        await conn.execute(
            f"UPDATE listings SET is_active = FALSE WHERE is_active = TRUE AND id NOT IN ({placeholders})",
            *current_ids
        )


async def get_filtered_listings(user: dict) -> list[dict]:
    """Get listings matching user filters."""
    conditions = ["is_active = TRUE", "validation IN ('✅ ОК', '📞 Уточнено')", "zhk != 'Не указан'"]
    params = []
    idx = 1

    # District filter
    districts = user.get("districts", "")
    if districts and districts != "Все":
        district_list = [d.strip() for d in districts.split(",")]
        placeholders = ",".join(f"${idx + i}" for i in range(len(district_list)))
        conditions.append(f"district IN ({placeholders})")
        params.extend(district_list)
        idx += len(district_list)

    # Rooms filter
    if user.get("rooms_min") and user["rooms_min"] > 0:
        conditions.append(f"rooms >= ${idx}")
        params.append(user["rooms_min"])
        idx += 1
    if user.get("rooms_max") and user["rooms_max"] < 99:
        conditions.append(f"rooms <= ${idx}")
        params.append(user["rooms_max"])
        idx += 1

    # Budget filter
    if user.get("budget_min") and user["budget_min"] > 0:
        conditions.append(f"price >= ${idx}")
        params.append(user["budget_min"])
        idx += 1
    if user.get("budget_max") and user["budget_max"] > 0:
        conditions.append(f"price <= ${idx}")
        params.append(user["budget_max"])
        idx += 1

    # Max price per m2
    if user.get("max_price_m2") and user["max_price_m2"] > 0:
        conditions.append(f"price_m2 <= ${idx}")
        params.append(user["max_price_m2"])
        idx += 1

    # Target ZhK
    target = user.get("target_zhk", "")
    if target:
        zhk_list = [z.strip() for z in target.split(",")]
        placeholders = ",".join(f"${idx + i}" for i in range(len(zhk_list)))
        conditions.append(f"LOWER(zhk) IN ({placeholders})")
        params.extend([z.lower() for z in zhk_list])
        idx += len(zhk_list)

    where = " AND ".join(conditions)
    query = f"SELECT * FROM listings WHERE {where} ORDER BY price_m2 ASC LIMIT 100"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
        return [dict(r) for r in rows]


async def search_by_zhk(name: str) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM listings WHERE is_active = TRUE AND LOWER(zhk) LIKE $1 ORDER BY price_m2 ASC LIMIT 20",
            f"%{name.lower()}%"
        )
        return [dict(r) for r in rows]


# ─── Subscriptions ─────────────────────────────────────────────────

async def add_subscription(chat_id: int, tariff: str, amount: int, method: str,
                           start: date, end: date, comment: str = ""):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO subscriptions (chat_id, tariff, amount, payment_method,
                                       period_start, period_end, comment)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, chat_id, tariff, amount, method, start, end, comment)
import asyncpg
import logging
from datetime import date
import config

logger = logging.getLogger(__name__)
pool = None


async def init_db():
    global pool
    pool = await asyncpg.create_pool(config.DATABASE_URL, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id BIGINT PRIMARY KEY,
                name TEXT DEFAULT '',
                districts TEXT DEFAULT '',
                rooms_min INT DEFAULT 0,
                rooms_max INT DEFAULT 0,
                budget_min BIGINT DEFAULT 0,
                budget_max BIGINT DEFAULT 0,
                max_price_m2 BIGINT DEFAULT 0,
                target_zhk TEXT DEFAULT '',
                tariff TEXT DEFAULT 'Триал',
                sub_start DATE DEFAULT CURRENT_DATE,
                sub_end DATE DEFAULT (CURRENT_DATE + INTERVAL '7 days'),
                status TEXT DEFAULT 'Активный',
                setup_step TEXT DEFAULT 'start',
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS listings (
                id TEXT PRIMARY KEY,
                district TEXT,
                zhk TEXT,
                rooms INT DEFAULT 0,
                area REAL DEFAULT 0,
                price BIGINT DEFAULT 0,
                price_m2 BIGINT DEFAULT 0,
                phone TEXT DEFAULT '',
                url TEXT DEFAULT '',
                photo1 TEXT DEFAULT '',
                photo2 TEXT DEFAULT '',
                validation TEXT DEFAULT '✅ ОК',
                comment TEXT DEFAULT '',
                is_active BOOLEAN DEFAULT TRUE,
                first_seen DATE DEFAULT CURRENT_DATE,
                last_checked TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT REFERENCES users(chat_id),
                tariff TEXT,
                amount INT DEFAULT 0,
                payment_method TEXT DEFAULT '',
                paid_at TIMESTAMP DEFAULT NOW(),
                period_start DATE,
                period_end DATE,
                status TEXT DEFAULT 'Активна',
                kaspi_ref TEXT DEFAULT '',
                comment TEXT DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_listings_active ON listings(is_active);
            CREATE INDEX IF NOT EXISTS idx_listings_district ON listings(district);
        """)
    logger.info("Database initialized")


async def close_db():
    global pool
    if pool:
        await pool.close()


# ─── Users ───────────────────────────────────────────────────

async def get_user(chat_id: int) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE chat_id = $1", chat_id)
        return dict(row) if row else None


async def create_user(chat_id: int, name: str, tariff: str, sub_start: date, sub_end: date):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (chat_id, name, tariff, sub_start, sub_end)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (chat_id) DO NOTHING
        """, chat_id, name, tariff, sub_start, sub_end)


async def update_user(chat_id: int, **kwargs):
    if not kwargs:
        return
    sets = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(kwargs.keys()))
    vals = [chat_id] + list(kwargs.values())
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE users SET {sets} WHERE chat_id = $1", *vals)


async def get_active_users() -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users WHERE status = 'Активный'")
        return [dict(r) for r in rows]


# ─── Listings ────────────────────────────────────────────────

async def upsert_listing(l: dict):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO listings (id, district, zhk, rooms, area, price, price_m2, phone, url, photo1, photo2, validation, comment, is_active, last_checked)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,TRUE,NOW())
            ON CONFLICT (id) DO UPDATE SET
                is_active = TRUE,
                last_checked = NOW(),
                price = EXCLUDED.price,
                price_m2 = EXCLUDED.price_m2,
                validation = EXCLUDED.validation,
                comment = EXCLUDED.comment
        """, l["id"], l["district"], l["zhk"], l["rooms"], l["area"],
             l["price"], l["price_m2"], l.get("phone",""), l["url"],
             l.get("photo1",""), l.get("photo2",""), l["validation"], l.get("comment",""))


async def deactivate_missing(current_ids: set):
    if not current_ids:
        return
    async with pool.acquire() as conn:
        placeholders = ",".join(f"${i+1}" for i in range(len(current_ids)))
        await conn.execute(
            f"UPDATE listings SET is_active = FALSE WHERE is_active = TRUE AND id NOT IN ({placeholders})",
            *current_ids
        )


async def get_filtered_listings(user: dict) -> list[dict]:
    """Get listings matching user filters."""
    conditions = ["is_active = TRUE", "validation IN ('✅ ОК', '📞 Уточнено')"]
    params = []
    idx = 1

    # District filter
    districts = user.get("districts", "")
    if districts and districts != "Все":
        district_list = [d.strip() for d in districts.split(",")]
        placeholders = ",".join(f"${idx + i}" for i in range(len(district_list)))
        conditions.append(f"district IN ({placeholders})")
        params.extend(district_list)
        idx += len(district_list)

    # Rooms filter
    if user.get("rooms_min") and user["rooms_min"] > 0:
        conditions.append(f"rooms >= ${idx}")
        params.append(user["rooms_min"])
        idx += 1
    if user.get("rooms_max") and user["rooms_max"] < 99:
        conditions.append(f"rooms <= ${idx}")
        params.append(user["rooms_max"])
        idx += 1

    # Budget filter
    if user.get("budget_min") and user["budget_min"] > 0:
        conditions.append(f"price >= ${idx}")
        params.append(user["budget_min"])
        idx += 1
    if user.get("budget_max") and user["budget_max"] > 0:
        conditions.append(f"price <= ${idx}")
        params.append(user["budget_max"])
        idx += 1

    # Max price per m2
    if user.get("max_price_m2") and user["max_price_m2"] > 0:
        conditions.append(f"price_m2 <= ${idx}")
        params.append(user["max_price_m2"])
        idx += 1

    # Target ZhK
    target = user.get("target_zhk", "")
    if target:
        zhk_list = [z.strip() for z in target.split(",")]
        placeholders = ",".join(f"${idx + i}" for i in range(len(zhk_list)))
        conditions.append(f"LOWER(zhk) IN ({placeholders})")
        params.extend([z.lower() for z in zhk_list])
        idx += len(zhk_list)

    where = " AND ".join(conditions)
    query = f"SELECT * FROM listings WHERE {where} ORDER BY price_m2 ASC LIMIT 100"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
        return [dict(r) for r in rows]


async def search_by_zhk(name: str) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM listings WHERE is_active = TRUE AND LOWER(zhk) LIKE $1 ORDER BY price_m2 ASC LIMIT 20",
            f"%{name.lower()}%"
        )
        return [dict(r) for r in rows]


# ─── Subscriptions ───────────────────────────────────────────

async def add_subscription(chat_id: int, tariff: str, amount: int, method: str,
                           start: date, end: date, comment: str = ""):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO subscriptions (chat_id, tariff, amount, payment_method, period_start, period_end, comment)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, chat_id, tariff, amount, method, start, end, comment)
