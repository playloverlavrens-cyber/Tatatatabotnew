import os
import asyncio
import decimal
import json
import asyncpg
import aiohttp
import uuid
import pytz
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ================== CONFIG ==================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()

PAYSYNC_APIKEY = os.getenv("PAYSYNC_APIKEY", "").strip()
PAYSYNC_CLIENT_ID_RAW = os.getenv("PAYSYNC_CLIENT_ID", "").strip()
PAYSYNC_CURRENCY = os.getenv("PAYSYNC_CURRENCY", "UAH").strip().upper()

CRYPTO_PAY_API_TOKEN = os.getenv("CRYPTO_PAY_API_TOKEN", "").strip()
CRYPTO_PAY_BASE_URL = os.getenv("CRYPTO_PAY_BASE_URL", "https://pay.crypt.bot/api").strip().rstrip("/")
CRYPTO_PAY_FIAT = os.getenv("CRYPTO_PAY_FIAT", "UAH").strip().upper()
CRYPTO_PAY_ACCEPTED_ASSETS = os.getenv("CRYPTO_PAY_ACCEPTED_ASSETS", "USDT,TON,BTC,ETH").strip()

PAYMENT_TIMEOUT_MINUTES = int(os.getenv("PAYMENT_TIMEOUT_MINUTES", "15"))
RESERVATION_MINUTES = int(os.getenv("RESERVATION_MINUTES", "15"))

UKRAINE_TZ = pytz.timezone('Europe/Kyiv')

required_env = {
    "BOT_TOKEN": BOT_TOKEN,
    "DATABASE_URL": DATABASE_URL,
    "ADMIN_ID": ADMIN_ID_RAW,
    "PAYSYNC_APIKEY": PAYSYNC_APIKEY,
    "PAYSYNC_CLIENT_ID": PAYSYNC_CLIENT_ID_RAW,
}

for key, value in required_env.items():
    if not value:
        raise RuntimeError(f"Missing env: {key}")

if not ADMIN_ID_RAW.lstrip("-").isdigit():
    raise RuntimeError("ADMIN_ID must be integer")

if not PAYSYNC_CLIENT_ID_RAW.isdigit():
    raise RuntimeError("PAYSYNC_CLIENT_ID must be numeric")

ADMIN_ID = int(ADMIN_ID_RAW)
CLIENT_ID = int(PAYSYNC_CLIENT_ID_RAW)
UAH = "₴"

# ================== TEXTS ==================
MAIN_TEXT = """🇮🇹 Добро пожаловать в «Итальянский Квадрат»

✨ Наши преимущества:
• Товары топ качества
• Регулярные пополнения
• Огромный выбор товара
• Быстрые отправки
• Техподдержка 24/7

🌐 Контакты:
Бот: @ititititbot_bot
Поддержка: @italiansquare

💳 Баланс: {balance} {uah}
📦 Заказов: {orders}"""

PROFILE_TEXT = "👤 Профиль\n\n💳 Баланс: {balance} {uah}\n📦 Заказов: {orders}"
HELP_TEXT = "Если возникли вопросы, свяжись с оператором:\n@italiansquare"
ITEM_TEXT = "✅ Выбран: {name}\n💰 Цена: {price} {uah}\n\n{desc}"
TOPUP_TEXT = f"💳 Введите сумму пополнения в гривнах ({UAH}) целым числом (минимум 10):\nНапример: 150"

# ================== DB ==================
pool: asyncpg.Pool | None = None


def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_ukraine_time() -> datetime:
    """Получить текущее время в Киеве"""
    utc_time = utc_now()
    ukraine_time = utc_time.astimezone(UKRAINE_TZ)
    return ukraine_time


def format_ukraine_time(dt: datetime | None) -> str:
    """Форматировать время в киевское"""
    if not dt:
        return "неизвестно"
    # Если это UTC время, конвертируем в Киев
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    ukraine_time = dt.astimezone(UKRAINE_TZ)
    return ukraine_time.strftime("%d.%m.%Y %H:%M")


def parse_amount(text: str) -> int | None:
    try:
        raw = (text or "").strip().replace(",", ".")
        d = decimal.Decimal(raw)
        if d <= 0:
            return None
        if d != d.quantize(decimal.Decimal("1")):
            return None
        return int(d)
    except Exception:
        return None


async def db_init() -> None:
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)

    async with pool.acquire() as con:
        await con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                balance NUMERIC(12,2) DEFAULT 0,
                orders_count INT DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await con.execute("""
            CREATE TABLE IF NOT EXISTS products (
                code TEXT PRIMARY KEY,
                city TEXT NOT NULL,
                name TEXT NOT NULL,
                price NUMERIC(12,2) DEFAULT 0,
                link TEXT DEFAULT '',
                description TEXT DEFAULT '',
                photo_id TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                reserved_by BIGINT,
                reserved_until TIMESTAMPTZ,
                sold_to BIGINT,
                sold_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await con.execute("""
            CREATE TABLE IF NOT EXISTS purchases (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                product_code TEXT,
                item_name TEXT,
                price NUMERIC(12,2),
                link TEXT,
                provider TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await con.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                trade_id TEXT PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                kind TEXT,
                amount_int INT,
                currency TEXT DEFAULT 'UAH',
                product_code TEXT,
                provider TEXT,
                status TEXT DEFAULT 'wait',
                expires_at TIMESTAMPTZ,
                paid_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await con.execute("""
            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY,
                discount_amount INT,
                max_uses INT,
                current_uses INT DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE,
                created_by BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)


async def ensure_user(uid: int) -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING",
            uid
        )


async def get_stats(uid: int) -> tuple[decimal.Decimal, int]:
    assert pool is not None
    async with pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT balance, orders_count FROM users WHERE user_id=$1",
            uid
        )
    if not row:
        return decimal.Decimal("0.00"), 0
    return decimal.Decimal(row["balance"]), row["orders_count"]


async def cleanup_expired() -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute("""
            UPDATE products
            SET reserved_by = NULL, reserved_until = NULL
            WHERE reserved_until IS NOT NULL
              AND reserved_until < NOW()
              AND sold_at IS NULL
        """)

        await con.execute("""
            UPDATE invoices
            SET status = 'expired'
            WHERE status = 'wait'
              AND expires_at IS NOT NULL
              AND expires_at < NOW()
        """)


async def background_cleanup():
    while True:
        try:
            await cleanup_expired()
        except Exception as e:
            print(f"[cleanup] {e}")
        await asyncio.sleep(60)

# ================== KEYBOARDS ==================
def bottom_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Главная"), KeyboardButton(text="👤 Профиль")],
            [KeyboardButton(text="❓ Помощь"), KeyboardButton(text="💼 Работа")]
        ],
        resize_keyboard=True
    )


def inline_city() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Одесса", callback_data="city:odesa")],
            [InlineKeyboardButton(text="Львов", callback_data="city:lviv")]
        ]
    )


def inline_products(rows: list, city: str) -> InlineKeyboardMarkup:
    if not rows:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="📭 Нет в наличии\nСкоро пополнение", callback_data="noop")]]
        )

    kb = []
    for r in rows:
        kb.append([
            InlineKeyboardButton(
                text=f"{r['name']} — {decimal.Decimal(r['price']):.0f} {UAH}",
                callback_data=f"prod:{city}:{r['code']}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def inline_pay(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Баланс", callback_data=f"pay:bal:{code}")],
            [InlineKeyboardButton(text="Карта (PaySync)", callback_data=f"pay:card:{code}")],
            [InlineKeyboardButton(text="Крипто", callback_data=f"pay:crypto:{code}")]
        ]
    )


def inline_profile() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Пополнить", callback_data="profile:topup")],
            [InlineKeyboardButton(text="🎟 Промокод", callback_data="profile:promo")],
            [InlineKeyboardButton(text="📜 История", callback_data="profile:history")]
        ]
    )


def inline_check(trade_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"check:{trade_id}")]
        ]
    )

# ================== PAYMENT ==================
async def paysync_create(amount: int, data: str) -> dict:
    """Создание заявки на оплату через PaySync"""
    url = f"https://paysync.bot/api/client{CLIENT_ID}/amount{amount}/currencyUAH"
    headers = {"apikey": PAYSYNC_APIKEY}
    params = {"data": data}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params, timeout=30) as resp:
            raw_text = await resp.text()

            if resp.status >= 400:
                raise RuntimeError(f"PaySync HTTP {resp.status}: {raw_text[:300]}")

            try:
                return json.loads(raw_text)
            except Exception as e:
                raise RuntimeError(f"PaySync вернул не JSON: {raw_text[:300]}")


async def paysync_check(trade_id: str) -> dict:
    """Проверка статуса платежа в PaySync"""
    url = f"https://paysync.bot/gettrans/{trade_id}"
    headers = {"apikey": PAYSYNC_APIKEY}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=30) as resp:
            raw_text = await resp.text()

            if resp.status >= 400:
                raise RuntimeError(f"PaySync HTTP {resp.status}: {raw_text[:300]}")

            try:
                return json.loads(raw_text)
            except Exception as e:
                raise RuntimeError(f"PaySync check вернул не JSON: {raw_text[:300]}")


async def crypto_pay_create(amount: int, description: str) -> dict:
    """Создание инвойса через Crypto Pay API"""
    url = f"{CRYPTO_PAY_BASE_URL}/invoices"
    headers = {
        "Crypto-Pay-API-Token": CRYPTO_PAY_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    payload = {
        "amount": str(amount),
        "asset": CRYPTO_PAY_ACCEPTED_ASSETS.split(",")[0],
        "description": description,
        "fiat": CRYPTO_PAY_FIAT,
        "accepted_assets": CRYPTO_PAY_ACCEPTED_ASSETS.split(",")
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload, timeout=30) as resp:
            raw_text = await resp.text()

            if resp.status >= 400:
                raise RuntimeError(f"Crypto Pay HTTP {resp.status}: {raw_text[:300]}")

            try:
                return json.loads(raw_text)
            except Exception:
                raise RuntimeError(f"Crypto Pay вернул не JSON: {raw_text[:300]}")


async def crypto_pay_check(invoice_id: str) -> dict:
    """Проверка статуса инвойса в Crypto Pay"""
    url = f"{CRYPTO_PAY_BASE_URL}/invoices/{invoice_id}"
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_API_TOKEN}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=30) as resp:
            raw_text = await resp.text()

            if resp.status >= 400:
                raise RuntimeError(f"Crypto Pay check HTTP {resp.status}: {raw_text[:300]}")

            try:
                return json.loads(raw_text)
            except Exception:
                raise RuntimeError(f"Crypto Pay check вернул не JSON: {raw_text[:300]}")


def extract_trade_id(js: dict) -> str:
    trade_id = js.get("trade") or js.get("result", {}).get("invoice_id")
    if trade_id is None:
        raise RuntimeError(f"Нет trade_id в ответе: {js}")
    return str(trade_id)


def extract_card_number(js: dict) -> str:
    card = js.get("card_number")
    if not card:
        return "не получена"
    return str(card).strip()


def extract_status(js: dict) -> str:
    status = js.get("status") or js.get("result", {}).get("status")
    return str(status or "").strip().lower()


def extract_amount(js: dict, fallback: int) -> int:
    raw = js.get("amount", fallback) or js.get("result", {}).get("amount", fallback)
    try:
        return int(raw)
    except Exception:
        try:
            return int(decimal.Decimal(str(raw)))
        except Exception:
            return fallback


def extract_paysync_time(js: dict) -> str:
    return str(js.get("time", "")).strip()


def extract_crypto_link(js: dict) -> str:
    """Извлечь ссылку на оплату из Crypto Pay"""
    return js.get("result", {}).get("pay_url") or js.get("pay_url") or ""

# ================== FSM ==================
class TopupStates(StatesGroup):
    waiting_amount = State()


class PromoStates(StatesGroup):
    waiting_code = State()


class AddProductStates(StatesGroup):
    waiting_city = State()
    waiting_name = State()
    waiting_price = State()
    waiting_description = State()
    waiting_photo = State()

# ================== BOT ==================
dp = Dispatcher(storage=MemoryStorage())


@dp.message(CommandStart())
async def cmd_start(message: Message):
    await ensure_user(message.from_user.id)
    bal, orders = await get_stats(message.from_user.id)
    text = MAIN_TEXT.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    await message.answer(text, reply_markup=bottom_menu())


@dp.message(F.text.contains("Главная"))
async def btn_main(message: Message):
    await ensure_user(message.from_user.id)
    bal, orders = await get_stats(message.from_user.id)
    text = MAIN_TEXT.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    await message.answer(text, reply_markup=inline_city())


@dp.message(F.text.contains("Профиль"))
async def btn_profile(message: Message):
    await ensure_user(message.from_user.id)
    bal, orders = await get_stats(message.from_user.id)
    text = PROFILE_TEXT.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    await message.answer(text, reply_markup=inline_profile())


@dp.message(F.text.contains("Помощь"))
async def btn_help(message: Message):
    await message.answer(HELP_TEXT, reply_markup=bottom_menu())


@dp.message(F.text.contains("Работа"))
async def btn_work(message: Message):
    await message.answer("Ищем ответственных. Напиши:\n@italiansquare", reply_markup=bottom_menu())


@dp.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()


@dp.callback_query(F.data.startswith("city:"))
async def cb_city(call: CallbackQuery):
    await call.answer()
    city = call.data.split(":")[1]
    city_name = "Одесса" if city == "odesa" else "Львов"
    
    assert pool is not None

    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT code, name, price
            FROM products
            WHERE city=$1
              AND is_active=TRUE
              AND sold_at IS NULL
              AND (reserved_until IS NULL OR reserved_until < NOW())
            ORDER BY created_at DESC
            LIMIT 50
        """, city)

    if not rows:
        await call.message.answer(f"📭 В {city_name} нет товара.\nСкоро пополнение!")
        return

    await call.message.answer(f"Товары {city_name}:", reply_markup=inline_products(rows, city))


@dp.callback_query(F.data.startswith("prod:"))
async def cb_product(call: CallbackQuery):
    await call.answer()
    _, city, code = call.data.split(":", 2)

    assert pool is not None
    async with pool.acquire() as con:
        row = await con.fetchrow("""
            SELECT code, city, name, price, description, photo_id, reserved_by, reserved_until, sold_at
            FROM products
            WHERE code=$1 AND city=$2 AND is_active=TRUE
        """, code, city)

    if not row:
        await call.message.answer("❌ Товар не найден")
        return

    if row["sold_at"] is not None:
        await call.message.answer("❌ Товар уже продан")
        return

    now = utc_now()
    if row["reserved_until"] and row["reserved_until"] > now and row["reserved_by"] != call.from_user.id:
        reserved_min = int((row["reserved_until"] - now).total_seconds() / 60)
        await call.message.answer(f"⏳ Товар зарезервирован. Попробуй через {reserved_min} мин")
        return

    text = ITEM_TEXT.format(
        name=row["name"],
        price=f"{decimal.Decimal(row['price']):.0f}",
        desc=row["description"] or "—",
        uah=UAH
    )
    
    if row["photo_id"]:
        await call.message.answer_photo(
            photo=row["photo_id"],
            caption=text,
            reply_markup=inline_pay(code)
        )
    else:
        await call.message.answer(text, reply_markup=inline_pay(code))


@dp.callback_query(F.data == "profile:topup")
async def cb_topup(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(TopupStates.waiting_amount)
    await call.message.answer(TOPUP_TEXT)


@dp.message(TopupStates.waiting_amount)
async def topup_amount(message: Message, state: FSMContext):
    amount = parse_amount(message.text)
    if not amount or amount < 10:
        await message.answer(f"❌ Минимум 10 {UAH}")
        return

    await ensure_user(message.from_user.id)

    try:
        nonce = uuid.uuid4().hex[:12]
        payload = f"topup:{message.from_user.id}:{nonce}"
        js = await paysync_create(amount, payload)

        trade_id = extract_trade_id(js)
        card_number = extract_card_number(js)
        real_amount = extract_amount(js, amount)
        paysync_time = extract_paysync_time(js)
        expires_at = get_ukraine_time() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO invoices(
                    trade_id, user_id, kind, amount_int, currency, product_code, provider, status, expires_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (trade_id) DO NOTHING
            """,
                trade_id,
                message.from_user.id,
                "topup",
                real_amount,
                PAYSYNC_CURRENCY,
                None,
                "paysync",
                "wait",
                expires_at
            )

        expire_time = format_ukraine_time(expires_at) if not paysync_time else paysync_time
        
        text = (
            f"💳 Пополнение баланса\n\n"
            f"Заявка: {trade_id}\n"
            f"Карта: {card_number}\n"
            f"Сумма: {real_amount} {PAYSYNC_CURRENCY}\n"
            f"Срок: до {expire_time}\n\n"
            "⚠️ Оплачивай точно указанную сумму одним платежом"
        )
        await message.answer(text, reply_markup=inline_check(trade_id))

    except Exception as e:
        print(f"[TOPUP ERROR] {repr(e)}")
        await message.answer(f"❌ Ошибка создания платежа: {str(e)[:200]}")
    finally:
        await state.clear()


@dp.callback_query(F.data.startswith("pay:bal:"))
async def cb_pay_balance(call: CallbackQuery):
    await call.answer()
    code = call.data.split(":")[2]
    uid = call.from_user.id

    try:
        assert pool is not None
        async with pool.acquire() as con:
            async with con.transaction():
                product = await con.fetchrow("""
                    SELECT *
                    FROM products
                    WHERE code=$1
                    FOR UPDATE
                """, code)

                if not product:
                    await call.message.answer("❌ Товар не найден")
                    return

                if product["sold_at"] is not None:
                    await call.message.answer("❌ Товар уже продан")
                    return

                now = utc_now()
                if product["reserved_until"] and product["reserved_until"] > now and product["reserved_by"] != uid:
                    await call.message.answer("⏳ Товар зарезервирован другим покупателем")
                    return

                user = await con.fetchrow("SELECT * FROM users WHERE user_id=$1 FOR UPDATE", uid)
                if not user:
                    await con.execute("INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING", uid)
                    user = await con.fetchrow("SELECT * FROM users WHERE user_id=$1 FOR UPDATE", uid)

                price = decimal.Decimal(product["price"])
                balance = decimal.Decimal(user["balance"])

                if balance < price:
                    await call.message.answer(
                        f"❌ Недостаточно средств\n"
                        f"Нужно: {price:.0f} {UAH}\n"
                        f"Есть: {balance:.0f} {UAH}"
                    )
                    return

                await con.execute("""
                    UPDATE users
                    SET balance = balance - $2,
                        orders_count = orders_count + 1
                    WHERE user_id = $1
                """, uid, price)

                await con.execute("""
                    UPDATE products
                    SET sold_to=$2, sold_at=NOW(), reserved_by=NULL, reserved_until=NULL
                    WHERE code=$1
                """, code, uid)

                await con.execute("""
                    INSERT INTO purchases(user_id, product_code, item_name, price, link, provider)
                    VALUES($1,$2,$3,$4,$5,$6)
                """, uid, code, product["name"], price, product["link"], "balance")

        text = (
            f"✅ Оплата прошла\n\n"
            f"📦 {product['name']}\n"
            f"Сумма: {price:.0f} {UAH}\n\n"
            f"🔗 {product['link']}"
        )
        
        if product["photo_id"]:
            await call.message.answer_photo(photo=product["photo_id"], caption=text)
        else:
            await call.message.answer(text)

    except Exception as e:
        print(f"[PAY BALANCE ERROR] {repr(e)}")
        await call.message.answer(f"❌ Ошибка платежа: {str(e)[:200]}")


@dp.callback_query(F.data.startswith("pay:card:"))
async def cb_pay_card(call: CallbackQuery):
    await call.answer()
    code = call.data.split(":")[2]
    uid = call.from_user.id

    try:
        assert pool is not None

        async with pool.acquire() as con:
            async with con.transaction():
                product = await con.fetchrow("""
                    SELECT *
                    FROM products
                    WHERE code=$1
                    FOR UPDATE
                """, code)

                if not product:
                    await call.message.answer("❌ Товар не найден")
                    return

                if product["sold_at"] is not None:
                    await call.message.answer("❌ Товар уже продан")
                    return

                now = utc_now()
                if product["reserved_until"] and product["reserved_until"] > now and product["reserved_by"] != uid:
                    await call.message.answer("⏳ Товар зарезервирован другим покупателем")
                    return

                reserve_until = now + timedelta(minutes=RESERVATION_MINUTES)

                await con.execute("""
                    UPDATE products
                    SET reserved_by=$2, reserved_until=$3
                    WHERE code=$1
                """, code, uid, reserve_until)

                price = int(decimal.Decimal(product["price"]))
                nonce = uuid.uuid4().hex[:12]
                payload = f"buy:{uid}:{code}:{nonce}"
                product_name = product["name"]

        js = await paysync_create(price, payload)

        trade_id = extract_trade_id(js)
        card_number = extract_card_number(js)
        real_amount = extract_amount(js, price)
        paysync_time = extract_paysync_time(js)
        expires_at = get_ukraine_time() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO invoices(
                    trade_id, user_id, kind, amount_int, currency, product_code, provider, status, expires_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (trade_id) DO NOTHING
            """,
                trade_id,
                uid,
                "purchase",
                real_amount,
                PAYSYNC_CURRENCY,
                code,
                "paysync",
                "wait",
                expires_at
            )

        expire_time = format_ukraine_time(expires_at) if not paysync_time else paysync_time
        
        text = (
            f"💳 Оплата по карте\n\n"
            f"📦 {product_name}\n"
            f"Заявка: {trade_id}\n"
            f"Карта: {card_number}\n"
            f"Сумма: {real_amount} {PAYSYNC_CURRENCY}\n"
            f"Срок: до {expire_time}\n\n"
            f"⏱ Зарезервировано на {RESERVATION_MINUTES} мин\n"
            "⚠️ Оплачивай точно указанную сумму одним платежом"
        )
        await call.message.answer(text, reply_markup=inline_check(trade_id))

    except Exception as e:
        if pool:
            async with pool.acquire() as con:
                await con.execute("""
                    UPDATE products
                    SET reserved_by=NULL, reserved_until=NULL
                    WHERE code=$1 AND reserved_by=$2 AND sold_at IS NULL
                """, code, uid)

        print(f"[CARD ERROR] {repr(e)}")
        await call.message.answer(f"❌ Ошибка создания платежа: {str(e)[:200]}")


@dp.callback_query(F.data.startswith("pay:crypto:"))
async def cb_pay_crypto(call: CallbackQuery):
    await call.answer()
    code = call.data.split(":")[2]
    uid = call.from_user.id

    try:
        assert pool is not None

        async with pool.acquire() as con:
            async with con.transaction():
                product = await con.fetchrow("""
                    SELECT *
                    FROM products
                    WHERE code=$1
                    FOR UPDATE
                """, code)

                if not product:
                    await call.message.answer("❌ Товар не найден")
                    return

                if product["sold_at"] is not None:
                    await call.message.answer("❌ Товар уже продан")
                    return

                now = utc_now()
                if product["reserved_until"] and product["reserved_until"] > now and product["reserved_by"] != uid:
                    await call.message.answer("⏳ Товар зарезервирован другим покупателем")
                    return

                reserve_until = now + timedelta(minutes=RESERVATION_MINUTES)

                await con.execute("""
                    UPDATE products
                    SET reserved_by=$2, reserved_until=$3
                    WHERE code=$1
                """, code, uid, reserve_until)

                price = int(decimal.Decimal(product["price"]))
                nonce = uuid.uuid4().hex[:12]
                payload = f"buy_crypto:{uid}:{code}:{nonce}"
                product_name = product["name"]

        # Создаём инвойс через Crypto Pay
        js = await crypto_pay_create(price, f"Покупка: {product_name}")

        invoice_id = extract_trade_id(js)
        crypto_link = extract_crypto_link(js)
        real_amount = extract_amount(js, price)
        expires_at = get_ukraine_time() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO invoices(
                    trade_id, user_id, kind, amount_int, currency, product_code, provider, status, expires_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (trade_id) DO NOTHING
            """,
                invoice_id,
                uid,
                "purchase",
                real_amount,
                CRYPTO_PAY_FIAT,
                code,
                "crypto",
                "wait",
                expires_at
            )

        text = (
            f"🪙 Крипто-платёж\n\n"
            f"📦 {product_name}\n"
            f"Сумма: {real_amount} {CRYPTO_PAY_FIAT}\n"
            f"Срок: до {format_ukraine_time(expires_at)}\n\n"
            f"⏱ Зарезервировано на {RESERVATION_MINUTES} мин"
        )
        
        if crypto_link:
            await call.message.answer(f"{text}\n\n🔗 [Оплатить]({crypto_link})", 
                                    parse_mode="Markdown",
                                    reply_markup=inline_check(invoice_id))
        else:
            await call.message.answer(f"{text}\n\nОшибка получения ссылки оплаты",
                                    reply_markup=inline_check(invoice_id))

    except Exception as e:
        if pool:
            async with pool.acquire() as con:
                await con.execute("""
                    UPDATE products
                    SET reserved_by=NULL, reserved_until=NULL
                    WHERE code=$1 AND reserved_by=$2 AND sold_at IS NULL
                """, code, uid)

        print(f"[CRYPTO ERROR] {repr(e)}")
        await call.message.answer(f"❌ Ошибка крипто-платежа: {str(e)[:200]}")


@dp.callback_query(F.data.startswith("check:"))
async def cb_check(call: CallbackQuery):
    await call.answer()
    trade_id = call.data.split(":", 1)[1]

    try:
        assert pool is not None

        async with pool.acquire() as con:
            inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)

        if not inv:
            await call.message.answer("❌ Заявка не найдена")
            return

        if inv["status"] == "paid":
            await call.message.answer("✅ Заявка уже обработана")
            return

        # Проверка в зависимости от провайдера
        if inv["provider"] == "paysync":
            js = await paysync_check(trade_id)
        elif inv["provider"] == "crypto":
            js = await crypto_pay_check(trade_id)
        else:
            raise RuntimeError("Неизвестный провайдер")

        status = extract_status(js)

        if status in {"paid", "success", "succeeded", "completed", "succeeded"}:
            async with pool.acquire() as con:
                async with con.transaction():
                    inv = await con.fetchrow("""
                        SELECT *
                        FROM invoices
                        WHERE trade_id=$1
                        FOR UPDATE
                    """, trade_id)

                    if not inv:
                        await call.message.answer("❌ Заявка не найдена")
                        return

                    if inv["status"] == "paid":
                        await call.message.answer("✅ Платёж уже зачислен")
                        return

                    if inv["kind"] == "topup":
                        await con.execute("""
                            UPDATE users
                            SET balance = balance + $2
                            WHERE user_id = $1
                        """, inv["user_id"], inv["amount_int"])

                        await con.execute("""
                            UPDATE invoices
                            SET status='paid', paid_at=NOW()
                            WHERE trade_id=$1
                        """, trade_id)

                        await call.message.answer(
                            f"✅ Баланс пополнен\n\n"
                            f"+ {inv['amount_int']} {inv['currency']}"
                        )
                        return

                    if inv["kind"] == "purchase":
                        product = await con.fetchrow("""
                            SELECT *
                            FROM products
                            WHERE code=$1
                            FOR UPDATE
                        """, inv["product_code"])

                        if not product:
                            await call.message.answer("❌ Товар не найден")
                            return

                        if product["sold_at"] is not None:
                            await con.execute("""
                                UPDATE invoices
                                SET status='paid', paid_at=NOW()
                                WHERE trade_id=$1
                            """, trade_id)

                            await call.message.answer(
                                "⚠️ Платёж найден, но товар уже продан\n"
                                "Напиши: @italiansquare"
                            )
                            return

                        await con.execute("""
                            UPDATE products
                            SET sold_to=$2,
                                sold_at=NOW(),
                                reserved_by=NULL,
                                reserved_until=NULL
                            WHERE code=$1
                        """, inv["product_code"], inv["user_id"])

                        await con.execute("""
                            UPDATE users
                            SET orders_count = orders_count + 1
                            WHERE user_id = $1
                        """, inv["user_id"])

                        await con.execute("""
                            INSERT INTO purchases(user_id, product_code, item_name, price, link, provider)
                            VALUES($1,$2,$3,$4,$5,$6)
                        """, inv["user_id"], product["code"], product["name"], product["price"], product["link"], inv["provider"])

                        await con.execute("""
                            UPDATE invoices
                            SET status='paid', paid_at=NOW()
                            WHERE trade_id=$1
                        """, trade_id)

                        text = (
                            f"✅ Товар получен\n\n"
                            f"📦 {product['name']}\n\n"
                            f"🔗 {product['link']}"
                        )
                        
                        if product["photo_id"]:
                            await call.message.answer_photo(photo=product["photo_id"], caption=text)
                        else:
                            await call.message.answer(text)
                        return

        elif status in {"expired", "cancelled", "canceled", "failed"}:
            async with pool.acquire() as con:
                async with con.transaction():
                    inv = await con.fetchrow("""
                        SELECT *
                        FROM invoices
                        WHERE trade_id=$1
                        FOR UPDATE
                    """, trade_id)

                    if inv and inv["kind"] == "purchase" and inv["product_code"]:
                        await con.execute("""
                            UPDATE products
                            SET reserved_by=NULL, reserved_until=NULL
                            WHERE code=$1
                              AND sold_at IS NULL
                              AND reserved_by=$2
                        """, inv["product_code"], inv["user_id"])

                    await con.execute("""
                        UPDATE invoices
                        SET status=$2
                        WHERE trade_id=$1
                    """, trade_id, status)

            await call.message.answer(f"❌ Платёж отклонён: {status}")
            return

        else:
            await call.message.answer("⏳ Платёж не подтвержден. Попробуй через минуту")
            return

    except Exception as e:
        print(f"[CHECK ERROR] {repr(e)}")
        await call.message.answer(f"❌ Ошибка проверки: {str(e)[:200]}")


@dp.callback_query(F.data == "profile:history")
async def cb_history(call: CallbackQuery):
    await call.answer()
    assert pool is not None

    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT item_name, link, price, provider, created_at
            FROM purchases
            WHERE user_id=$1
            ORDER BY created_at DESC
            LIMIT 20
        """, call.from_user.id)

    if not rows:
        await call.message.answer("📭 История пуста")
        return

    text = "📜 История покупок:\n\n"
    for r in rows:
        dt = format_ukraine_time(r["created_at"])
        price = decimal.Decimal(r["price"])
        provider_icon = "💳" if r["provider"] == "paysync" else "🪙" if r["provider"] == "crypto" else "💰"
        text += f"{provider_icon} {r['item_name']} — {price:.0f} {UAH}\n{r['link']}\n{dt}\n\n"

    await call.message.answer(text)


@dp.callback_query(F.data == "profile:promo")
async def cb_promo(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PromoStates.waiting_code)
    await call.message.answer("🎟 Введи промокод:")


@dp.message(PromoStates.waiting_code)
async def promo_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    
    try:
        assert pool is not None
        async with pool.acquire() as con:
            async with con.transaction():
                promo = await con.fetchrow("""
                    SELECT * FROM promo_codes
                    WHERE code=$1 AND is_active=TRUE
                    FOR UPDATE
                """, code)

                if not promo:
                    await message.answer("❌ Промокод не найден")
                    await state.clear()
                    return

                if promo["current_uses"] >= promo["max_uses"]:
                    await message.answer("❌ Промокод использован полностью")
                    await state.clear()
                    return

                user_id = message.from_user.id
                
                # Проверка, использовал ли пользователь этот промокод
                existing = await con.fetchval("""
                    SELECT COUNT(*) FROM purchases
                    WHERE user_id=$1 AND provider='promo' AND product_code=$2
                """, user_id, code)

                if existing > 0:
                    await message.answer("❌ Ты уже использовал этот промокод")
                    await state.clear()
                    return

                # Зачисляем баланс
                await con.execute("""
                    UPDATE users
                    SET balance = balance + $2
                    WHERE user_id = $1
                """, user_id, promo["discount_amount"])

                # Увеличиваем счётчик использований
                await con.execute("""
                    UPDATE promo_codes
                    SET current_uses = current_uses + 1
                    WHERE code=$1
                """, code)

                # Записываем в покупки для статистики
                await con.execute("""
                    INSERT INTO purchases(user_id, item_name, price, provider, product_code)
                    VALUES($1,$2,$3,$4,$5)
                """, user_id, "Промокод", promo["discount_amount"], "promo", code)

                await message.answer(
                    f"✅ Промокод активирован\n\n"
                    f"+ {promo['discount_amount']} {UAH}"
                )
    
    except Exception as e:
        print(f"[PROMO ERROR] {repr(e)}")
        await message.answer(f"❌ Ошибка: {str(e)[:200]}")
    finally:
        await state.clear()


# ================== ADMIN COMMANDS ==================
@dp.message(F.text.startswith("/addproduct"))
async def cmd_add(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        raw = message.text.replace("/addproduct", "", 1).strip()
        parts = [p.strip() for p in raw.split("|")]

        if len(parts) < 5:
            await message.answer("Формат: /addproduct город | код | название | цена | ссылка | описание")
            return

        city, code, name, price_raw, link = parts[:5]
        desc = parts[5] if len(parts) > 5 else ""

        # Валидация города
        if city.lower() not in ["odesa", "одесса", "lviv", "львов"]:
            await message.answer("Допустимые города: odesa, lviv")
            return

        city = "odesa" if city.lower() in ["odesa", "одесса"] else "lviv"

        price = decimal.Decimal(price_raw)

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO products(code, city, name, price, link, description, is_active)
                VALUES($1,$2,$3,$4,$5,$6,TRUE)
                ON CONFLICT(code) DO UPDATE SET
                    city=EXCLUDED.city,
                    name=EXCLUDED.name,
                    price=EXCLUDED.price,
                    link=EXCLUDED.link,
                    description=EXCLUDED.description,
                    is_active=TRUE,
                    sold_to=NULL,
                    sold_at=NULL,
                    reserved_by=NULL,
                    reserved_until=NULL
            """, code, city, name, price, link, desc)

        await message.answer(f"✅ Товар добавлен: {code}")

    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)[:200]}")


@dp.message(F.text.startswith("/delproduct"))
async def cmd_del(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        code = message.text.replace("/delproduct", "").strip()
        if not code:
            await message.answer("Формат: /delproduct КОД")
            return

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute("UPDATE products SET is_active=FALSE WHERE code=$1", code)

        await message.answer(f"✅ Товар {code} отключен")

    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)[:200]}")


@dp.message(F.text.startswith("/products"))
async def cmd_products(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        assert pool is not None
        async with pool.acquire() as con:
            rows = await con.fetch("""
                SELECT city, code, name, price, is_active, reserved_until, sold_at
                FROM products
                ORDER BY created_at DESC
                LIMIT 100
            """)

        if not rows:
            await message.answer("Товаров нет")
            return

        text = "📦 Товары:\n\n"
        for r in rows:
            state = "✅ ON" if r["is_active"] else "❌ OFF"
            if r["sold_at"] is not None:
                state = "✔️ ПРОДАН"
            elif r["reserved_until"] is not None and r["reserved_until"] > utc_now():
                state = f"⏳ ЗАРЕЗЕРВ. до {format_ukraine_time(r['reserved_until'])}"

            city_name = "Одесса" if r["city"] == "odesa" else "Львов"
            text += f"{city_name} | {r['code']} | {r['name']} | {decimal.Decimal(r['price']):.0f} {UAH} | {state}\n"

        await message.answer(text)

    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)[:200]}")


@dp.message(F.text.startswith("/addpromo"))
async def cmd_add_promo(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        raw = message.text.replace("/addpromo", "", 1).strip()
        parts = [p.strip() for p in raw.split("|")]

        if len(parts) < 3:
            await message.answer("Формат: /addpromo код | сумма | макс_использований")
            return

        code = parts[0].upper()
        amount = int(parts[1])
        max_uses = int(parts[2])

        if amount <= 0 or max_uses <= 0:
            await message.answer("Сумма и максимальные использования должны быть > 0")
            return

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO promo_codes(code, discount_amount, max_uses, is_active, created_by)
                VALUES($1,$2,$3,TRUE,$4)
                ON CONFLICT(code) DO UPDATE SET
                    discount_amount=EXCLUDED.discount_amount,
                    max_uses=EXCLUDED.max_uses,
                    is_active=TRUE
            """, code, amount, max_uses, message.from_user.id)

        await message.answer(f"✅ Промокод создан: {code}\n{amount} {UAH} x {max_uses} раз")

    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)[:200]}")


@dp.message(F.text.startswith("/setphoto"))
async def cmd_set_photo(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        code = message.text.replace("/setphoto", "").strip()
        if not code:
            await message.answer("Формат: /setphoto КОД\nТогда пришли фото ответом")
            return

        assert pool is not None
        async with pool.acquire() as con:
            product = await con.fetchrow("SELECT * FROM products WHERE code=$1", code)

        if not product:
            await message.answer(f"❌ Товар {code} не найден")
            return

        await message.answer(f"Пришли фото для товара {code}")
        # Ожидаем фото в следующем сообщении
        # Это обработается ниже

    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)[:200]}")


@dp.message(F.content_type == "photo")
async def handle_photo(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        # Получаем reply-to сообщение
        if not message.reply_to_message:
            await message.answer("Пришли фото ответом на команду /setphoto КОД")
            return

        reply_text = message.reply_to_message.text or ""
        if not reply_text.startswith("/setphoto"):
            await message.answer("Пришли фото ответом на команду /setphoto КОД")
            return

        code = reply_text.replace("/setphoto", "").strip()
        if not code:
            await message.answer("❌ Код товара не найден")
            return

        photo_id = message.photo[-1].file_id

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute(
                "UPDATE products SET photo_id=$2 WHERE code=$1",
                code, photo_id
            )

        await message.answer(f"✅ Фото установлено для товара {code}")

    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)[:200]}")


async def init_default_products():
    """Инициализация товаров по умолчанию при первом запуске"""
    assert pool is not None
    async with pool.acquire() as con:
        # Проверяем, есть ли уже товары
        count = await con.fetchval("SELECT COUNT(*) FROM products")
        
        if count == 0:
            # Добавляем товары по умолчанию
            await con.execute("""
                INSERT INTO products(code, city, name, price, link, description, is_active)
                VALUES
                    ('odesa_sh', 'odesa', 'Ш', 360, '', 'Топ качество', TRUE),
                    ('odesa_han', 'odesa', 'ХаН', 320, '', 'Проверено', TRUE)
            """)
            print("[INIT] Добавлены товары по умолчанию")


async def main():
    await db_init()
    await init_default_products()
    
    bot = Bot(token=BOT_TOKEN)
    cleanup_task = asyncio.create_task(background_cleanup())

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        cleanup_task.cancel()
        if pool is not None:
            await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
