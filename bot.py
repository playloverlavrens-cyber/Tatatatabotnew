import os
import asyncio
import decimal
import json
import asyncpg
import aiohttp
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
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
UKRAINE_TZ = ZoneInfo("Europe/Kyiv")

# ================== TEXTS ==================
MAIN_CAPTION = """🍝 Добро пожаловать в "Итальянский Квадрат"

⭐ Наши преимущества:
• Все товары топ качества
• Регулярные пополнения
• Огромный выбор товара по почте
• Шустрые отправки

📞 Контакты:
Бот: @ititititbot_bot
Оператор: @italiansquare

🏦 Баланс: {balance} {uah}
🛍️ Заказов: {orders}"""

PROFILE_TEXT = "👤 Профиль\n\n🏦 Баланс: {balance} {uah}\n🛍️ Заказов: {orders}"
HELP_TEXT = "Вопросы? Обратись: @italiansquare"
ITEM_TEXT = "✅ Выбран товар: {name}\n💰 Цена: {price} {uah}\n\n{desc}"
TOPUP_TEXT = f"💳 Введи сумму пополнения (мин. 10 {UAH}):"

# ================== DB ==================
pool: asyncpg.Pool | None = None


def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID


def get_ukraine_time() -> datetime:
    return datetime.now(UKRAINE_TZ)


def format_ukraine_time(dt: datetime | None) -> str:
    if not dt:
        return "неизвестно"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(UKRAINE_TZ).strftime("%Y-%m-%d %H:%M:%S")


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
                description TEXT DEFAULT '',
                is_active BOOLEAN DEFAULT TRUE,
                reserved_by BIGINT,
                reserved_until TIMESTAMPTZ,
                sold_to BIGINT,
                sold_at TIMESTAMPTZ,
                photo_id TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Добавляем колонку если её нет
        try:
            await con.execute("ALTER TABLE products ADD COLUMN photo_id TEXT")
        except:
            pass

        await con.execute("""
            CREATE TABLE IF NOT EXISTS purchases (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                product_code TEXT,
                item_name TEXT,
                price NUMERIC(12,2),
                photo_id TEXT,
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
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await con.execute("""
            CREATE TABLE IF NOT EXISTS promo_usage (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                promo_code TEXT REFERENCES promo_codes(code),
                used_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Инициализация товаров
        await con.execute("""
            INSERT INTO products(code, city, name, price, description, is_active)
            VALUES('sh_odesa', 'odesa', 'Ш', 360, 'Премиум качество', TRUE),
                   ('han_odesa', 'odesa', 'ХаН', 320, 'Отличный выбор', TRUE)
            ON CONFLICT DO NOTHING
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
            [KeyboardButton(text="ГЛАВНАЯ 🔘"), KeyboardButton(text="ПРОФИЛЬ 👤")],
            [KeyboardButton(text="ПОМОЩЬ 💬"), KeyboardButton(text="РАБОТА 💸")]
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
            inline_keyboard=[[InlineKeyboardButton(text="Нет товаров в наличии", callback_data="noop")]]
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
            [InlineKeyboardButton(text="💰 Баланс", callback_data=f"pay:bal:{code}")],
            [InlineKeyboardButton(text="💳 PaySync", callback_data=f"pay:card:{code}")],
            [InlineKeyboardButton(text="🪙 Crypto", callback_data=f"pay:crypto:{code}")]
        ]
    )


def inline_profile() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Пополнить", callback_data="profile:topup")],
            [InlineKeyboardButton(text="🎟 Промокод", callback_data="profile:promo")],
            [InlineKeyboardButton(text="🧾 История", callback_data="profile:history")]
        ]
    )


def inline_check(trade_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Проверить оплату", callback_data=f"check:{trade_id}")]
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
            except Exception:
                raise RuntimeError(f"PaySync вернул невалидный JSON: {raw_text[:300]}")


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
            except Exception:
                raise RuntimeError(f"PaySync вернул невалидный JSON: {raw_text[:300]}")


async def crypto_pay_create(amount: int, currency: str = "USDT") -> dict:
    """Создание инвойса через Crypto Pay API"""
    url = f"{CRYPTO_PAY_BASE_URL}/invoices"
    headers = {
        "Crypto-Pay-API-Token": CRYPTO_PAY_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    payload = {
        "asset": currency,
        "amount": str(amount),
        "currency": CRYPTO_PAY_FIAT,
        "description": f"Payment {uuid.uuid4().hex[:8]}"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload, timeout=30) as resp:
            raw_text = await resp.text()

            if resp.status >= 400:
                raise RuntimeError(f"Crypto Pay HTTP {resp.status}: {raw_text[:300]}")

            try:
                data = json.loads(raw_text)
                if data.get("ok"):
                    return data.get("result", {})
                raise RuntimeError(f"Crypto Pay error: {data.get('error', 'unknown')}")
            except Exception as e:
                raise RuntimeError(f"Crypto Pay ошибка: {str(e)[:300]}")


async def crypto_pay_check(invoice_id: int) -> dict:
    """Проверка статуса инвойса Crypto Pay"""
    url = f"{CRYPTO_PAY_BASE_URL}/invoices?invoice_ids={invoice_id}"
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_API_TOKEN}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=30) as resp:
            raw_text = await resp.text()

            if resp.status >= 400:
                raise RuntimeError(f"Crypto Pay HTTP {resp.status}: {raw_text[:300]}")

            try:
                data = json.loads(raw_text)
                if data.get("ok"):
                    invoices = data.get("result", {}).get("items", [])
                    return invoices[0] if invoices else {}
                raise RuntimeError(f"Crypto Pay error: {data.get('error', 'unknown')}")
            except Exception as e:
                raise RuntimeError(f"Crypto Pay ошибка: {str(e)[:300]}")


def extract_trade_id(js: dict) -> str:
    trade_id = js.get("trade")
    if trade_id is None:
        raise RuntimeError(f"PaySync: нет поля trade в ответе")
    return str(trade_id)


def extract_card_number(js: dict) -> str:
    card = js.get("card_number")
    if not card:
        return "не получена"
    return str(card).strip()


def extract_status(js: dict) -> str:
    return str(js.get("status", "")).strip().lower()


def extract_amount(js: dict, fallback: int) -> int:
    raw = js.get("amount", fallback)
    try:
        return int(raw)
    except Exception:
        try:
            return int(decimal.Decimal(str(raw)))
        except Exception:
            return fallback


def extract_paysync_time(js: dict) -> str:
    return str(js.get("time", "")).strip()

# ================== FSM ==================
class TopupStates(StatesGroup):
    waiting_amount = State()


class PromoStates(StatesGroup):
    waiting_code = State()


class SetPhotoStates(StatesGroup):
    waiting_photo = State()

# ================== BOT ==================
dp = Dispatcher(storage=MemoryStorage())


@dp.message(CommandStart())
async def cmd_start(message: Message):
    await ensure_user(message.from_user.id)
    bal, orders = await get_stats(message.from_user.id)
    caption = MAIN_CAPTION.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    try:
        await message.answer_photo(
            photo="https://i.postimg.cc/0jQVyKJX/italian-square.jpg",
            caption=caption,
            reply_markup=bottom_menu()
        )
    except:
        await message.answer(caption, reply_markup=bottom_menu())


@dp.message(F.text.contains("ГЛАВНАЯ"))
async def btn_main(message: Message):
    await ensure_user(message.from_user.id)
    bal, orders = await get_stats(message.from_user.id)
    caption = MAIN_CAPTION.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    try:
        await message.answer_photo(
            photo="https://i.postimg.cc/0jQVyKJX/italian-square.jpg",
            caption=caption,
            reply_markup=inline_city()
        )
    except:
        await message.answer(caption, reply_markup=inline_city())


@dp.message(F.text.contains("ПРОФИЛЬ"))
async def btn_profile(message: Message):
    await ensure_user(message.from_user.id)
    bal, orders = await get_stats(message.from_user.id)
    text = PROFILE_TEXT.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    await message.answer(text, reply_markup=inline_profile())


@dp.message(F.text.contains("ПОМОЩЬ"))
async def btn_help(message: Message):
    await message.answer(HELP_TEXT, reply_markup=bottom_menu())


@dp.message(F.text.contains("РАБОТА"))
async def btn_work(message: Message):
    await message.answer("Ищем ответственных! Пиши: @italiansquare", reply_markup=bottom_menu())


@dp.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()


@dp.callback_query(F.data.startswith("city:"))
async def cb_city(call: CallbackQuery):
    await call.answer()
    city = call.data.split(":")[1]
    assert pool is not None

    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT code, name, price, photo_id
            FROM products
            WHERE city=$1
              AND is_active=TRUE
              AND sold_at IS NULL
              AND (reserved_until IS NULL OR reserved_until < NOW())
            ORDER BY created_at DESC
            LIMIT 50
        """, city)

    await call.message.answer("Выбери товар:", reply_markup=inline_products(rows, city))


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
        await call.message.answer("Товар не найден")
        return

    if row["sold_at"] is not None:
        await call.message.answer("Товар уже продан")
        return

    now = get_ukraine_time()
    if row["reserved_until"] and row["reserved_until"].replace(tzinfo=UKRAINE_TZ) > now and row["reserved_by"] != call.from_user.id:
        reserved_min = int((row["reserved_until"] - now.astimezone(row["reserved_until"].tzinfo)).total_seconds() / 60)
        await call.message.answer(f"Товар зарезервирован на {reserved_min} мин")
        return

    text = ITEM_TEXT.format(
        name=row["name"],
        price=f"{decimal.Decimal(row['price']):.0f}",
        desc=row["description"] or "Без описания",
        uah=UAH
    )
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
        await message.answer(f"Минимум 10 {UAH}")
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
                    trade_id, user_id, kind, amount_int, currency, provider, status, expires_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (trade_id) DO NOTHING
            """,
                trade_id, message.from_user.id, "topup", real_amount,
                PAYSYNC_CURRENCY, "paysync", "wait", expires_at
            )

        expire_text = format_ukraine_time(expires_at)
        text = (
            "💳 Пополнение баланса\n\n"
            f"Заявка: {trade_id}\n"
            f"Карта: {card_number}\n"
            f"Сумма: {real_amount} {PAYSYNC_CURRENCY}\n"
            f"Срок: до {expire_text}\n\n"
            "Оплати точно указанную сумму одним платежом"
        )
        await message.answer(text, reply_markup=inline_check(trade_id))

    except Exception as e:
        print(f"[TOPUP ERROR] {repr(e)}")
        await message.answer(f"Ошибка при создании платежа: {str(e)[:200]}")
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
                product = await con.fetchrow(
                    "SELECT * FROM products WHERE code=$1 FOR UPDATE", code
                )

                if not product or product["sold_at"]:
                    await call.message.answer("Товар недоступен")
                    return

                user = await con.fetchrow("SELECT * FROM users WHERE user_id=$1 FOR UPDATE", uid)
                if not user:
                    await ensure_user(uid)
                    user = await con.fetchrow("SELECT * FROM users WHERE user_id=$1", uid)

                price = decimal.Decimal(product["price"])
                balance = decimal.Decimal(user["balance"])

                if balance < price:
                    await call.message.answer(f"Недостаточно средств\nНужно: {price:.0f} {UAH}\nВ наличии: {balance:.0f} {UAH}")
                    return

                await con.execute(
                    "UPDATE users SET balance = balance - $2, orders_count = orders_count + 1 WHERE user_id = $1",
                    uid, price
                )

                await con.execute(
                    "UPDATE products SET sold_to=$2, sold_at=NOW(), reserved_by=NULL, reserved_until=NULL WHERE code=$1",
                    code, uid
                )

                await con.execute(
                    "INSERT INTO purchases(user_id, product_code, item_name, price, photo_id, provider) VALUES($1,$2,$3,$4,$5,$6)",
                    uid, code, product["name"], price, product["photo_id"], "balance"
                )

        text = f"✅ Покупка успешна\n\n{product['name']} — {price:.0f} {UAH}"
        
        if product["photo_id"]:
            await call.message.answer_photo(
                photo=product["photo_id"],
                caption=text
            )
        else:
            await call.message.answer(text + "\n\n⚠️ Фото товара не загружено")

    except Exception as e:
        print(f"[PAY BALANCE ERROR] {repr(e)}")
        await call.message.answer(f"Ошибка при покупке: {str(e)[:200]}")


@dp.callback_query(F.data.startswith("pay:card:"))
async def cb_pay_card(call: CallbackQuery):
    await call.answer()
    code = call.data.split(":")[2]
    uid = call.from_user.id

    try:
        assert pool is not None

        async with pool.acquire() as con:
            async with con.transaction():
                product = await con.fetchrow(
                    "SELECT * FROM products WHERE code=$1 FOR UPDATE", code
                )

                if not product or product["sold_at"]:
                    await call.message.answer("Товар недоступен")
                    return

                now = get_ukraine_time()
                reserve_until = now + timedelta(minutes=RESERVATION_MINUTES)

                await con.execute(
                    "UPDATE products SET reserved_by=$2, reserved_until=$3 WHERE code=$1",
                    code, uid, reserve_until
                )

                price = int(decimal.Decimal(product["price"]))
                nonce = uuid.uuid4().hex[:12]
                payload = f"buy:{uid}:{code}:{nonce}"

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
                trade_id, uid, "purchase", real_amount, PAYSYNC_CURRENCY, code, "paysync", "wait", expires_at
            )

        expire_text = format_ukraine_time(expires_at)
        text = (
            "💳 Оплата товара\n\n"
            f"{product['name']}\n"
            f"Заявка: {trade_id}\n"
            f"Карта: {card_number}\n"
            f"Сумма: {real_amount} {PAYSYNC_CURRENCY}\n"
            f"Срок: до {expire_text}\n"
            f"Резервация: {RESERVATION_MINUTES} мин\n\n"
            "Оплати точно указанную сумму одним платежом"
        )
        await call.message.answer(text, reply_markup=inline_check(trade_id))

    except Exception as e:
        if pool:
            try:
                async with pool.acquire() as con:
                    await con.execute(
                        "UPDATE products SET reserved_by=NULL, reserved_until=NULL WHERE code=$1 AND sold_at IS NULL",
                        code
                    )
            except:
                pass

        print(f"[CARD ERROR] {repr(e)}")
        await call.message.answer(f"Ошибка при создании платежа: {str(e)[:200]}")


@dp.callback_query(F.data.startswith("pay:crypto:"))
async def cb_pay_crypto(call: CallbackQuery):
    await call.answer()
    code = call.data.split(":")[2]
    uid = call.from_user.id

    try:
        assert pool is not None

        async with pool.acquire() as con:
            product = await con.fetchrow(
                "SELECT code, name, price FROM products WHERE code=$1", code
            )

        if not product:
            await call.message.answer("Товар не найден")
            return

        price = int(decimal.Decimal(product["price"]))
        
        invoice = await crypto_pay_create(price, "USDT")
        
        invoice_id = invoice.get("invoice_id")
        pay_url = invoice.get("pay_url")

        if not invoice_id or not pay_url:
            raise RuntimeError("Не получены данные инвойса")

        expires_at = get_ukraine_time() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO invoices(
                    trade_id, user_id, kind, amount_int, currency, product_code, provider, status, expires_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (trade_id) DO NOTHING
            """,
                str(invoice_id), uid, "purchase", price, CRYPTO_PAY_FIAT, code, "crypto", "wait", expires_at
            )

        text = (
            "🪙 Оплата Crypto\n\n"
            f"{product['name']}\n"
            f"Сумма: {price} {CRYPTO_PAY_FIAT}\n"
            f"ID: {invoice_id}\n\n"
            f"[Ссылка для оплаты]({pay_url})"
        )
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Проверить оплату", callback_data=f"check:{invoice_id}")],
            [InlineKeyboardButton(text="Открыть платеж", url=pay_url)]
        ])

        await call.message.answer(text, reply_markup=kb, parse_mode="Markdown")

    except Exception as e:
        print(f"[CRYPTO ERROR] {repr(e)}")
        await call.message.answer(f"Ошибка при создании платежа: {str(e)[:200]}")


@dp.callback_query(F.data.startswith("check:"))
async def cb_check(call: CallbackQuery):
    await call.answer()
    trade_id = call.data.split(":", 1)[1]

    try:
        assert pool is not None

        async with pool.acquire() as con:
            inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)

        if not inv:
            await call.message.answer("Счет не найден")
            return

        if inv["status"] == "paid":
            await call.message.answer("Этот счет уже был обработан")
            return

        if inv["provider"] == "paysync":
            js = await paysync_check(trade_id)
            status = extract_status(js)
        elif inv["provider"] == "crypto":
            js = await crypto_pay_check(int(trade_id))
            status = js.get("status", "").lower()
        else:
            await call.message.answer("Неизвестный провайдер")
            return

        if status in {"paid", "success", "succeeded", "completed"}:
            async with pool.acquire() as con:
                async with con.transaction():
                    inv = await con.fetchrow(
                        "SELECT * FROM invoices WHERE trade_id=$1 FOR UPDATE", trade_id
                    )

                    if not inv or inv["status"] == "paid":
                        await call.message.answer("Платеж уже обработан")
                        return

                    if inv["kind"] == "topup":
                        await con.execute(
                            "UPDATE users SET balance = balance + $2 WHERE user_id = $1",
                            inv["user_id"], inv["amount_int"]
                        )

                        await con.execute(
                            "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                            trade_id
                        )

                        await call.message.answer(
                            f"✅ Платеж подтвержден\n\nБаланс пополнен на {inv['amount_int']} {inv['currency']}"
                        )
                        return

                    if inv["kind"] == "purchase":
                        product = await con.fetchrow(
                            "SELECT * FROM products WHERE code=$1 FOR UPDATE",
                            inv["product_code"]
                        )

                        if not product:
                            await call.message.answer("Товар не найден")
                            return

                        if product["sold_at"]:
                            await con.execute(
                                "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                                trade_id
                            )
                            await call.message.answer("Товар уже продан. Свяжись с оператором")
                            return

                        await con.execute(
                            "UPDATE products SET sold_to=$2, sold_at=NOW(), reserved_by=NULL, reserved_until=NULL WHERE code=$1",
                            inv["product_code"], inv["user_id"]
                        )

                        await con.execute(
                            "UPDATE users SET orders_count = orders_count + 1 WHERE user_id = $1",
                            inv["user_id"]
                        )

                        await con.execute(
                            "INSERT INTO purchases(user_id, product_code, item_name, price, photo_id, provider) VALUES($1,$2,$3,$4,$5,$6)",
                            inv["user_id"], product["code"], product["name"], product["price"], product["photo_id"], inv["provider"]
                        )

                        await con.execute(
                            "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                            trade_id
                        )

                        text = f"✅ Покупка успешна\n\n{product['name']} — {decimal.Decimal(product['price']):.0f} {UAH}"
                        
                        if product["photo_id"]:
                            await call.message.answer_photo(
                                photo=product["photo_id"],
                                caption=text
                            )
                        else:
                            await call.message.answer(text + "\n\n⚠️ Фото товара не загружено")
                        return

        elif status in {"expired", "cancelled", "canceled", "failed"}:
            async with pool.acquire() as con:
                async with con.transaction():
                    inv = await con.fetchrow(
                        "SELECT * FROM invoices WHERE trade_id=$1 FOR UPDATE", trade_id
                    )

                    if inv and inv["kind"] == "purchase" and inv["product_code"]:
                        await con.execute(
                            "UPDATE products SET reserved_by=NULL, reserved_until=NULL WHERE code=$1 AND sold_at IS NULL",
                            inv["product_code"]
                        )

                    await con.execute(
                        "UPDATE invoices SET status=$2 WHERE trade_id=$1",
                        trade_id, status
                    )

            await call.message.answer(f"Платеж отклонен: {status}")
            return

        else:
            await call.message.answer("Платеж не подтвержден. Попробуй через минуту")
            return

    except Exception as e:
        print(f"[CHECK ERROR] {repr(e)}")
        await call.message.answer(f"Ошибка проверки: {str(e)[:200]}")


@dp.callback_query(F.data == "profile:history")
async def cb_history(call: CallbackQuery):
    await call.answer()
    assert pool is not None

    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT item_name, price, provider, created_at
            FROM purchases
            WHERE user_id=$1
            ORDER BY created_at DESC
            LIMIT 20
        """, call.from_user.id)

    if not rows:
        await call.message.answer("История пуста")
        return

    text = "🧾 История покупок:\n\n"
    for r in rows:
        dt = format_ukraine_time(r["created_at"])
        price = decimal.Decimal(r["price"])
        text += f"• {r['item_name']} — {price:.0f} {UAH} ({dt})\n"

    await call.message.answer(text)


@dp.callback_query(F.data == "profile:promo")
async def cb_promo(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(PromoStates.waiting_code)
    await call.message.answer("Введи промокод:")


@dp.message(PromoStates.waiting_code)
async def promo_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    uid = message.from_user.id

    try:
        assert pool is not None

        async with pool.acquire() as con:
            promo = await con.fetchrow(
                "SELECT * FROM promo_codes WHERE code=$1 AND is_active=TRUE",
                code
            )

            if not promo:
                await message.answer("Промокод не найден")
                await state.clear()
                return

            if promo["current_uses"] >= promo["max_uses"]:
                await message.answer("Промокод исчерпан")
                await state.clear()
                return

            used = await con.fetchval(
                "SELECT COUNT(*) FROM promo_usage WHERE user_id=$1 AND promo_code=$2",
                uid, code
            )

            if used:
                await message.answer("Ты уже использовал этот промокод")
                await state.clear()
                return

            async with con.transaction():
                await con.execute(
                    "UPDATE users SET balance = balance + $2 WHERE user_id = $1",
                    uid, promo["discount_amount"]
                )

                await con.execute(
                    "INSERT INTO promo_usage(user_id, promo_code) VALUES($1, $2)",
                    uid, code
                )

                await con.execute(
                    "UPDATE promo_codes SET current_uses = current_uses + 1 WHERE code = $1",
                    code
                )

        await message.answer(f"✅ Промокод активирован\nДобавлено: {promo['discount_amount']} {UAH}")

    except Exception as e:
        print(f"[PROMO ERROR] {repr(e)}")
        await message.answer(f"Ошибка: {str(e)[:200]}")
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

        if len(parts) < 4:
            await message.answer("Формат: /addproduct город | код | название | цена | описание")
            return

        city, code, name, price_raw = parts[:4]
        desc = parts[4] if len(parts) > 4 else ""

        if city not in ["odesa", "lviv"]:
            await message.answer("Город должен быть: odesa или lviv")
            return

        price = decimal.Decimal(price_raw)

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO products(code, city, name, price, description, is_active)
                VALUES($1,$2,$3,$4,$5,TRUE)
                ON CONFLICT(code) DO UPDATE SET
                    city=EXCLUDED.city,
                    name=EXCLUDED.name,
                    price=EXCLUDED.price,
                    description=EXCLUDED.description,
                    is_active=TRUE,
                    sold_to=NULL,
                    sold_at=NULL,
                    reserved_by=NULL,
                    reserved_until=NULL,
                    photo_id=NULL
            """, code, city, name, price, desc)

        await message.answer(f"✅ Товар добавлен: {code}")

    except Exception as e:
        await message.answer(f"Ошибка: {str(e)[:300]}")


@dp.message(F.text.startswith("/setphoto"))
async def cmd_set_photo(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        code = message.text.replace("/setphoto", "").strip()
        if not code:
            await message.answer("Формат: /setphoto КОД")
            return

        assert pool is not None
        async with pool.acquire() as con:
            product = await con.fetchrow("SELECT * FROM products WHERE code=$1", code)

        if not product:
            await message.answer(f"Товар {code} не найден")
            return

        await state.set_state(SetPhotoStates.waiting_photo)
        await state.update_data(product_code=code)
        await message.answer(f"Отправь фото для товара {code}")

    except Exception as e:
        await message.answer(f"Ошибка: {str(e)[:300]}")


@dp.message(SetPhotoStates.waiting_photo, F.photo)
async def handle_photo(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        code = data.get("product_code")

        if not message.photo:
            await message.answer("Это не фото")
            return

        photo_id = message.photo[-1].file_id

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute(
                "UPDATE products SET photo_id=$2 WHERE code=$1",
                code, photo_id
            )

        await message.answer(f"✅ Фото сохранено для товара {code}")

    except Exception as e:
        await message.answer(f"Ошибка: {str(e)[:300]}")
    finally:
        await state.clear()


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
        await message.answer(f"Ошибка: {str(e)[:300]}")


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
                ORDER BY city, created_at DESC
                LIMIT 100
            """)

        if not rows:
            await message.answer("Товаров нет")
            return

        text = "📦 Товары:\n\n"
        for r in rows:
            state = "⏹️" if not r["is_active"] else "✅"
            if r["sold_at"] is not None:
                state = "🔒"
            elif r["reserved_until"] and r["reserved_until"] > get_ukraine_time():
                state = "⏳"

            text += f"{state} {r['city'][:2].upper()} | {r['code']} | {r['name']} | {decimal.Decimal(r['price']):.0f} {UAH}\n"

        await message.answer(text)

    except Exception as e:
        await message.answer(f"Ошибка: {str(e)[:300]}")


@dp.message(F.text.startswith("/addpromo"))
async def cmd_add_promo(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        raw = message.text.replace("/addpromo", "", 1).strip()
        parts = [p.strip() for p in raw.split("|")]

        if len(parts) < 3:
            await message.answer("Формат: /addpromo КОД | СУММА | МАКС_ИСПОЛЬЗОВАНИЙ")
            return

        code = parts[0].upper()
        discount = int(parts[1])
        max_uses = int(parts[2])

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO promo_codes(code, discount_amount, max_uses, is_active)
                VALUES($1, $2, $3, TRUE)
                ON CONFLICT(code) DO UPDATE SET
                    discount_amount=EXCLUDED.discount_amount,
                    max_uses=EXCLUDED.max_uses,
                    is_active=TRUE,
                    current_uses=0
            """, code, discount, max_uses)

        await message.answer(f"✅ Промокод {code} добавлен: {discount} {UAH} × {max_uses}")

    except Exception as e:
        await message.answer(f"Ошибка: {str(e)[:300]}")


# ================== MAIN ==================
async def main():
    await db_init()
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
