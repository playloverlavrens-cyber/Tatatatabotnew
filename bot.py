import os
import asyncio
import decimal
import json
import asyncpg
import aiohttp
import uuid
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

PAYMENT_TIMEOUT_MINUTES = int(os.getenv("PAYMENT_TIMEOUT_MINUTES", "15"))

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
MAIN_TEXT = """Приветствуем! 🫡

✍🏻 О СЕРВИСЕ
📚 Легальный магазин ссылок на книги

°Готовые товары
°Оптовые цены
°Быстрая доставка
°Разные способы оплаты
°Поддержка 24/7

📞 Контакты:
Бот: @YourBotName
Оператор: @YourSupport

🏦 Баланс: {balance} {uah}
🛍️ Заказов: {orders}"""

PROFILE_TEXT = "👤 Профиль\n\n🏦 Баланс: {balance} {uah}\n🛍️ Заказов: {orders}"
HELP_TEXT = "Вопросы? Пиши: @YourSupport"
TOPUP_TEXT = f"💳 Введи сумму пополнения ({UAH}):\nНапример: 150"

# ================== DB ==================
pool: asyncpg.Pool | None = None


def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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


def dt_to_text(dt: datetime | None) -> str:
    if not dt:
        return "неизвестно"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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
                id BIGSERIAL PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                city TEXT NOT NULL,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price NUMERIC(12,2) DEFAULT 0,
                photo_id TEXT,
                link TEXT DEFAULT '',
                description TEXT DEFAULT '',
                is_active BOOLEAN DEFAULT TRUE,
                sold_to BIGINT,
                sold_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await con.execute("""
            CREATE TABLE IF NOT EXISTS purchases (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                product_id BIGINT REFERENCES products(id),
                item_name TEXT,
                category TEXT,
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
                product_id BIGINT REFERENCES products(id),
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
                discount INT NOT NULL,
                max_uses INT NOT NULL,
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


def inline_categories(rows: list) -> InlineKeyboardMarkup:
    if not rows:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Нет товаров", callback_data="noop")]]
        )

    categories = {}
    for r in rows:
        if r["category"] not in categories:
            categories[r["category"]] = r["price"]

    kb = []
    for cat, price in sorted(categories.items()):
        kb.append([
            InlineKeyboardButton(
                text=f"{cat} — {decimal.Decimal(price):.0f} {UAH}",
                callback_data=f"cat:{cat}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def inline_products(rows: list, city: str, category: str) -> InlineKeyboardMarkup:
    filtered = [r for r in rows if r["category"] == category]
    
    if not filtered:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Нет в наличии", callback_data="noop")]]
        )

    kb = []
    for r in filtered:
        count = len([x for x in filtered if x["category"] == category])
        kb.append([
            InlineKeyboardButton(
                text=f"#{r['id']} ({count} доступно)",
                callback_data=f"prod:{r['id']}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def inline_pay(product_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Балансом", callback_data=f"pay:bal:{product_id}")],
            [InlineKeyboardButton(text="Картой PaySync", callback_data=f"pay:card:{product_id}")],
            [InlineKeyboardButton(text="Crypto", callback_data=f"pay:crypto:{product_id}")]
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
                raise RuntimeError(f"PaySync JSON error: {raw_text[:300]}")


async def paysync_check(trade_id: str) -> dict:
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
                raise RuntimeError(f"PaySync JSON error: {raw_text[:300]}")


def extract_trade_id(js: dict) -> str:
    trade_id = js.get("trade")
    if trade_id is None:
        raise RuntimeError("no trade field")
    return str(trade_id)


def extract_card_number(js: dict) -> str:
    card = js.get("card_number")
    return str(card).strip() if card else "не получена"


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


class CityState(StatesGroup):
    city = State()
    category = State()

# ================== BOT ==================
dp = Dispatcher(storage=MemoryStorage())


@dp.message(CommandStart())
async def cmd_start(message: Message):
    await ensure_user(message.from_user.id)
    bal, orders = await get_stats(message.from_user.id)
    text = MAIN_TEXT.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    await message.answer(text, reply_markup=bottom_menu())


@dp.message(F.text.contains("ГЛАВНАЯ"))
async def btn_main(message: Message):
    await ensure_user(message.from_user.id)
    bal, orders = await get_stats(message.from_user.id)
    text = MAIN_TEXT.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    await message.answer(text, reply_markup=inline_city())


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
    await message.answer("Ищем ответственных! Пиши: @YourSupport", reply_markup=bottom_menu())


@dp.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()


@dp.callback_query(F.data.startswith("city:"))
async def cb_city(call: CallbackQuery, state: FSMContext):
    await call.answer()
    city = call.data.split(":")[1]
    
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT DISTINCT category, price FROM products
            WHERE city=$1 AND is_active=TRUE AND sold_at IS NULL
            ORDER BY category
        """, city)

    if not rows:
        await call.message.answer("Нет товаров")
        return

    await state.update_data(city=city)
    await call.message.answer("Выбери категорию:", reply_markup=inline_categories(rows))


@dp.callback_query(F.data.startswith("cat:"))
async def cb_category(call: CallbackQuery, state: FSMContext):
    await call.answer()
    category = call.data.split(":", 1)[1]
    
    data = await state.get_data()
    city = data.get("city", "odesa")
    
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT id, category, price FROM products
            WHERE city=$1 AND category=$2 AND is_active=TRUE AND sold_at IS NULL
            ORDER BY created_at DESC
        """, city, category)

    if not rows:
        await call.message.answer("Нет в наличии")
        return

    await call.message.answer(f"Выбери товар '{category}':", reply_markup=inline_products(rows, city, category))


@dp.callback_query(F.data.startswith("prod:"))
async def cb_product(call: CallbackQuery):
    await call.answer()
    product_id = int(call.data.split(":")[1])

    assert pool is not None
    async with pool.acquire() as con:
        item = await con.fetchrow("""
            SELECT * FROM products WHERE id=$1 AND is_active=TRUE AND sold_at IS NULL
        """, product_id)

    if not item:
        await call.message.answer("Товар недоступен")
        return

    price = decimal.Decimal(item["price"])
    text = (
        f"✅ {item['category']}\n"
        f"💰 Цена: {price:.0f} {UAH}\n"
        f"Описание: {item['description'] or 'Нет'}\n\n"
        f"Выбери оплату:"
    )

    await call.message.answer(text, reply_markup=inline_pay(product_id))


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
        expires_at = utc_now() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

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

        text = (
            "💳 Пополнение баланса\n\n"
            f"Заявка: {trade_id}\n"
            f"Карта: {card_number}\n"
            f"Сумма: {real_amount} {PAYSYNC_CURRENCY}\n"
            f"Срок: {paysync_time or dt_to_text(expires_at)}\n\n"
            "Оплати точно указанную сумму"
        )
        await message.answer(text, reply_markup=inline_check(trade_id))

    except Exception as e:
        print(f"[TOPUP ERROR] {repr(e)}")
        await message.answer(f"Ошибка. Попробуй позже")
    finally:
        await state.clear()


@dp.callback_query(F.data.startswith("pay:bal:"))
async def cb_pay_balance(call: CallbackQuery):
    await call.answer()
    product_id = int(call.data.split(":")[2])
    uid = call.from_user.id

    try:
        assert pool is not None
        async with pool.acquire() as con:
            async with con.transaction():
                item = await con.fetchrow(
                    "SELECT * FROM products WHERE id=$1 FOR UPDATE", product_id
                )

                if not item or item["sold_at"]:
                    await call.message.answer("Товар недоступен")
                    return

                user = await con.fetchrow("SELECT * FROM users WHERE user_id=$1 FOR UPDATE", uid)
                if not user:
                    await ensure_user(uid)
                    user = await con.fetchrow("SELECT * FROM users WHERE user_id=$1", uid)

                price = decimal.Decimal(item["price"])
                balance = decimal.Decimal(user["balance"])

                if balance < price:
                    await call.message.answer(
                        f"Недостаточно средств\n"
                        f"Нужно: {price:.0f} {UAH}\n"
                        f"У вас: {balance:.0f} {UAH}"
                    )
                    return

                await con.execute(
                    "UPDATE users SET balance = balance - $2, orders_count = orders_count + 1 WHERE user_id = $1",
                    uid, price
                )

                await con.execute(
                    "UPDATE products SET sold_to=$2, sold_at=NOW() WHERE id=$1",
                    product_id, uid
                )

                await con.execute(
                    "INSERT INTO purchases(user_id, product_id, item_name, category, price, link, provider) VALUES($1,$2,$3,$4,$5,$6,$7)",
                    uid, product_id, item["name"], item["category"], price, item["link"], "balance"
                )

        await call.message.answer(
            f"✅ Успешно!\n\n"
            f"{item['category']}\n"
            f"💰 {price:.0f} {UAH}\n\n"
            f"🔗 Ссылка:\n{item['link']}"
        )

    except Exception as e:
        print(f"[PAY BALANCE ERROR] {repr(e)}")
        await call.message.answer("Ошибка при покупке")


@dp.callback_query(F.data.startswith("pay:card:"))
async def cb_pay_card(call: CallbackQuery):
    await call.answer()
    product_id = int(call.data.split(":")[2])
    uid = call.from_user.id

    try:
        assert pool is not None

        async with pool.acquire() as con:
            item = await con.fetchrow(
                "SELECT * FROM products WHERE id=$1", product_id
            )

            if not item or item["sold_at"]:
                await call.message.answer("Товар недоступен")
                return

            price = int(decimal.Decimal(item["price"]))
            nonce = uuid.uuid4().hex[:12]
            payload = f"buy:{uid}:{product_id}:{nonce}"

        js = await paysync_create(price, payload)

        trade_id = extract_trade_id(js)
        card_number = extract_card_number(js)
        real_amount = extract_amount(js, price)
        paysync_time = extract_paysync_time(js)
        expires_at = utc_now() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO invoices(
                    trade_id, user_id, kind, amount_int, currency, product_id, provider, status, expires_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (trade_id) DO NOTHING
            """,
                trade_id, uid, "purchase", real_amount, PAYSYNC_CURRENCY, product_id, "paysync", "wait", expires_at
            )

        text = (
            "💳 Оплата товара\n\n"
            f"Заявка: {trade_id}\n"
            f"Карта: {card_number}\n"
            f"Сумма: {real_amount} {PAYSYNC_CURRENCY}\n"
            f"Срок: {paysync_time or dt_to_text(expires_at)}\n\n"
            "Оплати точно указанную сумму"
        )
        await call.message.answer(text, reply_markup=inline_check(trade_id))

    except Exception as e:
        print(f"[CARD ERROR] {repr(e)}")
        await call.message.answer("Ошибка при создании заявки")


@dp.callback_query(F.data.startswith("pay:crypto:"))
async def cb_pay_crypto(call: CallbackQuery):
    await call.answer()
    await call.message.answer("Крипто не подключена")


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
            await call.message.answer("Уже обработан")
            return

        js = await paysync_check(trade_id)
        status = extract_status(js)

        if status in {"paid", "success", "succeeded", "completed"}:
            async with pool.acquire() as con:
                async with con.transaction():
                    inv = await con.fetchrow(
                        "SELECT * FROM invoices WHERE trade_id=$1 FOR UPDATE", trade_id
                    )

                    if not inv or inv["status"] == "paid":
                        await call.message.answer("Обработан")
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
                            f"✅ Платеж подтвержден!\n\n"
                            f"Баланс +{inv['amount_int']} {inv['currency']}"
                        )
                        return

                    if inv["kind"] == "purchase":
                        item = await con.fetchrow(
                            "SELECT * FROM products WHERE id=$1 FOR UPDATE",
                            inv["product_id"]
                        )

                        if not item:
                            await call.message.answer("Товар не найден")
                            return

                        if item["sold_at"]:
                            await con.execute(
                                "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                                trade_id
                            )
                            await call.message.answer("Товар уже продан")
                            return

                        await con.execute(
                            "UPDATE products SET sold_to=$2, sold_at=NOW() WHERE id=$1",
                            inv["product_id"], inv["user_id"]
                        )

                        await con.execute(
                            "UPDATE users SET orders_count = orders_count + 1 WHERE user_id = $1",
                            inv["user_id"]
                        )

                        await con.execute(
                            "INSERT INTO purchases(user_id, product_id, item_name, category, price, link, provider) VALUES($1,$2,$3,$4,$5,$6,$7)",
                            inv["user_id"], inv["product_id"], item["name"], item["category"], item["price"], item["link"], "paysync"
                        )

                        await con.execute(
                            "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                            trade_id
                        )

                        await call.message.answer(
                            f"✅ Успешно!\n\n"
                            f"{item['category']}\n\n"
                            f"🔗 Ссылка:\n{item['link']}"
                        )
                        return

        elif status in {"expired", "cancelled", "canceled", "failed"}:
            async with pool.acquire() as con:
                await con.execute(
                    "UPDATE invoices SET status=$2 WHERE trade_id=$1",
                    trade_id, status
                )

            await call.message.answer(f"Платеж отклонен: {status}")
            return

        else:
            await call.message.answer("Не подтвержден. Попробуй через минуту")
            return

    except Exception as e:
        print(f"[CHECK ERROR] {repr(e)}")
        await call.message.answer("Ошибка проверки")


@dp.callback_query(F.data == "profile:history")
async def cb_history(call: CallbackQuery):
    await call.answer()
    assert pool is not None

    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT item_name, category, link, price, provider, created_at
            FROM purchases
            WHERE user_id=$1
            ORDER BY created_at DESC
            LIMIT 20
        """, call.from_user.id)

    if not rows:
        await call.message.answer("История п��ста")
        return

    text = "🧾 История:\n\n"
    for r in rows:
        dt = r["created_at"].strftime("%d.%m %H:%M")
        price = decimal.Decimal(r["price"])
        text += f"• {r['category']} — {price:.0f} {UAH} ({dt})\n{r['link']}\n\n"

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
                    uid, promo["discount"]
                )

                await con.execute(
                    "INSERT INTO promo_usage(user_id, promo_code) VALUES($1, $2)",
                    uid, code
                )

                await con.execute(
                    "UPDATE promo_codes SET current_uses = current_uses + 1 WHERE code = $1",
                    code
                )

        await message.answer(f"✅ Активирован\nДобавлено: {promo['discount']} {UAH}")

    except Exception as e:
        print(f"[PROMO ERROR] {repr(e)}")
        await message.answer(f"Ошибка: {str(e)[:100]}")
    finally:
        await state.clear()


# ================== ADMIN ==================
@dp.message(F.text.startswith("/addstock"))
async def cmd_add_stock(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        raw = message.text.replace("/addstock", "", 1).strip()
        parts = [p.strip() for p in raw.split("|")]

        if len(parts) < 5:
            await message.answer("Формат: /addstock город | категория | цена | ссылка | описание")
            return

        city, category, price_raw, link, desc = parts[:5]

        if city not in ["odesa", "lviv"]:
            await message.answer("Город: odesa или lviv")
            return

        price = decimal.Decimal(price_raw)
        code = f"{category.lower()}-{uuid.uuid4().hex[:6]}"

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO products(code, city, name, category, price, link, description, is_active, photo_id)
                VALUES($1,$2,$3,$4,$5,$6,$7,TRUE,$8)
            """, code, city, category, category, price, link, desc, "photo_placeholder")

        await message.answer(f"✅ Добавлено: {category} — {price:.0f} {UAH}\nКод: {code}")

    except Exception as e:
        print(f"[ADD ERROR] {repr(e)}")
        await message.answer(f"Ошибка: {str(e)[:100]}")


@dp.message(F.text.startswith("/addpromo"))
async def cmd_add_promo(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        raw = message.text.replace("/addpromo", "", 1).strip()
        parts = [p.strip() for p in raw.split("|")]

        if len(parts) < 3:
            await message.answer("Формат: /addpromo КОД | СУММА | МАКС")
            return

        code = parts[0].upper()
        discount = int(parts[1])
        max_uses = int(parts[2])

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO promo_codes(code, discount, max_uses, is_active)
                VALUES($1,$2,$3,TRUE)
                ON CONFLICT (code) DO NOTHING
            """, code, discount, max_uses)

        await message.answer(f"✅ Промокод: {code} = {discount} {UAH} × {max_uses}")

    except Exception as e:
        print(f"[PROMO ADD ERROR] {repr(e)}")
        await message.answer(f"Ошибка: {str(e)[:100]}")


@dp.message(F.text.startswith("/stock"))
async def cmd_stock(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        assert pool is not None
        async with pool.acquire() as con:
            rows = await con.fetch("""
                SELECT code, city, category, price, sold_at, is_active
                FROM products
                ORDER BY created_at DESC
                LIMIT 50
            """)

        if not rows:
            await message.answer("Товаров нет")
            return

        text = "📦 Товары:\n\n"
        for r in rows:
            state = "✅" if not r["sold_at"] else "🔒"
            state = "⏹" if not r["is_active"] else state
            text += f"{state} {r['code']} | {r['category']} ({r['city']}) — {decimal.Decimal(r['price']):.0f} {UAH}\n"

        await message.answer(text)

    except Exception as e:
        await message.answer(f"Ошибка: {str(e)[:100]}")


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
