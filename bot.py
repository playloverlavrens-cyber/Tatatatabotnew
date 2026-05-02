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

ADMIN_ID = int(ADMIN_ID_RAW)
CLIENT_ID = int(PAYSYNC_CLIENT_ID_RAW)
UAH = "₴"
UKRAINE_TZ = ZoneInfo("Europe/Kyiv")

# ================== CONST ==================
MAIN_PHOTO_URL = "https://postimg.cc/VJW1MWxQ/7f592b2b"
PRODUCT_PHOTO_URL = "https://postimg.cc/w1gY6s1d/a98e25ac"

CITIES = {
    "odesa": {"emoji": "🌊", "name": "Одесса"},
    "kyiv": {"emoji": "🏛️", "name": "Київ"},
    "poltava": {"emoji": "🌳", "name": "Полтава"}
}

MAIN_TEXT = """🏢 TrustCity — Надійний магазин онлайн

⭐ Наші переваги:
• Верифіковані товари преміум якості
• Швидке оновлення каталогу
• Безпечні методи оплати
• Відслідковується кожна продаж
• 24/7 піддтримка

📞 Зв'язок:
🤖 Бот: @TrustCity1_bot
👨‍💼 Оператор: @TrustCitySupport

🏦 Баланс: {balance} {uah}
📦 Покупок: {orders}"""

PROFILE_TEXT = """👤 ПРОФІЛЬ

🏦 Баланс: {balance} {uah}
📦 Кількість покупок: {orders}

Управління профілем та платежами"""

RULES_TEXT = """📋 ПРАВИЛА ТА УМОВИ

1️⃣ ПОРЯДОК РОБОТИ
• Вибір товару → Вибір району → Оплата
• Товар резервується на час оплати
• Після підтвердження платежу товар переходить у вас

2️⃣ ОПЛАТА
• PaySync — прямий переведення на карту
• Баланс — деньги з вашого рахунку
• Строк оплати: 15 хвилин з моменту створення заявки

3️⃣ ДОСТАВКА
• Кожна позиція має фото
• Данні про район вказані в описі
• Підтвердження: ID pokупця + дата/час в базі даних

4️⃣ КОНФІДЕНЦІЙНІСТЬ
• Ваш користувач і покупки лишаються секретом
• Немає розголошення даних третім особам
• Всі операції фіксуються в нашій базі

5️⃣ ГАРАНТІЇ
• Прозорість всіх операцій
• Можливість виводу коштів з балансу
• Технічна підтримка: @TrustCitySupport"""

HELP_TEXT = "Питання? Напиши оператору: @TrustCitySupport"

# ================== DB ==================
pool: asyncpg.Pool | None = None


def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID


def get_ukraine_time() -> datetime:
    return datetime.now(UKRAINE_TZ)


def format_ukraine_time(dt: datetime | None) -> str:
    if not dt:
        return "невідомо"
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
                username TEXT,
                balance NUMERIC(12,2) DEFAULT 0,
                orders_count INT DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await con.execute("""
            CREATE TABLE IF NOT EXISTS stock (
                id BIGSERIAL PRIMARY KEY,
                city TEXT NOT NULL,
                product_name TEXT NOT NULL,
                districts JSONB NOT NULL,
                price NUMERIC(12,2) NOT NULL,
                photo_id TEXT NOT NULL,
                description TEXT DEFAULT '',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await con.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE SET NULL,
                username TEXT,
                stock_id BIGINT NOT NULL,
                product_name TEXT NOT NULL,
                city TEXT NOT NULL,
                district TEXT NOT NULL,
                price NUMERIC(12,2) NOT NULL,
                payment_method TEXT,
                paid_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await con.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                trade_id TEXT PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                stock_id BIGINT,
                kind TEXT,
                amount_int INT,
                currency TEXT DEFAULT 'UAH',
                provider TEXT,
                status TEXT DEFAULT 'wait',
                expires_at TIMESTAMPTZ,
                paid_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)


async def ensure_user(uid: int, username: str = "") -> None:
    assert pool is not None
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO users(user_id, username) VALUES($1, $2) ON CONFLICT(user_id) DO UPDATE SET username=COALESCE($2, username)",
            uid, username
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
            [KeyboardButton(text="🏠 ГОЛОВНА"), KeyboardButton(text="👤 ПРОФІЛЬ")],
            [KeyboardButton(text="💬 ДОПОМОГА"), KeyboardButton(text="📋 ПРАВИЛА ПЗ")]
        ],
        resize_keyboard=True
    )


def inline_cities() -> InlineKeyboardMarkup:
    kb = []
    for city_key, city_data in CITIES.items():
        kb.append([
            InlineKeyboardButton(
                text=f"{city_data['emoji']} {city_data['name']}",
                callback_data=f"city:{city_key}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def inline_product(city: str, product_name: str, photo_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛒 Купити", callback_data=f"buy:{city}:{product_name}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back:cities")]
        ]
    )


def inline_districts(city: str, product_name: str, districts_list: list) -> InlineKeyboardMarkup:
    if not districts_list:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="❌ Нема в наявності", callback_data="noop")]]
        )

    kb = []
    for district in districts_list:
        kb.append([
            InlineKeyboardButton(
                text=f"📍 {district}",
                callback_data=f"select_district:{city}:{product_name}:{district}"
            )
        ])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:cities")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def inline_pay(stock_id: int, district: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 PaySync", callback_data=f"pay:card:{stock_id}:{district}")],
            [InlineKeyboardButton(text="💰 Баланс", callback_data=f"pay:bal:{stock_id}:{district}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back:product")]
        ]
    )


def inline_profile() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Поповнити баланс", callback_data="profile:topup")],
            [InlineKeyboardButton(text="📊 Історія покупок", callback_data="profile:history")]
        ]
    )


def inline_check(trade_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Перевірити оплату", callback_data=f"check:{trade_id}")]
        ]
    )

# ================== PAYMENT ==================
async def paysync_create(amount: int, data: str) -> dict:
    url = f"https://paysync.bot/api/client{CLIENT_ID}/amount{amount}/currencyUAH"
    headers = {"apikey": PAYSYNC_APIKEY}
    params = {"data": data}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params, timeout=30) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"PaySync HTTP {resp.status}")
            return await resp.json()


async def paysync_check(trade_id: str) -> dict:
    url = f"https://paysync.bot/gettrans/{trade_id}"
    headers = {"apikey": PAYSYNC_APIKEY}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=30) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"PaySync HTTP {resp.status}")
            return await resp.json()


def extract_trade_id(js: dict) -> str:
    trade_id = js.get("trade")
    if trade_id is None:
        raise RuntimeError("No trade ID")
    return str(trade_id)


def extract_card_number(js: dict) -> str:
    card = js.get("card_number")
    return str(card).strip() if card else "не отримана"


def extract_status(js: dict) -> str:
    return str(js.get("status", "")).strip().lower()


def extract_amount(js: dict, fallback: int) -> int:
    raw = js.get("amount", fallback)
    try:
        return int(raw)
    except:
        try:
            return int(decimal.Decimal(str(raw)))
        except:
            return fallback

# ================== FSM ==================
class TopupStates(StatesGroup):
    waiting_amount = State()


class AddStockStates(StatesGroup):
    waiting_photo = State()

# ================== BOT ==================
dp = Dispatcher(storage=MemoryStorage())


@dp.message(CommandStart())
async def cmd_start(message: Message):
    username = message.from_user.username or "unknown"
    await ensure_user(message.from_user.id, username)
    bal, orders = await get_stats(message.from_user.id)
    
    text = MAIN_TEXT.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    
    try:
        await message.answer_photo(
            photo=MAIN_PHOTO_URL,
            caption=text,
            reply_markup=bottom_menu()
        )
    except:
        await message.answer(text, reply_markup=bottom_menu())


@dp.message(F.text.contains("🏠 ГОЛОВНА"))
async def btn_main(message: Message):
    username = message.from_user.username or "unknown"
    await ensure_user(message.from_user.id, username)
    bal, orders = await get_stats(message.from_user.id)
    
    text = MAIN_TEXT.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    
    try:
        await message.answer_photo(
            photo=MAIN_PHOTO_URL,
            caption=text,
            reply_markup=inline_cities()
        )
    except:
        await message.answer(text, reply_markup=inline_cities())


@dp.message(F.text.contains("👤 ПРОФІЛЬ"))
async def btn_profile(message: Message):
    username = message.from_user.username or "unknown"
    await ensure_user(message.from_user.id, username)
    bal, orders = await get_stats(message.from_user.id)
    
    text = PROFILE_TEXT.format(balance=f"{bal:.2f}", orders=orders, uah=UAH)
    await message.answer(text, reply_markup=inline_profile())


@dp.message(F.text.contains("💬 ДОПОМОГА"))
async def btn_help(message: Message):
    await message.answer(HELP_TEXT, reply_markup=bottom_menu())


@dp.message(F.text.contains("📋 ПРАВИЛА ПЗ"))
async def btn_rules(message: Message):
    await message.answer(RULES_TEXT, reply_markup=bottom_menu())


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
            SELECT DISTINCT product_name, photo_id, price, description
            FROM stock
            WHERE city=$1 AND is_active=TRUE
            ORDER BY product_name
        """, city)

    if not rows:
        await call.message.answer("❌ Товарів немає", reply_markup=inline_cities())
        return

    city_name = CITIES[city]["name"]
    text = f"🏪 Товари в {city_name}:\n\nВибери товар:"
    
    kb = []
    for r in rows:
        kb.append([
            InlineKeyboardButton(
                text=f"{r['product_name']} • {decimal.Decimal(r['price']):.0f} {UAH}",
                callback_data=f"prod:{city}:{r['product_name']}"
            )
        ])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:cities")])
    
    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@dp.callback_query(F.data.startswith("prod:"))
async def cb_product(call: CallbackQuery):
    await call.answer()
    _, city, product = call.data.split(":", 2)
    assert pool is not None

    async with pool.acquire() as con:
        row = await con.fetchrow("""
            SELECT photo_id, price, description, districts
            FROM stock
            WHERE city=$1 AND product_name=$2 AND is_active=TRUE
            LIMIT 1
        """, city, product)

    if not row:
        await call.message.answer("❌ Товар недоступний")
        return

    districts_list = json.loads(row['districts']) if isinstance(row['districts'], str) else row['districts']

    text = f"""📦 {product}

💰 Ціна: {decimal.Decimal(row['price']):.0f} {UAH}

📝 Опис: {row['description'] or 'Немає описання'}

🏙️ Вибери район:"""

    kb = []
    for district in districts_list:
        kb.append([
            InlineKeyboardButton(
                text=f"📍 {district}",
                callback_data=f"district:{city}:{product}:{district}"
            )
        ])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:cities")])

    try:
        await call.message.answer_photo(
            photo=row['photo_id'],
            caption=text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )
    except:
        await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@dp.callback_query(F.data.startswith("district:"))
async def cb_district(call: CallbackQuery):
    await call.answer()
    _, city, product, district = call.data.split(":", 3)
    assert pool is not None

    async with pool.acquire() as con:
        row = await con.fetchrow("""
            SELECT id, photo_id, price, description
            FROM stock
            WHERE city=$1 AND product_name=$2 AND is_active=TRUE
            LIMIT 1
        """, city, product)

    if not row:
        await call.message.answer("❌ Товар недоступний")
        return

    stock_id = row['id']
    price = decimal.Decimal(row['price'])

    text = f"""✅ ЗАМОВЛЕННЯ

📦 Товар: {product}
🏙️ Район: {district}
💰 Ціна: {price:.0f} {UAH}

Виберіть спосіб оплати:"""

    kb = [
        [InlineKeyboardButton(text="💳 PaySync", callback_data=f"pay:card:{stock_id}:{district}")],
        [InlineKeyboardButton(text="💰 Баланс", callback_data=f"pay:bal:{stock_id}:{district}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back:cities")]
    ]

    try:
        await call.message.answer_photo(
            photo=row['photo_id'],
            caption=text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )
    except:
        await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@dp.callback_query(F.data.startswith("pay:bal:"))
async def cb_pay_balance(call: CallbackQuery):
    await call.answer()
    parts = call.data.split(":")
    stock_id = int(parts[2])
    district = parts[3] if len(parts) > 3 else "unknown"
    uid = call.from_user.id

    try:
        assert pool is not None
        async with pool.acquire() as con:
            async with con.transaction():
                item = await con.fetchrow(
                    "SELECT * FROM stock WHERE id=$1 FOR UPDATE", stock_id
                )

                if not item:
                    await call.message.answer("❌ Товар недоступний")
                    return

                user = await con.fetchrow("SELECT * FROM users WHERE user_id=$1 FOR UPDATE", uid)
                if not user:
                    username = call.from_user.username or "unknown"
                    await ensure_user(uid, username)
                    user = await con.fetchrow("SELECT * FROM users WHERE user_id=$1", uid)

                price = decimal.Decimal(item["price"])
                balance = decimal.Decimal(user["balance"])

                if balance < price:
                    await call.message.answer(
                        f"❌ Недостатньо коштів\n\n"
                        f"Потрібно: {price:.0f} {UAH}\n"
                        f"На рахунку: {balance:.0f} {UAH}"
                    )
                    return

                await con.execute(
                    "UPDATE users SET balance = balance - $2, orders_count = orders_count + 1 WHERE user_id = $1",
                    uid, price
                )

                current_time = get_ukraine_time()
                username = call.from_user.username or "user_id_" + str(uid)

                await con.execute("""
                    INSERT INTO sales(user_id, username, stock_id, product_name, city, district, price, payment_method, paid_at)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                """,
                    uid, username, stock_id, item["product_name"],
                    item["city"], district, price, "balance", current_time
                )

        text = f"""✅ ПОКУПКА УСПІШНА

📦 Товар: {item['product_name']}
📍 Район: {district}
💰 Ціна: {price:.0f} {UAH}
⏰ Час: {format_ukraine_time(current_time)}

Дякуємо за покупку! 🎉"""

        try:
            await call.message.answer_photo(
                photo=item["photo_id"],
                caption=text
            )
        except:
            await call.message.answer(text)

    except Exception as e:
        print(f"[PAY BALANCE ERROR] {repr(e)}")
        await call.message.answer(f"❌ Помилка: {str(e)[:200]}")


@dp.callback_query(F.data.startswith("pay:card:"))
async def cb_pay_card(call: CallbackQuery):
    await call.answer()
    parts = call.data.split(":")
    stock_id = int(parts[2])
    district = parts[3] if len(parts) > 3 else "unknown"
    uid = call.from_user.id

    try:
        assert pool is not None

        async with pool.acquire() as con:
            item = await con.fetchrow(
                "SELECT * FROM stock WHERE id=$1 FOR UPDATE", stock_id
            )

            if not item:
                await call.message.answer("❌ Товар недоступний")
                return

            price = int(decimal.Decimal(item["price"]))
            nonce = uuid.uuid4().hex[:12]
            payload = f"buy:{uid}:{stock_id}:{nonce}"

        js = await paysync_create(price, payload)

        trade_id = extract_trade_id(js)
        card_number = extract_card_number(js)
        real_amount = extract_amount(js, price)
        paysync_time = extract_paysync_time(js) if "time" in js else ""
        expires_at = get_ukraine_time() + timedelta(minutes=PAYMENT_TIMEOUT_MINUTES)

        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO invoices(
                    trade_id, user_id, stock_id, kind, amount_int, currency, provider, status, expires_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (trade_id) DO NOTHING
            """,
                trade_id, uid, stock_id, "purchase", real_amount,
                PAYSYNC_CURRENCY, "paysync", "wait", expires_at
            )

        expire_text = format_ukraine_time(expires_at)
        text = (
            f"""💳 ОПЛАТА ЗАМОВЛЕННЯ

📦 Товар: {item['product_name']}
📍 Район: {district}

🧾 Заявка: {trade_id}
🏦 Карта: {card_number}
💰 Сума: {real_amount} {PAYSYNC_CURRENCY}
⏳ Термін: до {expire_text}

❗️ Переведи точну суму одним платежем"""
        )
        
        kb = [
            [InlineKeyboardButton(text="✅ Перевірити оплату", callback_data=f"check:{trade_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back:cities")]
        ]

        try:
            await call.message.answer_photo(
                photo=item["photo_id"],
                caption=text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
            )
        except:
            await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

    except Exception as e:
        print(f"[CARD ERROR] {repr(e)}")
        await call.message.answer(f"❌ Помилка: {str(e)[:200]}")


@dp.callback_query(F.data.startswith("check:"))
async def cb_check(call: CallbackQuery):
    await call.answer()
    trade_id = call.data.split(":", 1)[1]

    try:
        assert pool is not None

        async with pool.acquire() as con:
            inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)

        if not inv:
            await call.message.answer("❌ Заявку не знайдено")
            return

        if inv["status"] == "paid":
            await call.message.answer("✅ Заявка вже оброблена")
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
                        await call.message.answer("✅ Платіж обробляється")
                        return

                    item = await con.fetchrow("SELECT * FROM stock WHERE id=$1", inv["stock_id"])
                    user = await con.fetchrow("SELECT username FROM users WHERE user_id=$1", inv["user_id"])

                    username = user["username"] if user else "unknown"
                    
                    # Отримай район з попередньої заявки
                    district_info = await con.fetchval("""
                        SELECT district FROM sales WHERE stock_id=$1 ORDER BY paid_at DESC LIMIT 1
                    """, inv["stock_id"])

                    current_time = get_ukraine_time()

                    await con.execute("""
                        INSERT INTO sales(user_id, username, stock_id, product_name, city, district, price, payment_method, paid_at)
                        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    """,
                        inv["user_id"], username, inv["stock_id"],
                        item["product_name"], item["city"],
                        district_info or "unknown", inv["amount_int"], "paysync", current_time
                    )

                    await con.execute(
                        "UPDATE users SET orders_count = orders_count + 1 WHERE user_id = $1",
                        inv["user_id"]
                    )

                    await con.execute(
                        "UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1",
                        trade_id
                    )

            text = f"""✅ ПОКУПКА УСПІШНА

📦 Товар: {item['product_name']}
💰 Сума: {inv['amount_int']} {inv['currency']}
⏰ Час: {format_ukraine_time(current_time)}

Дякуємо за замовлення! 🎉"""

            try:
                await call.message.answer_photo(
                    photo=item["photo_id"],
                    caption=text
                )
            except:
                await call.message.answer(text)
            return

        elif status in {"expired", "cancelled", "canceled", "failed"}:
            async with pool.acquire() as con:
                await con.execute(
                    "UPDATE invoices SET status=$2 WHERE trade_id=$1",
                    trade_id, status
                )

            await call.message.answer(f"❌ Платіж скасований: {status}")
            return

        else:
            await call.message.answer("⏳ Платіж ще не підтверджено. Спробуй через хвилину")
            return

    except Exception as e:
        print(f"[CHECK ERROR] {repr(e)}")
        await call.message.answer(f"❌ Помилка: {str(e)[:200]}")


@dp.callback_query(F.data == "profile:topup")
async def cb_topup(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(TopupStates.waiting_amount)
    await call.message.answer(f"💳 Введи суму пополнення (мін. 10 {UAH}):")


@dp.message(TopupStates.waiting_amount)
async def topup_amount(message: Message, state: FSMContext):
    amount = parse_amount(message.text)
    if not amount or amount < 10:
        await message.answer(f"❌ Мінімум 10 {UAH}")
        return

    username = message.from_user.username or "unknown"
    await ensure_user(message.from_user.id, username)

    try:
        nonce = uuid.uuid4().hex[:12]
        payload = f"topup:{message.from_user.id}:{nonce}"
        js = await paysync_create(amount, payload)

        trade_id = extract_trade_id(js)
        card_number = extract_card_number(js)
        real_amount = extract_amount(js, amount)
        paysync_time = extract_paysync_time(js) if "time" in js else ""
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
            f"""💳 ПОПОЛНЕННЯ БАЛАНСУ

🧾 Заявка: {trade_id}
🏦 Карта: {card_number}
💰 Сума: {real_amount} {PAYSYNC_CURRENCY}
⏳ Термін: до {expire_text}

❗️ Переведи точну суму одним платежем"""
        )
        await message.answer(text, reply_markup=inline_check(trade_id))

    except Exception as e:
        print(f"[TOPUP ERROR] {repr(e)}")
        await message.answer(f"❌ Помилка: {str(e)[:200]}")
    finally:
        await state.clear()


@dp.callback_query(F.data == "profile:history")
async def cb_history(call: CallbackQuery):
    await call.answer()
    assert pool is not None

    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT product_name, district, city, price, payment_method, paid_at
            FROM sales
            WHERE user_id=$1 OR username=$2
            ORDER BY paid_at DESC
            LIMIT 20
        """, call.from_user.id, call.from_user.username or "")

    if not rows:
        await call.message.answer("📭 Історія пуста", reply_markup=inline_profile())
        return

    text = "📊 ІСТОРІЯ ПОКУПОК:\n\n"
    for r in rows:
        dt = format_ukraine_time(r["paid_at"])
        price = decimal.Decimal(r["price"])
        text += f"""• {r['product_name']}
   📍 {r['district']} ({r['city']})
   💰 {price:.0f} {UAH}
   ⏰ {dt}\n\n"""

    await call.message.answer(text, reply_markup=inline_profile())


@dp.callback_query(F.data.startswith("back:"))
async def cb_back(call: CallbackQuery):
    await call.answer()
    await cmd_start(call.message)


# ================== ADMIN COMMANDS ==================
@dp.message(F.text.startswith("/addstock"))
async def cmd_add_stock(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        raw = message.text.replace("/addstock", "", 1).strip()
        parts = [p.strip() for p in raw.split("|")]

        if len(parts) < 4:
            await message.answer("Формат: /addstock город | товар | районы(через,запятую) | цена | описание")
            return

        city = parts[0].lower()
        product = parts[1]
        districts_raw = parts[2]
        price_raw = parts[3]
        desc = parts[4] if len(parts) > 4 else ""

        if city not in CITIES:
            await message.answer(f"Город: {', '.join(CITIES.keys())}")
            return

        price = decimal.Decimal(price_raw)
        districts_list = [d.strip() for d in districts_raw.split(",")]

        await state.set_state(AddStockStates.waiting_photo)
        await state.update_data(
            city=city, product=product, districts=districts_list,
            price=price, desc=desc
        )
        await message.answer(f"📸 Відправ фото для {product}")

    except Exception as e:
        await message.answer(f"❌ Помилка: {str(e)[:300]}")


@dp.message(AddStockStates.waiting_photo, F.photo)
async def handle_stock_photo(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        photo_id = message.photo[-1].file_id

        assert pool is not None
        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO stock(city, product_name, districts, price, photo_id, description, is_active)
                VALUES($1,$2,$3,$4,$5,$6,TRUE)
            """,
                data["city"], data["product"],
                json.dumps(data["districts"]),
                data["price"], photo_id, data["desc"]
            )

        districts_str = ", ".join(data["districts"])
        await message.answer(
            f"✅ Товар додано\n\n"
            f"{data['product']}\n"
            f"📍 Райони: {districts_str}\n"
            f"💰 Ціна: {data['price']:.0f} {UAH}"
        )

    except Exception as e:
        await message.answer(f"❌ Помилка: {str(e)[:300]}")
    finally:
        await state.clear()


@dp.message(F.text.startswith("/sales"))
async def cmd_sales(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        assert pool is not None
        async with pool.acquire() as con:
            rows = await con.fetch("""
                SELECT id, user_id, username, product_name, city, district, price, payment_method, paid_at
                FROM sales
                ORDER BY paid_at DESC
                LIMIT 100
            """)

        if not rows:
            await message.answer("📭 Продажів немає")
            return

        text = "💰 ТАБЛИЦЯ ПРОДАЖІВ:\n\n"
        for r in rows:
            dt = format_ukraine_time(r["paid_at"])
            text += f"""ID: {r['id']}
👤 {r['username']} (uid: {r['user_id']})
📦 {r['product_name']} • {r['city']} • {r['district']}
💰 {decimal.Decimal(r['price']):.0f} {UAH} • {r['payment_method']}
⏰ {dt}
━━━━━━━━━━━━━━\n"""

        await message.answer(text)

    except Exception as e:
        await message.answer(f"❌ Помилка: {str(e)[:300]}")


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
