import os
import asyncio
import decimal
import asyncpg
import aiohttp
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ================== CONFIG ==================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
PAYSYNC_APIKEY = os.getenv("PAYSYNC_APIKEY", "").strip()
PAYSYNC_CLIENT_ID = os.getenv("PAYSYNC_CLIENT_ID", "").strip()
PAYSYNC_CURRENCY = os.getenv("PAYSYNC_CURRENCY", "UAH").strip().upper()
CRYPTO_PAY_API_TOKEN = os.getenv("CRYPTO_PAY_API_TOKEN", "").strip()
CRYPTO_PAY_BASE_URL = os.getenv("CRYPTO_PAY_BASE_URL", "https://pay.crypt.bot/api").strip().rstrip("/")
CRYPTO_PAY_FIAT = os.getenv("CRYPTO_PAY_FIAT", "UAH").strip().upper()
CRYPTO_PAY_ACCEPTED_ASSETS = os.getenv("CRYPTO_PAY_ACCEPTED_ASSETS", "USDT,TON,BTC,ETH").strip()
PAYMENT_TIMEOUT_MINUTES = int(os.getenv("PAYMENT_TIMEOUT_MINUTES", "15"))
RESERVATION_MINUTES = int(os.getenv("RESERVATION_MINUTES", "15"))

# Validation
for name, val in [("BOT_TOKEN", BOT_TOKEN), ("DATABASE_URL", DATABASE_URL), ("ADMIN_ID", ADMIN_ID_RAW),
                   ("PAYSYNC_APIKEY", PAYSYNC_APIKEY), ("PAYSYNC_CLIENT_ID", PAYSYNC_CLIENT_ID)]:
    if not val or (name == "PAYSYNC_CLIENT_ID" and not val.isdigit()):
        raise RuntimeError(f"Missing or invalid: {name}")

ADMIN_ID = int(ADMIN_ID_RAW)
CLIENT_ID = int(PAYSYNC_CLIENT_ID)
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
🛍️ Заказов: {orders}
"""

PROFILE_TEXT = "👤 Профиль\n\n🏦 Баланс: {balance} {uah}\n🛍️ Заказов: {orders}"
HELP_TEXT = "Вопросы? Пиши: @YourSupport"
ITEM_TEXT = "✅ Выбрал: {name}\n💰 Цена: {price} {uah}\n\n{desc}"
TOPUP_TEXT = f"💳 Введи сумму в {UAH} (целое число):\nПример: 150"

# ================== DB ==================
pool: asyncpg.Pool | None = None

def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def parse_amount(text: str) -> int | None:
    try:
        d = decimal.Decimal((text or "").strip().replace(",", "."))
        return int(d) if d > 0 and d == d.quantize(decimal.Decimal("1")) else None
    except: return None

async def db_init() -> None:
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
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

async def ensure_user(uid: int) -> None:
    assert pool
    async with pool.acquire() as con:
        await con.execute("INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING", uid)

async def get_stats(uid: int) -> tuple[decimal.Decimal, int]:
    assert pool
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT balance, orders_count FROM users WHERE user_id=$1", uid)
    return (decimal.Decimal(row["balance"]), row["orders_count"]) if row else (decimal.Decimal("0"), 0)

async def cleanup_expired() -> None:
    assert pool
    async with pool.acquire() as con:
        await con.execute("""UPDATE products SET reserved_by=NULL, reserved_until=NULL 
                           WHERE reserved_until < NOW() AND sold_at IS NULL""")
        await con.execute("""UPDATE invoices SET status='expired' WHERE status='wait' AND expires_at < NOW()""")

async def background_cleanup():
    while True:
        try:
            await cleanup_expired()
        except Exception as e:
            print(f"[cleanup] {e}")
        await asyncio.sleep(60)

# ================== KEYBOARDS ==================
def bottom_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="ГЛАВНАЯ 🔘"), KeyboardButton(text="ПРОФИЛЬ 👤")],
        [KeyboardButton(text="ПОМОЩЬ 💬"), KeyboardButton(text="РАБОТА 💸")]
    ], resize_keyboard=True)

def inline_city() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Одесса", callback_data="city:odesa")]])

def inline_products(rows: list, city: str) -> InlineKeyboardMarkup:
    if not rows:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Нет товаров", callback_data="noop")]])
    kb = [[InlineKeyboardButton(text=f"{r['name']} — {decimal.Decimal(r['price']):.2f} {UAH}", 
                               callback_data=f"prod:{city}:{r['code']}")] for r in rows]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def inline_pay(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Балансом", callback_data=f"pay:bal:{code}")],
        [InlineKeyboardButton(text="Картой", callback_data=f"pay:card:{code}")],
        [InlineKeyboardButton(text="Crypto", callback_data=f"pay:crypto:{code}")]
    ])

def inline_profile() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Пополнить", callback_data="profile:topup")],
        [InlineKeyboardButton(text="🎟 Промокод", callback_data="profile:promo")],
        [InlineKeyboardButton(text="🧾 История", callback_data="profile:history")]
    ])

def inline_check(trade_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Проверить", callback_data=f"check:{trade_id}")]])

# ================== PAYMENT ==================
async def paysync_create(amount: int, data: str) -> dict:
    url = f"https://paysync.bot/api/client{CLIENT_ID}/amount{amount}/currency{PAYSYNC_CURRENCY}?data={quote(data)}"
    headers = {"Content-Type": "application/json", "apikey": PAYSYNC_APIKEY}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=30) as resp:
            return await resp.json()

async def paysync_check(trade_id: str) -> dict:
    url = f"https://paysync.bot/gettrans/{trade_id}"
    headers = {"apikey": PAYSYNC_APIKEY}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, timeout=30) as resp:
            return await resp.json()

async def crypto_request(method: str, payload: dict = None) -> dict:
    if not CRYPTO_PAY_API_TOKEN:
        raise RuntimeError("CRYPTO_PAY_API_TOKEN missing")
    url = f"{CRYPTO_PAY_BASE_URL}/{method}"
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_API_TOKEN, "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload or {}, timeout=30) as resp:
            js = await resp.json()
    if not js.get("ok"):
        raise RuntimeError(f"Crypto error: {js.get('error')}")
    return js

async def crypto_create_invoice(amount: int, title: str, payload: str) -> dict:
    body = {
        "currency_type": "fiat",
        "fiat": CRYPTO_PAY_FIAT,
        "accepted_assets": CRYPTO_PAY_ACCEPTED_ASSETS,
        "amount": f"{amount:.2f}",
        "description": title[:1024],
        "payload": payload[:4096],
        "allow_comments": False,
        "allow_anonymous": True,
        "expires_in": PAYMENT_TIMEOUT_MINUTES * 60
    }
    result = await crypto_request("createInvoice", body)
    return result.get("result", {})

# ================== FSM ==================
class TopupStates(StatesGroup):
    waiting_amount = State()

class PromoStates(StatesGroup):
    waiting_code = State()

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

@dp.callback_query(F.data == "city:odesa")
async def cb_city(call: CallbackQuery):
    await call.answer()
    assert pool
    async with pool.acquire() as con:
        rows = await con.fetch("""SELECT code, name, price FROM products 
                                 WHERE city='odesa' AND is_active=TRUE AND sold_at IS NULL
                                 AND (reserved_until IS NULL OR reserved_until < NOW())
                                 ORDER BY created_at DESC LIMIT 20""")
    await call.message.answer("Выбери товар:", reply_markup=inline_products(rows, "odesa"))

@dp.callback_query(F.data == "profile:topup")
async def cb_topup(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(TopupStates.waiting_amount)
    await call.message.answer(f"💳 Выбери способ пополнения:\n\n{TOPUP_TEXT}")

@dp.message(TopupStates.waiting_amount)
async def topup_amount(message: Message, state: FSMContext):
    amount = parse_amount(message.text)
    if not amount or amount < 10:
        await message.answer(f"❌ Минимум 10 {UAH}")
        return
    
    await ensure_user(message.from_user.id)
    try:
        nonce = uuid.uuid4().hex[:10]
        payload = f"topup:{message.from_user.id}:{nonce}"
        js = await paysync_create(amount, payload)
        trade_id = js.get("trade")
        
        assert pool
        async with pool.acquire() as con:
            await con.execute("""INSERT INTO invoices(trade_id, user_id, kind, amount_int, provider, status)
                               VALUES($1,$2,$3,$4,$5,$6)""",
                            trade_id, message.from_user.id, "topup", amount, "paysync", "wait")
        
        await message.answer(f"Платёж создан! Номер: {trade_id}", reply_markup=inline_check(trade_id))
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
    finally:
        await state.clear()

@dp.callback_query(F.data.startswith("check:"))
async def cb_check(call: CallbackQuery):
    await call.answer()
    trade_id = call.data.split(":", 1)[1]
    try:
        js = await paysync_check(trade_id)
        if js.get("status") == "paid":
            assert pool
            async with pool.acquire() as con:
                inv = await con.fetchrow("SELECT * FROM invoices WHERE trade_id=$1", trade_id)
                if inv["kind"] == "topup":
                    await con.execute("UPDATE users SET balance = balance + $2 WHERE user_id=$1",
                                    inv["user_id"], inv["amount_int"])
                await con.execute("UPDATE invoices SET status='paid', paid_at=NOW() WHERE trade_id=$1", trade_id)
            await call.message.answer("✅ Оплачено!")
        else:
            await call.message.answer("⏳ Оплата ещё не подтверждена")
    except Exception as e:
        await call.message.answer(f"❌ Ошибка: {e}")

@dp.message(F.text.startswith("/addproduct"))
async def cmd_add(message: Message):
    if not is_admin(message.from_user.id):
        return
    try:
        parts = message.text.split("|")
        if len(parts) < 5:
            await message.answer("Формат: /addproduct город | код | название | цена | ссылка | описание")
            return
        
        city, code, name, price, link = [p.strip() for p in parts[:5]]
        desc = parts[5].strip() if len(parts) > 5 else ""
        
        assert pool
        async with pool.acquire() as con:
            await con.execute("""INSERT INTO products(code, city, name, price, link, description, is_active)
                               VALUES($1,$2,$3,$4,$5,$6,TRUE) ON CONFLICT(code) DO UPDATE SET
                               city=EXCLUDED.city, name=EXCLUDED.name, price=EXCLUDED.price""",
                            code, city, name, decimal.Decimal(price), link, desc)
        await message.answer(f"✅ Товар добавлен: {code}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

async def main():
    await db_init()
    bot = Bot(token=BOT_TOKEN)
    cleanup_task = asyncio.create_task(background_cleanup())
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        cleanup_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
