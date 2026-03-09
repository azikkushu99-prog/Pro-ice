import asyncio
import json
import re
import logging
from typing import Any, Awaitable, Callable, Dict

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, TelegramObject,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile
)

from config import BOT_TOKEN, ADMIN_IDS, NOTIFY_IDS, MIN_ORDER_AMOUNT, DB_PATH
import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════
#  КОНСТАНТЫ
# ═══════════════════════════════════════════════════════

PHONE_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📱 Поделиться номером", request_contact=True)]],
    resize_keyboard=True,
    one_time_keyboard=True,
)

PAYMENT_LABELS = {
    "card": "💳 Безналичная оплата",
    "cash": "💵 Наличный расчёт",
    "transfer": "📲 Перевод",
}
STATUS_LABELS = {
    "new": "🆕 Новый",
    "in_progress": "🔄 В работе",
    "delivered": "✅ Доставлен",
    "cancelled": "❌ Отменён",
}

WELCOME_TEXT = (
    "👋 Добро пожаловать в <b>ПРО лёд</b>!\n\n"
    "🧊 Мы доставляем премиальный лёд:\n"
    "  • Кубы, бруски, особые формы\n"
    "  • Лёд на развес (кг)\n"
    "  • Быстрая доставка по городу\n\n"
    "Выберите действие:"
)

CATALOG_TEXT = (
    "🧊 <b>Каталог льда</b>\n\n"
    "Выберите категорию:"
)

CONTACT_TEXT = (
    "📞 <b>Связь с менеджером</b>\n\n"
    "Telegram: @your_manager\n"
    "Телефон: +7 (XXX) XXX-XX-XX\n"
    "Время работы: 08:00 — 22:00"
)

# ═══════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════

async def safe_edit(cb: CallbackQuery, text: str, **kwargs):
    """Edit message text; if message is a photo — delete it and send new."""
    try:
        await cb.message.edit_text(text, **kwargs)
    except Exception:
        try:
            await cb.message.delete()
        except Exception:
            pass
        await cb.message.answer(text, **kwargs)


# ═══════════════════════════════════════════════════════
#  КЛАВИАТУРЫ ПОЛЬЗОВАТЕЛЯ
# ═══════════════════════════════════════════════════════

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🧊 Заказать лёд", callback_data="menu:order"),
        ],
        [
            InlineKeyboardButton(text="🛒 Корзина", callback_data="cart:show"),
            InlineKeyboardButton(text="📦 Мои заказы", callback_data="menu:my_orders"),
        ],
        [
            InlineKeyboardButton(text="🔁 Повторить заказ", callback_data="menu:repeat"),
            InlineKeyboardButton(text="📞 Контакты", callback_data="menu:contact"),
        ],
    ])


def catalog_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧊 Дикий лёд — поштучно", callback_data="cat:wild")],
        [InlineKeyboardButton(text="⚖️ Лёд на развес — за кг", callback_data="cat:weight")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main")],
    ])


def wild_sub_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟦 Кубы", callback_data="sub:cubes")],
        [InlineKeyboardButton(text="🔷 Бруски", callback_data="sub:bars")],
        [InlineKeyboardButton(text="✨ Особые формы", callback_data="sub:special")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="cat:back")],
    ])


PER_PAGE = 6


def product_list_kb(products: list, back_cb: str, page: int = 0, page_cb_prefix: str = "") -> InlineKeyboardMarkup:
    """Paginated product list. page_cb_prefix used for nav buttons."""
    total_pages = max(1, (len(products) + PER_PAGE - 1) // PER_PAGE)
    chunk = products[page * PER_PAGE:(page + 1) * PER_PAGE]
    btns = []
    for p in chunk:
        price_s = f" · {p['price']} ₽/{p['unit']}" if p["price"] > 0 else " · цена по запросу"
        btns.append([InlineKeyboardButton(
            text=f"{p['name']}{price_s}",
            callback_data=f"prod:{p['id']}",
        )])
    if total_pages > 1 and page_cb_prefix:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"{page_cb_prefix}:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"{page_cb_prefix}:{page + 1}"))
        btns.append(nav)
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def qty_kb(pid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1", callback_data=f"qty:{pid}:1"),
            InlineKeyboardButton(text="3", callback_data=f"qty:{pid}:3"),
            InlineKeyboardButton(text="5", callback_data=f"qty:{pid}:5"),
            InlineKeyboardButton(text="10", callback_data=f"qty:{pid}:10"),
        ],
        [InlineKeyboardButton(text="✏️ Своё количество", callback_data=f"qin:{pid}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="cat:back")],
    ])


def added_to_cart_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить ещё", callback_data="cat:back")],
        [
            InlineKeyboardButton(text="🛒 Перейти в корзину", callback_data="cart:show"),
        ],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main")],
    ])


def cart_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Оформить заказ", callback_data="checkout")],
        [InlineKeyboardButton(text="➕ Добавить ещё", callback_data="cat:back")],
        [
            InlineKeyboardButton(text="🗑 Очистить", callback_data="cart:clear"),
            InlineKeyboardButton(text="◀️ Меню", callback_data="menu:main"),
        ],
    ])


def cart_empty_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧊 Перейти в каталог", callback_data="cat:back")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main")],
    ])


def payment_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Безналичная оплата", callback_data="pay:card")],
        [InlineKeyboardButton(text="💵 Наличный расчёт", callback_data="pay:cash")],
        [InlineKeyboardButton(text="📲 Перевод", callback_data="pay:transfer")],
    ])


def confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить заказ", callback_data="order:confirm")],
        [InlineKeyboardButton(text="✏️ Изменить", callback_data="order:edit")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="order:cancel")],
    ])


def orders_history_kb(orders) -> InlineKeyboardMarkup:
    btns = []
    for o in orders:
        items = json.loads(o[3])
        short = ", ".join(f"{i['name']}×{i['qty']}" for i in items)
        if len(short) > 38:
            short = short[:35] + "..."
        btns.append([InlineKeyboardButton(
            text=f"#{o[0]} · {o[4][:10]} · {o[2]:.0f} ₽",
            callback_data=f"hist:{o[0]}",
        )])
    btns.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def order_detail_kb(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Повторить этот заказ", callback_data=f"rep:{order_id}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del:{order_id}")],
        [InlineKeyboardButton(text="◀️ К списку заказов", callback_data="menu:my_orders")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main")],
    ])


def repeat_order_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Повторить заказ", callback_data="menu:repeat")],
    ])


def notify_order_kb(oid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Доставлен", callback_data=f"adm:setstatus:{oid}:delivered"),
            InlineKeyboardButton(text="❌ Отменить", callback_data=f"adm:setstatus:{oid}:cancelled"),
        ],
    ])


def back_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main")],
    ])


# ═══════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════

async def init_db():
    config.DB = await aiosqlite.connect(DB_PATH)
    config.DB.row_factory = aiosqlite.Row
    await config.DB.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
            phone TEXT, address TEXT,
            created_at TEXT DEFAULT (datetime('now')), last_order_at TEXT
        );
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL, subcategory TEXT,
            name TEXT NOT NULL, unit TEXT NOT NULL,
            base_price REAL NOT NULL, sort_order INTEGER DEFAULT 0, active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS individual_prices (
            user_id INTEGER, product_id INTEGER, price REAL,
            PRIMARY KEY (user_id, product_id)
        );
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, status TEXT DEFAULT 'new',
            delivery_date TEXT, delivery_time TEXT, address TEXT, phone TEXT,
            payment_method TEXT, total REAL DEFAULT 0,
            items_json TEXT, created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
    """)
    await _seed()
    await config.DB.commit()


async def _seed():
    r = await config.DB.execute_fetchall("SELECT COUNT(*) FROM products")
    if r[0][0] > 0:
        return
    data = [
        ("wild", "cubes",   "Куб 5×5 см",          "шт", 0,   1),
        ("wild", "cubes",   "Куб 6×5 см",          "шт", 0,   2),
        ("wild", "cubes",   "Куб 5.5×5.5 см",      "шт", 0,   3),
        ("wild", "cubes",   "Куб 4×4 см",          "шт", 0,   4),
        ("wild", "cubes",   "Куб 4.5×4.5 см",      "шт", 0,   5),
        ("wild", "bars",    "Брусок 12×4 см",       "шт", 0,  10),
        ("wild", "bars",    "Брусок 8×4 см",        "шт", 0,  11),
        ("wild", "special", "Свободный распил",     "кг", 170, 20),
        ("wild", "special", "Куб с фото внутри",    "шт", 0,  21),
        ("wild", "special", "Куб с цветком внутри", "шт", 0,  22),
        ("weight", None,    "Лёд гурме",            "кг", 0,  30),
        ("weight", None,    "Лёд дайс",             "кг", 0,  31),
        ("weight", None,    "Лёд хошизаки стандарт","кг", 0,  32),
        ("weight", None,    "Лёд хошизаки 5×5",    "кг", 0,  33),
        ("weight", None,    "Лёд подушечка",        "кг", 0,  34),
        ("weight", None,    "Лёд фраппе",           "кг", 0,  35),
    ]
    await config.DB.executemany(
        "INSERT INTO products(category,subcategory,name,unit,base_price,sort_order) VALUES(?,?,?,?,?,?)",
        data,
    )


async def get_price(uid: int, pid: int) -> float:
    r = await config.DB.execute_fetchall(
        "SELECT price FROM individual_prices WHERE user_id=? AND product_id=?", (uid, pid)
    )
    if r:
        return r[0][0]
    r2 = await config.DB.execute_fetchall("SELECT base_price FROM products WHERE id=?", (pid,))
    return r2[0][0]


# ═══════════════════════════════════════════════════════
#  MIDDLEWARE
# ═══════════════════════════════════════════════════════

async def send_main_menu(chat_id: int, bot: Bot, state: FSMContext = None):
    if state:
        await state.set_state(None)
    photo = FSInputFile("Photo/img.jpg")
    await bot.send_photo(
        chat_id=chat_id,
        photo=photo,
        caption=WELCOME_TEXT,
        reply_markup=main_menu_kb(),
        parse_mode="HTML"
    )


class AuthMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable, event: TelegramObject, data: Dict[str, Any]) -> Any:
        if isinstance(event, Message):
            if event.text and event.text.startswith("/"):
                return await handler(event, data)
            if event.contact:
                return await handler(event, data)
        uid = event.from_user.id if hasattr(event, "from_user") else None
        if uid:
            u = await config.DB.execute_fetchall("SELECT phone FROM users WHERE tg_id=?", (uid,))
            if not u or not u[0][0]:
                if isinstance(event, Message):
                    await event.answer(
                        "Для работы с ботом сначала поделитесь номером телефона — /start",
                        reply_markup=PHONE_KB,
                    )
                elif isinstance(event, CallbackQuery):
                    await event.answer("Сначала пройдите регистрацию: /start", show_alert=True)
                return
        return await handler(event, data)


# ═══════════════════════════════════════════════════════
#  STATES
# ═══════════════════════════════════════════════════════

class Auth(StatesGroup):
    phone = State()


class Catalog(StatesGroup):
    qty_input = State()
    custom_text = State()


class Checkout(StatesGroup):
    date = State()
    time = State()
    address = State()
    payment = State()
    confirm = State()


# ═══════════════════════════════════════════════════════
#  РОУТЕР
# ═══════════════════════════════════════════════════════

rt = Router()


@rt.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()


# ── Авторизация ──

@rt.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext, bot: Bot):
    await state.clear()
    u = await config.DB.execute_fetchall("SELECT phone FROM users WHERE tg_id=?", (msg.from_user.id,))
    if u and u[0][0]:
        await send_main_menu(msg.from_user.id, bot, state)
        try:
            await msg.delete()
        except Exception:
            pass
        return
    await config.DB.execute("""
        INSERT INTO users(tg_id, username, full_name) VALUES(?,?,?)
        ON CONFLICT(tg_id) DO UPDATE SET username=excluded.username, full_name=excluded.full_name
    """, (msg.from_user.id, msg.from_user.username, msg.from_user.full_name))
    await config.DB.commit()
    await state.set_state(Auth.phone)
    await msg.answer(
        "👋 Добро пожаловать!\n\n"
        "Для оформления заказов нам нужен ваш номер телефона.\n"
        "Нажмите кнопку ниже:",
        reply_markup=PHONE_KB,
    )


@rt.message(Auth.phone, F.contact)
async def on_contact(msg: Message, state: FSMContext):
    phone = msg.contact.phone_number
    await config.DB.execute("UPDATE users SET phone=? WHERE tg_id=?", (phone, msg.from_user.id))
    await config.DB.commit()
    await state.clear()
    await msg.answer(f"✅ Номер сохранён!", reply_markup=ReplyKeyboardRemove())
    await msg.answer(WELCOME_TEXT, reply_markup=main_menu_kb(), parse_mode="HTML")


@rt.message(Auth.phone)
async def on_phone_bad(msg: Message):
    await msg.answer("Пожалуйста, нажмите кнопку «📱 Поделиться номером».", reply_markup=PHONE_KB)


# ── Главное меню ──

@rt.callback_query(F.data == "menu:main")
async def cb_main_menu(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(None)
    try:
        await cb.message.delete()
    except Exception:
        pass
    await send_main_menu(cb.from_user.id, bot, state)
    await cb.answer()


@rt.callback_query(F.data == "menu:order")
async def cb_menu_order(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.update_data(cart=d.get("cart", []))
    await safe_edit(cb, CATALOG_TEXT, reply_markup=catalog_kb(), parse_mode="HTML")
    await cb.answer()


# ── Мои заказы ──

@rt.callback_query(F.data == "menu:my_orders")
async def cb_menu_my_orders(cb: CallbackQuery):
    rows = await config.DB.execute_fetchall(
        "SELECT id, status, total, items_json, created_at FROM orders "
        "WHERE user_id=? ORDER BY id DESC LIMIT 10",
        (cb.from_user.id,),
    )
    if not rows:
        await safe_edit(
            cb,
            "📦 У вас ещё нет заказов.\n\nОформите первый — это просто!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🧊 Заказать лёд", callback_data="menu:order")],
                [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main")],
            ]),
        )
        await cb.answer()
        return
    await safe_edit(
        cb,
        "📦 <b>Ваши заказы:</b>\n\nВыберите заказ для просмотра деталей:",
        reply_markup=orders_history_kb(rows),
        parse_mode="HTML",
    )
    await cb.answer()


@rt.callback_query(F.data.startswith("hist:"))
async def cb_hist_detail(cb: CallbackQuery):
    oid = int(cb.data.split(":")[1])
    row = await config.DB.execute_fetchall(
        "SELECT id, status, total, items_json, created_at, delivery_date, delivery_time, address, payment_method "
        "FROM orders WHERE id=? AND user_id=?",
        (oid, cb.from_user.id),
    )
    if not row:
        await cb.answer("Заказ не найден.", show_alert=True)
        return
    o = row[0]
    items = json.loads(o[3])
    lines = [f"  • {i['name']} × {i['qty']} {i['unit']} = {i['price'] * i['qty']:.0f} ₽" for i in items]
    pay = PAYMENT_LABELS.get(o[8], o[8])
    text = (
        f"📋 <b>Заказ #{o[0]}</b>\n"
        f"📅 {o[4][:10]}\n"
        f"🚚 Доставка: {o[5] or '—'} {o[6] or ''}\n"
        f"📍 {o[7] or '—'}\n"
        f"💳 {pay}\n"
        f"📌 {STATUS_LABELS.get(o[1], o[1])}\n\n"
        + "\n".join(lines)
        + f"\n\n💰 <b>Итого: {o[2]:.0f} ₽</b>"
    )
    await cb.message.edit_text(text, reply_markup=order_detail_kb(oid), parse_mode="HTML")
    await cb.answer()


@rt.callback_query(F.data.startswith("rep:"))
async def cb_rep_order(cb: CallbackQuery, state: FSMContext):
    oid = int(cb.data.split(":")[1])
    row = await config.DB.execute_fetchall(
        "SELECT items_json FROM orders WHERE id=? AND user_id=?", (oid, cb.from_user.id)
    )
    if not row:
        await cb.answer("Заказ не найден.", show_alert=True)
        return
    items = json.loads(row[0][0])
    await state.update_data(cart=items)
    await state.set_state(Checkout.date)
    await cb.message.edit_text(
        "🔁 Товары добавлены в корзину!\n\n"
        "📅 Укажите дату доставки (ДД.ММ.ГГГГ):"
    )
    await cb.answer()


@rt.callback_query(F.data.startswith("del:"))
async def cb_del_order(cb: CallbackQuery):
    oid = int(cb.data.split(":")[1])
    await config.DB.execute("DELETE FROM orders WHERE id=? AND user_id=?", (oid, cb.from_user.id))
    await config.DB.commit()
    rows = await config.DB.execute_fetchall(
        "SELECT id, status, total, items_json, created_at FROM orders "
        "WHERE user_id=? ORDER BY id DESC LIMIT 10",
        (cb.from_user.id,),
    )
    if not rows:
        await cb.message.edit_text("🗑 Заказ удалён. Заказов больше нет.", reply_markup=back_menu_kb())
    else:
        await cb.message.edit_text(
            "🗑 Заказ удалён.\n\n📦 <b>Ваши заказы:</b>",
            reply_markup=orders_history_kb(rows),
            parse_mode="HTML",
        )
    await cb.answer()


# ── Повторить заказ ──

@rt.callback_query(F.data == "menu:repeat")
async def cb_menu_repeat(cb: CallbackQuery, state: FSMContext):
    rows = await config.DB.execute_fetchall(
        "SELECT id, status, total, items_json, created_at FROM orders "
        "WHERE user_id=? ORDER BY id DESC LIMIT 10",
        (cb.from_user.id,),
    )
    if not rows:
        await safe_edit(
            cb,
            "📦 Предыдущих заказов нет.\n\nОформите первый заказ!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🧊 Заказать лёд", callback_data="menu:order")],
                [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main")],
            ]),
        )
        await cb.answer()
        return
    await safe_edit(
        cb,
        "🔁 <b>Выберите заказ для повтора:</b>",
        reply_markup=orders_history_kb(rows),
        parse_mode="HTML",
    )
    await cb.answer()


# ── Контакты ──

@rt.callback_query(F.data == "menu:contact")
async def cb_menu_contact(cb: CallbackQuery):
    await safe_edit(cb, CONTACT_TEXT, reply_markup=back_menu_kb(), parse_mode="HTML")
    await cb.answer()


# ── Каталог ──

@rt.callback_query(F.data == "cat:back")
async def cb_back_cat(cb: CallbackQuery):
    await cb.message.edit_text(CATALOG_TEXT, reply_markup=catalog_kb(), parse_mode="HTML")
    await cb.answer()


@rt.callback_query(F.data == "cat:wild")
async def cb_wild(cb: CallbackQuery):
    await cb.message.edit_text(
        "🧊 <b>Дикий лёд</b>\n\nВыберите тип:",
        reply_markup=wild_sub_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


@rt.callback_query(F.data == "cat:weight")
async def cb_weight(cb: CallbackQuery):
    rows = await config.DB.execute_fetchall(
        "SELECT id, name, base_price, unit FROM products "
        "WHERE category='weight' AND active=1 ORDER BY sort_order"
    )
    prods = []
    for r in rows:
        p = await get_price(cb.from_user.id, r[0])
        prods.append({"id": r[0], "name": r[1], "price": p, "unit": r[3]})
    await cb.message.edit_text(
        "⚖️ <b>Лёд на развес</b>\n\nЦена за кг:",
        reply_markup=product_list_kb(prods, "cat:back", page_cb_prefix="catpg:weight"),
        parse_mode="HTML",
    )
    await cb.answer()


@rt.callback_query(F.data.startswith("catpg:weight:"))
async def cb_weight_page(cb: CallbackQuery):
    page = int(cb.data.split(":")[2])
    rows = await config.DB.execute_fetchall(
        "SELECT id, name, base_price, unit FROM products "
        "WHERE category='weight' AND active=1 ORDER BY sort_order"
    )
    prods = []
    for r in rows:
        p = await get_price(cb.from_user.id, r[0])
        prods.append({"id": r[0], "name": r[1], "price": p, "unit": r[3]})
    await cb.message.edit_text(
        "⚖️ <b>Лёд на развес</b>\n\nЦена за кг:",
        reply_markup=product_list_kb(prods, "cat:back", page=page, page_cb_prefix="catpg:weight"),
        parse_mode="HTML",
    )
    await cb.answer()


@rt.callback_query(F.data.startswith("sub:"))
async def cb_sub(cb: CallbackQuery):
    sc = cb.data.split(":")[1]
    labels = {"cubes": "🟦 Кубы", "bars": "🔷 Бруски", "special": "✨ Особые формы"}
    rows = await config.DB.execute_fetchall(
        "SELECT id, name, base_price, unit FROM products "
        "WHERE category='wild' AND subcategory=? AND active=1 ORDER BY sort_order",
        (sc,),
    )
    prods = []
    for r in rows:
        p = await get_price(cb.from_user.id, r[0])
        prods.append({"id": r[0], "name": r[1], "price": p, "unit": r[3]})
    await cb.message.edit_text(
        f"🧊 <b>{labels.get(sc, sc)}</b>\n\nВыберите позицию:",
        reply_markup=product_list_kb(prods, "cat:wild", page_cb_prefix=f"catpg:{sc}"),
        parse_mode="HTML",
    )
    await cb.answer()


@rt.callback_query(F.data.startswith("catpg:"))
async def cb_cat_page(cb: CallbackQuery):
    parts = cb.data.split(":")
    sc = parts[1]
    page = int(parts[2])
    if sc == "weight":
        return  # handled by catpg:weight handler above
    labels = {"cubes": "🟦 Кубы", "bars": "🔷 Бруски", "special": "✨ Особые формы"}
    rows = await config.DB.execute_fetchall(
        "SELECT id, name, base_price, unit FROM products "
        "WHERE category='wild' AND subcategory=? AND active=1 ORDER BY sort_order",
        (sc,),
    )
    prods = []
    for r in rows:
        p = await get_price(cb.from_user.id, r[0])
        prods.append({"id": r[0], "name": r[1], "price": p, "unit": r[3]})
    await cb.message.edit_text(
        f"🧊 <b>{labels.get(sc, sc)}</b>\n\nВыберите позицию:",
        reply_markup=product_list_kb(prods, "cat:wild", page=page, page_cb_prefix=f"catpg:{sc}"),
        parse_mode="HTML",
    )
    await cb.answer()


# ── Товар → количество ──

@rt.callback_query(F.data.startswith("prod:"))
async def cb_prod(cb: CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    r = await config.DB.execute_fetchall("SELECT name, unit FROM products WHERE id=?", (pid,))
    price = await get_price(cb.from_user.id, pid)
    await state.update_data(sel_pid=pid)
    price_str = f"{price} ₽/{r[0][1]}" if price > 0 else "цена по запросу"
    await cb.message.edit_text(
        f"📦 <b>{r[0][0]}</b>\n💰 {price_str}\n\nВыберите количество:",
        reply_markup=qty_kb(pid),
        parse_mode="HTML",
    )
    await cb.answer()


@rt.callback_query(F.data.startswith("qty:"))
async def cb_qty(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":")
    pid, q = int(parts[1]), float(parts[2])
    await _add_cart(cb, state, pid, q)


@rt.callback_query(F.data.startswith("qin:"))
async def cb_qin(cb: CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    r = await config.DB.execute_fetchall("SELECT name, unit FROM products WHERE id=?", (pid,))
    await state.update_data(sel_pid=pid)
    if r[0][0] == "Свободный распил":
        await state.set_state(Catalog.custom_text)
        await cb.message.edit_text(
            "✏️ Опишите пожелания по распилу и укажите количество (кг):\n\n"
            "<i>Например: 10 кг, продольный распил по 5 см</i>",
            parse_mode="HTML",
        )
    else:
        await state.set_state(Catalog.qty_input)
        await cb.message.edit_text(f"✏️ Введите количество ({r[0][1]}):")
    await cb.answer()


@rt.message(Catalog.qty_input)
async def on_qty(msg: Message, state: FSMContext):
    try:
        q = float(msg.text.replace(",", "."))
        assert q > 0
    except (ValueError, AssertionError):
        await msg.answer("Введите число больше 0:")
        return
    d = await state.get_data()
    await state.set_state(None)
    await _add_cart_msg(msg, state, d["sel_pid"], q)


@rt.message(Catalog.custom_text)
async def on_custom(msg: Message, state: FSMContext):
    d = await state.get_data()
    nums = re.findall(r"[\d]+[.,]?[\d]*", msg.text)
    q = float(nums[-1].replace(",", ".")) if nums else 1.0
    await state.set_state(None)
    await _add_cart_msg(msg, state, d["sel_pid"], q, note=msg.text)


async def _add_cart(cb: CallbackQuery, state: FSMContext, pid: int, q: float, note: str = ""):
    r = await config.DB.execute_fetchall("SELECT name, unit FROM products WHERE id=?", (pid,))
    price = await get_price(cb.from_user.id, pid)
    d = await state.get_data()
    cart = d.get("cart", [])
    cart.append({"product_id": pid, "name": r[0][0], "unit": r[0][1], "price": price, "qty": q, "note": note})
    await state.update_data(cart=cart)
    total = sum(i["price"] * i["qty"] for i in cart)
    items_count = len(cart)
    await cb.message.edit_text(
        f"✅ <b>{r[0][0]}</b> × {q} {r[0][1]} добавлен в корзину\n\n"
        f"🛒 В корзине: {items_count} поз. на <b>{total:.0f} ₽</b>",
        reply_markup=added_to_cart_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


async def _add_cart_msg(msg: Message, state: FSMContext, pid: int, q: float, note: str = ""):
    r = await config.DB.execute_fetchall("SELECT name, unit FROM products WHERE id=?", (pid,))
    price = await get_price(msg.from_user.id, pid)
    d = await state.get_data()
    cart = d.get("cart", [])
    cart.append({"product_id": pid, "name": r[0][0], "unit": r[0][1], "price": price, "qty": q, "note": note})
    await state.update_data(cart=cart)
    total = sum(i["price"] * i["qty"] for i in cart)
    items_count = len(cart)
    await msg.answer(
        f"✅ <b>{r[0][0]}</b> × {q} {r[0][1]} добавлен в корзину\n\n"
        f"🛒 В корзине: {items_count} поз. на <b>{total:.0f} ₽</b>",
        reply_markup=added_to_cart_kb(),
        parse_mode="HTML",
    )


# ── Корзина ──

@rt.callback_query(F.data == "cart:show")
async def cb_cart(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    cart = d.get("cart", [])
    if not cart:
        await safe_edit(
            cb,
            "🛒 Корзина пуста.\n\nДобавьте товары из каталога!",
            reply_markup=cart_empty_kb(),
        )
        await cb.answer()
        return
    lines, total = [], 0
    for idx, it in enumerate(cart, 1):
        s = it["price"] * it["qty"]
        total += s
        lines.append(f"{idx}. {it['name']} × {it['qty']} {it['unit']} = <b>{s:.0f} ₽</b>")
        if it.get("note"):
            lines.append(f"   📝 <i>{it['note']}</i>")
    await safe_edit(
        cb,
        "🛒 <b>Ваша корзина:</b>\n\n"
        + "\n".join(lines)
        + f"\n\n💰 <b>Итого: {total:.0f} ₽</b>",
        reply_markup=cart_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


@rt.callback_query(F.data == "cart:clear")
async def cb_cart_clear(cb: CallbackQuery, state: FSMContext):
    await state.update_data(cart=[])
    await cb.message.edit_text(
        "🗑 Корзина очищена.\n\nВыберите товары в каталоге:",
        reply_markup=cart_empty_kb(),
    )
    await cb.answer()


# ── Оформление заказа ──

@rt.callback_query(F.data == "checkout")
async def cb_checkout(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    cart = d.get("cart", [])
    if not cart:
        await cb.answer("Корзина пуста!", show_alert=True)
        return
    total = sum(i["price"] * i["qty"] for i in cart)
    if config.MIN_ORDER_AMOUNT and total < config.MIN_ORDER_AMOUNT:
        await cb.answer(
            f"Минимальный заказ {config.MIN_ORDER_AMOUNT} ₽\nВ корзине: {total:.0f} ₽",
            show_alert=True,
        )
        return
    await state.set_state(Checkout.date)
    await cb.message.edit_text(
        "📋 <b>Оформление заказа</b>\n\n"
        "📅 Шаг 1/4 — Укажите дату доставки:\n"
        "<i>Формат: ДД.ММ.ГГГГ (например, 25.01.2025)</i>",
        parse_mode="HTML",
    )
    await cb.answer()


@rt.message(Checkout.date)
async def on_date(msg: Message, state: FSMContext):
    await state.update_data(d_date=msg.text.strip())
    await state.set_state(Checkout.time)
    await msg.answer(
        "🕐 Шаг 2/4 — Укажите удобное время доставки:\n"
        "<i>Например: 10:00–12:00 или после 14:00</i>",
        parse_mode="HTML",
    )


@rt.message(Checkout.time)
async def on_time(msg: Message, state: FSMContext):
    await state.update_data(d_time=msg.text.strip())
    u = await config.DB.execute_fetchall("SELECT address FROM users WHERE tg_id=?", (msg.from_user.id,))
    addr = u[0][0] if u and u[0][0] else None
    await state.set_state(Checkout.address)
    if addr:
        await msg.answer(
            f"📍 Шаг 3/4 — Адрес доставки\n\n"
            f"Последний адрес: <b>{addr}</b>\n\n"
            f"Отправьте его же или введите новый:",
            parse_mode="HTML",
        )
    else:
        await msg.answer(
            "📍 Шаг 3/4 — Введите адрес доставки:\n"
            "<i>Укажите улицу, дом и при необходимости квартиру</i>",
            parse_mode="HTML",
        )


@rt.message(Checkout.address)
async def on_addr(msg: Message, state: FSMContext):
    await state.update_data(d_addr=msg.text.strip())
    u = await config.DB.execute_fetchall("SELECT phone FROM users WHERE tg_id=?", (msg.from_user.id,))
    phone = u[0][0] if u else ""
    await state.update_data(d_phone=phone)
    await state.set_state(Checkout.payment)
    await msg.answer(
        f"💳 Шаг 4/4 — Выберите способ оплаты:\n"
        f"<i>📱 Ваш номер для связи: {phone}</i>",
        reply_markup=payment_kb(),
        parse_mode="HTML",
    )


@rt.callback_query(F.data.startswith("pay:"))
async def on_pay(cb: CallbackQuery, state: FSMContext):
    method = cb.data.split(":")[1]
    await state.update_data(d_pay=method)
    d = await state.get_data()
    cart = d.get("cart", [])
    if not cart:
        await cb.answer("Ошибка: корзина пуста", show_alert=True)
        return
    total = sum(i["price"] * i["qty"] for i in cart)
    lines = []
    for it in cart:
        s = it["price"] * it["qty"]
        lines.append(f"  • {it['name']} × {it['qty']} {it['unit']} = {s:.0f} ₽")
        if it.get("note"):
            lines.append(f"    📝 {it['note']}")
    summary = (
        "📋 <b>Подтвердите заказ</b>\n\n"
        + "\n".join(lines)
        + f"\n\n💰 <b>Итого: {total:.0f} ₽</b>\n\n"
        f"📅 {d['d_date']}  🕐 {d['d_time']}\n"
        f"📍 {d['d_addr']}\n"
        f"📱 {d['d_phone']}\n"
        f"💳 {PAYMENT_LABELS.get(method, method)}"
    )
    await state.set_state(Checkout.confirm)
    await cb.message.edit_text(summary, reply_markup=confirm_kb(), parse_mode="HTML")
    await cb.answer()


@rt.callback_query(F.data == "order:edit")
async def on_order_edit(cb: CallbackQuery, state: FSMContext):
    await state.set_state(None)
    d = await state.get_data()
    cart = d.get("cart", [])
    if not cart:
        await cb.message.edit_text("🛒 Корзина пуста.", reply_markup=cart_empty_kb())
        await cb.answer()
        return
    lines, total = [], 0
    for idx, it in enumerate(cart, 1):
        s = it["price"] * it["qty"]
        total += s
        lines.append(f"{idx}. {it['name']} × {it['qty']} {it['unit']} = <b>{s:.0f} ₽</b>")
    await cb.message.edit_text(
        "🛒 <b>Корзина:</b>\n\n" + "\n".join(lines) + f"\n\n💰 <b>Итого: {total:.0f} ₽</b>",
        reply_markup=cart_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


@rt.callback_query(F.data == "order:confirm")
async def on_confirm(cb: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    cart = d.get("cart", [])
    if not cart:
        await cb.answer("Ошибка", show_alert=True)
        return
    total = sum(i["price"] * i["qty"] for i in cart)

    cursor = await config.DB.execute(
        "INSERT INTO orders(user_id, items_json, total, delivery_date, delivery_time, address, phone, payment_method) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (
            cb.from_user.id,
            json.dumps(cart, ensure_ascii=False),
            total,
            d["d_date"], d["d_time"], d["d_addr"], d["d_phone"], d["d_pay"],
        ),
    )
    oid = cursor.lastrowid
    await config.DB.execute(
        "UPDATE users SET last_order_at=datetime('now'), address=? WHERE tg_id=?",
        (d["d_addr"], cb.from_user.id),
    )
    await config.DB.commit()

    await state.update_data(cart=[])
    await state.set_state(None)

    await cb.message.edit_text(
        f"🎉 <b>Заказ #{oid} оформлен!</b>\n\n"
        f"Мы получили ваш заказ и скоро свяжемся для подтверждения.\n\n"
        f"📅 {d['d_date']}  🕐 {d['d_time']}\n"
        f"📍 {d['d_addr']}\n\n"
        f"Спасибо, что выбираете нас! 🧊",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 Мои заказы", callback_data="menu:my_orders")],
            [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main")],
        ]),
        parse_mode="HTML",
    )
    await cb.answer()

    u = await config.DB.execute_fetchall(
        "SELECT full_name, phone, username FROM users WHERE tg_id=?", (cb.from_user.id,)
    )
    lines = [
        f"  • {i['name']} × {i['qty']} {i['unit']} = {i['price'] * i['qty']:.0f} ₽"
        for i in cart
    ]
    notify = (
        f"🆕 Заказ #{oid}\n\n"
        f"👤 {u[0][0]}\n📱 {u[0][1]}\n🔗 @{u[0][2] or '—'}\n"
        f"📍 {d['d_addr']}\n📅 {d['d_date']} {d['d_time']}\n"
        f"💳 {PAYMENT_LABELS.get(d['d_pay'], d['d_pay'])}\n\n"
        + "\n".join(lines)
        + f"\n\n💰 Итого: {total:.0f} ₽"
    )
    for cid in NOTIFY_IDS:
        try:
            await cb.bot.send_message(cid, notify, reply_markup=notify_order_kb(oid))
        except Exception:
            pass


@rt.callback_query(F.data == "order:cancel")
async def on_order_cancel(cb: CallbackQuery, state: FSMContext):
    await state.update_data(cart=[])
    await state.set_state(None)
    await cb.message.edit_text(
        "❌ Заказ отменён.\n\nЕсли передумаете — мы всегда здесь! 🧊",
        reply_markup=back_menu_kb(),
    )
    await cb.answer()


# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════

async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(AuthMiddleware())
    dp.callback_query.middleware(AuthMiddleware())

    from admin import router as admin_router, run_scheduler
    dp.include_router(rt)
    dp.include_router(admin_router)

    await init_db()

    r = await config.DB.execute_fetchall("SELECT value FROM settings WHERE key='min_order_amount'")
    if r:
        config.MIN_ORDER_AMOUNT = int(r[0][0])

    logger.info("Bot starting...")
    asyncio.create_task(run_scheduler(bot))

    try:
        await dp.start_polling(bot)
    finally:
        if config.DB:
            await config.DB.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())