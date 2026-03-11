import json
import asyncio
import logging
import html
from datetime import datetime

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command

from config import ADMIN_IDS, NOTIFY_IDS, INACTIVE_DAYS
import config

logger = logging.getLogger(__name__)
router = Router()

STATUS_LABELS = {
    "new": "🆕 Новый",
    "in_progress": "🔄 В работе",
    "delivered": "✅ Доставлен",
    "cancelled": "❌ Отменён",
}
PAYMENT_LABELS = {
    "card": "💳 Безналичная",
    "cash": "💵 Наличные",
    "transfer": "📲 Перевод",
}
SUBCAT_LABELS = {
    "cubes": "🟦 Кубы",
    "bars": "🔷 Бруски",
    "special": "✨ Особые формы",
}

PER_PAGE = 6


# ═══════════════════════════════════════════════════════
#  КЛАВИАТУРЫ — ПАНЕЛЬ
# ═══════════════════════════════════════════════════════

def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Заказы", callback_data="adm:orders"),
            InlineKeyboardButton(text="📊 Отчёт", callback_data="adm:report"),
        ],
        [
            InlineKeyboardButton(text="📦 Товары", callback_data="adm:products"),
            InlineKeyboardButton(text="📢 Рассылка", callback_data="adm:broadcast"),
        ],
        [
            InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main"),
        ],
    ])


def admin_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="adm:back")],
    ])


def report_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Сбросить статистику", callback_data="adm:report:reset_confirm")],
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="adm:back")],
    ])


def report_reset_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, сбросить всё", callback_data="adm:report:reset_do"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="adm:report"),
        ],
    ])


# ═══════════════════════════════════════════════════════
#  КЛАВИАТУРЫ — ЗАКАЗЫ
# ═══════════════════════════════════════════════════════

def orders_tabs_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 В работе", callback_data="adm:orders:active"),
            InlineKeyboardButton(text="✅ Выполненные", callback_data="adm:orders:done"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")],
    ])


def clients_list_kb(clients: list, tab: str) -> InlineKeyboardMarkup:
    """Shared keyboard for both active and done client lists."""
    btns = []
    for uid, phone, name, cnt, total in clients:
        label_name = phone or name or str(uid)
        if tab == "active":
            label = f"{label_name} | {cnt} акт."
            cb = f"adm:cli:{uid}:active"
        else:
            label = f"{label_name} | {cnt} зак. | {total:.0f} ₽"
            cb = f"adm:cli:{uid}:done"
        btns.append([InlineKeyboardButton(text=label, callback_data=cb)])
    btns.append([InlineKeyboardButton(text="◀️ Заказы", callback_data="adm:orders")])
    btns.append([InlineKeyboardButton(text="◀️ Панель", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def client_orders_kb(orders: list, uid: int, tab: str) -> InlineKeyboardMarkup:
    """Order list for a specific client (active or done tab)."""
    btns = []
    for o in orders:
        st = STATUS_LABELS.get(o["status"], o["status"])
        btns.append([InlineKeyboardButton(
            text=f"#{o['id']} | {o['date']} | {o['total']:.0f} ₽ | {st}",
            callback_data=f"adm:odetail:{o['id']}:{tab}:{uid}",
        )])
    btns.append([InlineKeyboardButton(
        text="◀️ К списку клиентов",
        callback_data=f"adm:orders:{tab}",
    )])
    btns.append([
        InlineKeyboardButton(text="◀️ Заказы", callback_data="adm:orders"),
        InlineKeyboardButton(text="◀️ Панель", callback_data="adm:back"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def order_detail_kb(oid: int, uid: int, tab: str) -> InlineKeyboardMarkup:
    """Detail view for one order — active gets status buttons, done does not."""
    btns = []
    if tab == "active":
        btns.append([
            InlineKeyboardButton(text="✅ Доставлен", callback_data=f"adm:setstatus:{oid}:delivered:{uid}:{tab}"),
            InlineKeyboardButton(text="🔄 В работе", callback_data=f"adm:setstatus:{oid}:in_progress:{uid}:{tab}"),
        ])
        btns.append([
            InlineKeyboardButton(text="❌ Отменить", callback_data=f"adm:setstatus:{oid}:cancelled:{uid}:{tab}"),
        ])
    btns.append([InlineKeyboardButton(
        text="◀️ К заказам клиента",
        callback_data=f"adm:cli:{uid}:{tab}",
    )])
    btns.append([
        InlineKeyboardButton(text="◀️ Заказы", callback_data="adm:orders"),
        InlineKeyboardButton(text="◀️ Панель", callback_data="adm:back"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def notify_order_kb(oid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Доставлен", callback_data=f"adm:setstatus:{oid}:delivered:0:active"),
            InlineKeyboardButton(text="❌ Отменить", callback_data=f"adm:setstatus:{oid}:cancelled:0:active"),
        ],
    ])


def order_status_kb(oid: int) -> InlineKeyboardMarkup:
    """For NOTIFY_IDS messages."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🆕", callback_data=f"status:{oid}:new"),
            InlineKeyboardButton(text="🔄", callback_data=f"status:{oid}:in_progress"),
        ],
        [
            InlineKeyboardButton(text="✅", callback_data=f"status:{oid}:delivered"),
            InlineKeyboardButton(text="❌", callback_data=f"status:{oid}:cancelled"),
        ],
    ])


def repeat_order_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Повторить заказ", callback_data="menu:repeat")],
    ])


# ═══════════════════════════════════════════════════════
#  КЛАВИАТУРЫ — ТОВАРЫ (общая пагинация)
# ═══════════════════════════════════════════════════════

def products_categories_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧊 Дикий лёд", callback_data="adm:prod:cat:wild")],
        [InlineKeyboardButton(text="⚖️ Лёд на развес", callback_data="adm:prod:cat:weight")],
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="adm:back")],
    ])


def wild_subcats_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟦 Кубы", callback_data="adm:prod:sub:cubes")],
        [InlineKeyboardButton(text="🔷 Бруски", callback_data="adm:prod:sub:bars")],
        [InlineKeyboardButton(text="✨ Особые формы", callback_data="adm:prod:sub:special")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:products")],
    ])


def _paginate_products_adm(products: list, page: int, back_cb: str, add_cb: str) -> InlineKeyboardMarkup:
    """Paginated product list for admin."""
    total_pages = max(1, (len(products) + PER_PAGE - 1) // PER_PAGE)
    chunk = products[page * PER_PAGE:(page + 1) * PER_PAGE]
    btns = []
    for p in chunk:
        price_s = f"{p['price']} ₽/{p['unit']}" if p["price"] > 0 else "по запросу"
        btns.append([InlineKeyboardButton(
            text=f"{p['name']} — {price_s}",
            callback_data=f"adm:prod:detail:{p['id']}",
        )])
    # Pagination nav
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"adm:prod:pg:{page-1}:{add_cb}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"adm:prod:pg:{page+1}:{add_cb}"))
        btns.append(nav)
    btns.append([InlineKeyboardButton(text="➕ Добавить товар", callback_data=add_cb)])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def product_detail_adm_kb(pid: int, back_cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить цену", callback_data=f"adm:prod:price:{pid}")],
        [InlineKeyboardButton(text="👤 Инд. цена для клиента", callback_data=f"indp:{pid}")],
        [InlineKeyboardButton(text="❌ Удалить товар", callback_data=f"adm:prod:del:{pid}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)],
    ])


def unit_select_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="шт", callback_data="adm:prod:unit:шт"),
            InlineKeyboardButton(text="кг", callback_data="adm:prod:unit:кг"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:products")],
    ])


def users_page_kb(users: list, pid: int, page: int) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(users) + PER_PAGE - 1) // PER_PAGE)
    chunk = users[page * PER_PAGE:(page + 1) * PER_PAGE]
    btns = []
    for u in chunk:
        name = u[2] or "—"
        btns.append([InlineKeyboardButton(
            text=f"{name} | {u[3] or '—'}",
            callback_data=f"indpu:{pid}:{u[0]}",
        )])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"upage:{pid}:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"upage:{pid}:{page + 1}"))
    if nav:
        btns.append(nav)
    btns.append([InlineKeyboardButton(text="◀️ К товару", callback_data=f"adm:prod:detail:{pid}")])
    btns.append([InlineKeyboardButton(text="◀️ Панель", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


# ═══════════════════════════════════════════════════════
#  КЛАВИАТУРА — ПАГИНАЦИЯ ТОВАРОВ ДЛЯ ПОЛЬЗОВАТЕЛЯ
# ═══════════════════════════════════════════════════════

def paginate_products_user(products: list, page: int, back_cb: str, page_cb_prefix: str) -> InlineKeyboardMarkup:
    """Paginated product list for regular users."""
    total_pages = max(1, (len(products) + PER_PAGE - 1) // PER_PAGE)
    chunk = products[page * PER_PAGE:(page + 1) * PER_PAGE]
    btns = []
    for p in chunk:
        price_s = f" · {p['price']} ₽/{p['unit']}" if p["price"] > 0 else " · цена по запросу"
        btns.append([InlineKeyboardButton(
            text=f"{p['name']}{price_s}",
            callback_data=f"prod:{p['id']}",
        )])
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"{page_cb_prefix}:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"{page_cb_prefix}:{page+1}"))
        btns.append(nav)
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=btns)


# ═══════════════════════════════════════════════════════
#  STATES
# ═══════════════════════════════════════════════════════

class AdminStates(StatesGroup):
    broadcast_text = State()
    broadcast_photo = State()
    entering_base_price = State()
    entering_ind_price = State()
    adding_product_name = State()
    adding_product_price = State()


# ═══════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


def _back_cb_for_product(cat: str, subcat) -> str:
    if cat == "weight":
        return "adm:prod:cat:weight"
    return f"adm:prod:sub:{subcat}"


def _add_cb_for(cat: str, subcat) -> str:
    sub = subcat if subcat else "none"
    return f"adm:prod:add:{cat}:{sub}"


async def _build_order_detail_text(db, oid: int) -> str:
    rows = await db.execute_fetchall(
        "SELECT id, user_id, status, delivery_date, delivery_time, address, phone, "
        "payment_method, total, items_json, created_at FROM orders WHERE id=?",
        (oid,),
    )
    if not rows:
        return "Заказ не найден."
    o = rows[0]
    u = await db.execute_fetchall("SELECT full_name, username FROM users WHERE tg_id=?", (o[1],))
    name = u[0][0] if u else str(o[1])
    username = f"@{u[0][1]}" if u and u[0][1] else "—"
    items = json.loads(o[9])
    lines = []
    for i in items:
        lines.append(f"  • {i['name']} × {i['qty']} {i['unit']} = {i['price'] * i['qty']:.0f} ₽")
        if i.get("note"):
            lines.append(f"    📝 {i['note']}")
    st = STATUS_LABELS.get(o[2], o[2])
    pay = PAYMENT_LABELS.get(o[7], o[7])
    return (
        f"📋 Заказ #{o[0]}\n"
        f"👤 {name} | {username}\n"
        f"📱 {o[6] or '—'}\n"
        f"📅 {o[3]} {o[4]}\n"
        f"📍 {o[5] or '—'}\n"
        f"💳 {pay}\n"
        f"📌 {st}\n\n"
        + "\n".join(lines)
        + f"\n\n💰 Итого: {o[8]:.0f} ₽"
    )


async def _get_products(cat: str, subcat) -> list:
    db = config.DB
    if cat == "weight":
        rows = await db.execute_fetchall(
            "SELECT id, name, base_price, unit FROM products "
            "WHERE category='weight' AND active=1 ORDER BY sort_order, id"
        )
    else:
        rows = await db.execute_fetchall(
            "SELECT id, name, base_price, unit FROM products "
            "WHERE category='wild' AND subcategory=? AND active=1 ORDER BY sort_order, id",
            (subcat,),
        )
    return [{"id": r[0], "name": r[1], "price": r[2], "unit": r[3]} for r in rows]


async def _show_product_list_adm(cb: CallbackQuery, cat: str, subcat, page: int = 0):
    if cat == "weight":
        title = "⚖️ <b>Лёд на развес</b>"
        back_cb = "adm:products"
    else:
        title = f"🧊 <b>{SUBCAT_LABELS.get(subcat, subcat)}</b>"
        back_cb = "adm:prod:cat:wild"

    products = await _get_products(cat, subcat)
    add_cb = _add_cb_for(cat, subcat)
    count_str = f"Товаров: {len(products)}" if products else "Товаров нет"
    await cb.message.edit_text(
        f"{title}\n\n{count_str}",
        reply_markup=_paginate_products_adm(products, page, back_cb, add_cb),
        parse_mode="HTML",
    )


# ═══════════════════════════════════════════════════════
#  ВХОД В ПАНЕЛЬ
# ═══════════════════════════════════════════════════════

@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("🔧 Админ-панель:", reply_markup=admin_kb())


@router.callback_query(F.data == "adm:back")
async def cb_admin_back(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await cb.message.edit_text("🔧 Админ-панель:", reply_markup=admin_kb())
    await cb.answer()


# ═══════════════════════════════════════════════════════
#  ЗАКАЗЫ — ТАБЫ
# ═══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm:orders")
async def cb_orders(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text("📋 Заказы:", reply_markup=orders_tabs_kb())
    await cb.answer()


# ── Активные — по клиентам ──

@router.callback_query(F.data == "adm:orders:active")
async def cb_orders_active(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = config.DB
    rows = await db.execute_fetchall("""
        SELECT o.user_id, u.phone, u.full_name, COUNT(o.id), COALESCE(SUM(o.total), 0)
        FROM orders o
        LEFT JOIN users u ON o.user_id = u.tg_id
        WHERE o.status IN ('new', 'in_progress')
        GROUP BY o.user_id
        ORDER BY MAX(o.id) DESC
    """)
    if not rows:
        await cb.message.edit_text("🔄 Активных заказов нет.", reply_markup=orders_tabs_kb())
        await cb.answer()
        return
    clients = [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
    await cb.message.edit_text(
        "🔄 <b>Активные заказы — по клиентам:</b>",
        reply_markup=clients_list_kb(clients, "active"),
        parse_mode="HTML",
    )
    await cb.answer()


# ── Выполненные — по клиентам ──

@router.callback_query(F.data == "adm:orders:done")
async def cb_orders_done(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = config.DB
    rows = await db.execute_fetchall("""
        SELECT o.user_id, u.phone, u.full_name, COUNT(o.id), COALESCE(SUM(o.total), 0)
        FROM orders o
        LEFT JOIN users u ON o.user_id = u.tg_id
        WHERE o.status IN ('delivered', 'cancelled')
        GROUP BY o.user_id
        ORDER BY SUM(o.total) DESC
    """)
    if not rows:
        await cb.message.edit_text("✅ Выполненных заказов нет.", reply_markup=orders_tabs_kb())
        await cb.answer()
        return
    clients = [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
    await cb.message.edit_text(
        "✅ <b>Выполненные — по клиентам:</b>",
        reply_markup=clients_list_kb(clients, "done"),
        parse_mode="HTML",
    )
    await cb.answer()


# ── Заказы конкретного клиента (active или done) ──

@router.callback_query(F.data.startswith("adm:cli:"))
async def cb_client_orders(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    uid = int(parts[2])
    tab = parts[3]  # "active" or "done"

    db = config.DB
    if tab == "active":
        statuses = ("new", "in_progress")
    else:
        statuses = ("delivered", "cancelled")

    rows = await db.execute_fetchall(
        "SELECT id, status, total, delivery_date FROM orders "
        "WHERE user_id=? AND status IN (?,?) ORDER BY id DESC",
        (uid, *statuses),
    )
    u = await db.execute_fetchall("SELECT full_name, phone FROM users WHERE tg_id=?", (uid,))
    name = u[0][0] if u else str(uid)
    phone = u[0][1] if u else "—"

    if not rows:
        label = "активных" if tab == "active" else "выполненных"
        await cb.message.edit_text(
            f"У {name} нет {label} заказов.",
            reply_markup=orders_tabs_kb(),
        )
        await cb.answer()
        return

    orders = [{"id": r[0], "status": r[1], "total": r[2], "date": r[3] or "—"} for r in rows]
    tab_label = "🔄 Активные" if tab == "active" else "✅ Выполненные"
    await cb.message.edit_text(
        f"👤 <b>{html.escape(name)}</b>\n📱 {html.escape(phone or '—')}\n\n{tab_label} заказы:",
        reply_markup=client_orders_kb(orders, uid, tab),
        parse_mode="HTML",
    )
    await cb.answer()


# ── Детали заказа ──

@router.callback_query(F.data.startswith("adm:odetail:"))
async def cb_order_detail(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    parts = cb.data.split(":")
    oid = int(parts[2])
    tab = parts[3]
    uid = int(parts[4]) if len(parts) > 4 else 0

    db = config.DB
    text = await _build_order_detail_text(db, oid)
    await cb.message.edit_text(text, reply_markup=order_detail_kb(oid, uid, tab), parse_mode=None)
    await cb.answer()


# ── Смена статуса ──

@router.callback_query(F.data.startswith("adm:setstatus:"))
async def cb_adm_setstatus(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    parts = cb.data.split(":")
    oid = int(parts[2])
    new_status = parts[3]
    uid = int(parts[4]) if len(parts) > 4 else 0
    tab = parts[5] if len(parts) > 5 else "active"

    db = config.DB
    await db.execute("UPDATE orders SET status=? WHERE id=?", (new_status, oid))
    await db.commit()
    label = STATUS_LABELS.get(new_status, new_status)
    await cb.answer(f"Статус: {label}", show_alert=True)

    # After status change: if moved out of active — go back to client list
    if tab == "active" and new_status in ("delivered", "cancelled"):
        # Refresh active client list
        rows = await db.execute_fetchall("""
            SELECT o.user_id, u.phone, u.full_name, COUNT(o.id), COALESCE(SUM(o.total), 0)
            FROM orders o
            LEFT JOIN users u ON o.user_id = u.tg_id
            WHERE o.status IN ('new', 'in_progress')
            GROUP BY o.user_id
            ORDER BY MAX(o.id) DESC
        """)
        if not rows:
            await cb.message.edit_text("🔄 Активных заказов нет.", reply_markup=orders_tabs_kb())
        else:
            clients = [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
            await cb.message.edit_text(
                "🔄 <b>Активные заказы — по клиентам:</b>",
                reply_markup=clients_list_kb(clients, "active"),
                parse_mode="HTML",
            )
    else:
        # Refresh the order detail
        text = await _build_order_detail_text(db, oid)
        await cb.message.edit_text(text, reply_markup=order_detail_kb(oid, uid, tab), parse_mode=None)


@router.callback_query(F.data.startswith("status:"))
async def cb_status(cb: CallbackQuery):
    """For NOTIFY_IDS messages."""
    if cb.from_user.id not in ADMIN_IDS and cb.from_user.id not in NOTIFY_IDS:
        await cb.answer("Нет доступа", show_alert=True)
        return
    db = config.DB
    parts = cb.data.split(":")
    oid, st = int(parts[1]), parts[2]
    await db.execute("UPDATE orders SET status=? WHERE id=?", (st, oid))
    await db.commit()
    label = STATUS_LABELS.get(st, st)
    await cb.answer(f"Статус: {label}", show_alert=True)
    lines = [ln for ln in cb.message.text.split("\n") if not ln.startswith("📌 Статус:")]
    lines.append(f"📌 Статус: {label}")
    await cb.message.edit_text("\n".join(lines), reply_markup=order_status_kb(oid))


# ═══════════════════════════════════════════════════════
#  ОТЧЁТ
# ═══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm:report")
async def cb_report(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    text = await build_report()
    await cb.message.edit_text(text, reply_markup=report_kb(), parse_mode=None)
    await cb.answer()


@router.message(Command("report"))
async def cmd_report(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(await build_report(), parse_mode=None)


@router.callback_query(F.data == "adm:report:reset_confirm")
async def cb_report_reset_confirm(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "⚠️ Вы уверены?\n\n"
        "Это действие удалит ВСЕ заказы из базы данных.\n"
        "Восстановить их будет невозможно.",
        reply_markup=report_reset_confirm_kb(),
    )
    await cb.answer()


@router.callback_query(F.data == "adm:report:reset_do")
async def cb_report_reset_do(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = config.DB
    await db.execute("DELETE FROM orders")
    await db.commit()
    await cb.answer("✅ Статистика сброшена", show_alert=True)
    text = await build_report()
    await cb.message.edit_text(text, reply_markup=report_kb(), parse_mode=None)


async def build_report() -> str:
    db = config.DB
    now = datetime.now()
    today_str = now.strftime("%d.%m.%Y %H:%M")

    all_st = await db.execute_fetchall(
        "SELECT COUNT(*), COALESCE(SUM(total), 0), COUNT(DISTINCT user_id) "
        "FROM orders WHERE status != 'cancelled'"
    )
    all_cnt, all_rev, all_cli = all_st[0]

    m_start = f"{now.year}-{now.month:02d}-01"
    m_end = (
        f"{now.year}-{now.month + 1:02d}-01"
        if now.month < 12
        else f"{now.year + 1}-01-01"
    )
    m_st = await db.execute_fetchall(
        "SELECT COUNT(*), COALESCE(SUM(total), 0), COUNT(DISTINCT user_id) "
        "FROM orders WHERE created_at >= ? AND created_at < ? AND status != 'cancelled'",
        (m_start, m_end),
    )
    m_cnt, m_rev, m_cli = m_st[0]

    active_st = await db.execute_fetchall(
        "SELECT COUNT(*), COALESCE(SUM(total), 0) "
        "FROM orders WHERE status IN ('new', 'in_progress')"
    )
    active_cnt, active_rev = active_st[0]

    lines = [
        f"📊 Отчёт на {today_str}\n",
        "━━━ За всё время ━━━",
        f"💰 Оборот: {all_rev:.0f} ₽",
        f"📦 Заказов выполнено: {all_cnt}",
        f"👥 Уникальных клиентов: {all_cli}",
        "",
        f"━━━ {now.month:02d}.{now.year} ━━━",
        f"💰 Оборот: {m_rev:.0f} ₽",
        f"📦 Заказов: {m_cnt}",
        f"👥 Клиентов: {m_cli}",
        "",
        "━━━ Сейчас в работе ━━━",
        f"🔄 Заказов: {active_cnt}",
        f"💵 На сумму: {active_rev:.0f} ₽",
    ]

    top_cl = await db.execute_fetchall("""
        SELECT u.full_name, u.phone, COUNT(o.id), SUM(o.total)
        FROM orders o JOIN users u ON o.user_id = u.tg_id
        WHERE o.status != 'cancelled'
        GROUP BY o.user_id ORDER BY SUM(o.total) DESC LIMIT 5
    """)
    if top_cl:
        lines.append("\n━━━ Топ-5 клиентов ━━━")
        for i, c in enumerate(top_cl, 1):
            lines.append(f"  {i}. {c[0]} | {c[2]} зак. | {c[3]:.0f} ₽")

    items_rows = await db.execute_fetchall(
        "SELECT items_json FROM orders WHERE status != 'cancelled'"
    )
    totals: dict = {}
    for r in items_rows:
        for it in json.loads(r[0]):
            k = it["name"]
            totals.setdefault(k, {"qty": 0, "unit": it.get("unit", "шт"), "rev": 0})
            totals[k]["qty"] += it["qty"]
            totals[k]["rev"] += it["price"] * it["qty"]
    if totals:
        sorted_items = sorted(totals.items(), key=lambda x: x[1]["rev"], reverse=True)
        lines.append("\n━━━ Товары (всё время) ━━━")
        for n, d in sorted_items:
            lines.append(f"  • {n}: {d['qty']} {d['unit']} | {d['rev']:.0f} ₽")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════
#  РАССЫЛКА
# ═══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm:broadcast")
async def cb_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(AdminStates.broadcast_text)
    await cb.message.edit_text(
        "📢 Рассылка\n\nШаг 1/2 — введите текст сообщения\n(/cancel для отмены):"
    )
    await cb.answer()


@router.message(AdminStates.broadcast_text)
async def on_broadcast_text(message: Message, state: FSMContext):
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=admin_kb())
        return
    if not message.text:
        await message.answer("Отправьте текст сообщения:")
        return
    await state.update_data(broadcast_text=message.text)
    await state.set_state(AdminStates.broadcast_photo)
    await message.answer(
        "Шаг 2/2 — прикрепите фото к рассылке\n"
        "(или отправьте /skip чтобы разослать без фото):"
    )


@router.message(AdminStates.broadcast_photo)
async def on_broadcast_photo(message: Message, state: FSMContext):
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=admin_kb())
        return

    d = await state.get_data()
    text = d.get("broadcast_text", "")
    photo_id = None

    if message.photo:
        photo_id = message.photo[-1].file_id
    elif message.text and message.text.strip() == "/skip":
        photo_id = None
    else:
        await message.answer("Отправьте фото или /skip, /cancel для отмены:")
        return

    await state.clear()
    db = config.DB
    users = await db.execute_fetchall("SELECT tg_id FROM users WHERE phone IS NOT NULL AND phone != ''")
    sent, failed = 0, 0
    for u in users:
        try:
            if photo_id:
                await message.bot.send_photo(u[0], photo=photo_id, caption=text)
            else:
                await message.bot.send_message(u[0], text)
            sent += 1
        except Exception:
            failed += 1

    await message.answer(
        f"📢 Рассылка завершена\n\n✅ Доставлено: {sent} из {sent + failed}\n❌ Не доставлено: {failed}",
        reply_markup=admin_kb(),
    )


# ═══════════════════════════════════════════════════════
#  УПРАВЛЕНИЕ ТОВАРАМИ
# ═══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm:products")
async def cb_adm_products(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    await cb.message.edit_text(
        "📦 <b>Управление товарами</b>\n\nВыберите категорию:",
        reply_markup=products_categories_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "adm:prod:cat:wild")
async def cb_adm_prod_wild(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "🧊 <b>Дикий лёд</b>\n\nВыберите подкатегорию:",
        reply_markup=wild_subcats_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "adm:prod:cat:weight")
async def cb_adm_prod_weight(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await _show_product_list_adm(cb, "weight", None)
    await cb.answer()


@router.callback_query(F.data.startswith("adm:prod:sub:"))
async def cb_adm_prod_sub(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    subcat = cb.data.split(":")[3]
    await _show_product_list_adm(cb, "wild", subcat)
    await cb.answer()


# ── Пагинация страниц товаров (адм) ──
# callback: adm:prod:pg:{page}:{add_cb}
# add_cb содержит двоеточия, поэтому склеиваем всё после 3-го ":"

@router.callback_query(F.data.startswith("adm:prod:pg:"))
async def cb_adm_prod_page(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    # format: adm:prod:pg:{page}:{cat}:{subcat_or_none}
    parts = cb.data.split(":")
    page = int(parts[3])
    # add_cb starts from index 4 joined back
    add_cb = ":".join(parts[4:])
    # add_cb format: adm:prod:add:{cat}:{subcat}
    add_parts = add_cb.split(":")
    cat = add_parts[3]
    subcat = add_parts[4] if add_parts[4] != "none" else None
    await _show_product_list_adm(cb, cat, subcat, page)
    await cb.answer()


@router.callback_query(F.data.startswith("adm:prod:detail:"))
async def cb_adm_prod_detail(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    pid = int(cb.data.split(":")[3])
    db = config.DB
    r = await db.execute_fetchall(
        "SELECT name, base_price, unit, category, subcategory FROM products WHERE id=?", (pid,)
    )
    if not r:
        await cb.answer("Товар не найден.", show_alert=True)
        return
    name, base_price, unit, cat, subcat = r[0]
    ind = await db.execute_fetchall("""
        SELECT u.full_name, ip.price FROM individual_prices ip
        JOIN users u ON ip.user_id = u.tg_id WHERE ip.product_id = ?
    """, (pid,))
    price_str = f"{base_price} ₽/{unit}" if base_price > 0 else "по запросу"
    text = f"📦 <b>{html.escape(name)}</b>\n💰 Базовая цена: {price_str}"
    if ind:
        text += "\n\n👤 <b>Инд. цены:</b>"
        for i in ind:
            text += f"\n  • {html.escape(i[0])}: {i[1]} ₽"
    back_cb = _back_cb_for_product(cat, subcat)
    await cb.message.edit_text(text, reply_markup=product_detail_adm_kb(pid, back_cb), parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data.startswith("adm:prod:price:"))
async def cb_adm_prod_price(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    pid = int(cb.data.split(":")[3])
    db = config.DB
    r = await db.execute_fetchall("SELECT name, base_price, unit FROM products WHERE id=?", (pid,))
    if not r:
        await cb.answer("Товар не найден.", show_alert=True)
        return
    await state.update_data(edit_pid=pid)
    await state.set_state(AdminStates.entering_base_price)
    await cb.message.edit_text(
        f"📦 <b>{r[0][0]}</b>\nТекущая цена: {r[0][1]} ₽/{r[0][2]}\n\n"
        f"Введите новую цену (0 = по запросу):",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(AdminStates.entering_base_price)
async def on_base_price(message: Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
        assert price >= 0
    except (ValueError, AssertionError):
        await message.answer("Введите число (0 или больше):")
        return
    d = await state.get_data()
    pid = d["edit_pid"]
    db = config.DB
    await db.execute("UPDATE products SET base_price=? WHERE id=?", (price, pid))
    await db.commit()
    r = await db.execute_fetchall("SELECT name, unit FROM products WHERE id=?", (pid,))
    await state.clear()
    price_str = f"{price} ₽/{r[0][1]}" if price > 0 else "по запросу"
    await message.answer(
        f"✅ Цена обновлена: <b>{r[0][0]}</b> — {price_str}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К товару", callback_data=f"adm:prod:detail:{pid}")],
            [InlineKeyboardButton(text="◀️ Товары", callback_data="adm:products")],
        ]),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("adm:prod:del:"))
async def cb_adm_prod_del(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    pid = int(cb.data.split(":")[3])
    db = config.DB
    r = await db.execute_fetchall("SELECT category, subcategory, name FROM products WHERE id=?", (pid,))
    if not r:
        await cb.answer("Товар не найден.", show_alert=True)
        return
    cat, subcat, name = r[0]
    await db.execute("DELETE FROM individual_prices WHERE product_id=?", (pid,))
    await db.execute("DELETE FROM products WHERE id=?", (pid,))
    await db.commit()
    await cb.answer(f"«{name}» удалён.", show_alert=True)
    await _show_product_list_adm(cb, cat, subcat)


@router.callback_query(F.data.startswith("adm:prod:add:"))
async def cb_adm_prod_add(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    cat = parts[3]
    subcat = parts[4] if parts[4] != "none" else None
    await state.update_data(new_prod_cat=cat, new_prod_subcat=subcat)
    await state.set_state(AdminStates.adding_product_name)
    await cb.message.edit_text(
        "➕ <b>Добавление товара</b>\n\nШаг 1/3 — Введите название:",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(AdminStates.adding_product_name)
async def on_add_product_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        await message.answer("Название не может быть пустым:")
        return
    await state.update_data(new_prod_name=name)
    await state.set_state(None)
    await message.answer(
        f"Шаг 2/3 — Выберите единицу измерения для <b>{name}</b>:",
        reply_markup=unit_select_kb(),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("adm:prod:unit:"))
async def cb_adm_prod_unit(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    unit = cb.data.split(":")[3]
    d = await state.get_data()
    if "new_prod_name" not in d:
        await cb.answer("Сессия истекла. Начните заново.", show_alert=True)
        await state.clear()
        return
    await state.update_data(new_prod_unit=unit)
    await state.set_state(AdminStates.adding_product_price)
    await cb.message.edit_text(
        f"Шаг 3/3 — Введите цену за <b>{unit}</b>\n(0 = цена по запросу):",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(AdminStates.adding_product_price)
async def on_add_product_price(message: Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
        assert price >= 0
    except (ValueError, AssertionError):
        await message.answer("Введите число (0 или больше):")
        return

    d = await state.get_data()
    cat = d["new_prod_cat"]
    subcat = d.get("new_prod_subcat")
    name = d["new_prod_name"]
    unit = d["new_prod_unit"]

    db = config.DB
    sort_rows = await db.execute_fetchall(
        "SELECT COALESCE(MAX(sort_order), 0) FROM products WHERE category=?", (cat,)
    )
    sort_order = sort_rows[0][0] + 1

    await db.execute(
        "INSERT INTO products(category, subcategory, name, unit, base_price, sort_order) VALUES(?,?,?,?,?,?)",
        (cat, subcat, name, unit, price, sort_order),
    )
    await db.commit()
    await state.clear()

    price_str = f"{price} ₽/{unit}" if price > 0 else "по запросу"
    back_cb = _back_cb_for_product(cat, subcat)
    await message.answer(
        f"✅ Товар добавлен!\n\n📦 <b>{name}</b>\n💰 {price_str}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К списку", callback_data=back_cb)],
            [InlineKeyboardButton(text="◀️ Товары", callback_data="adm:products")],
        ]),
        parse_mode="HTML",
    )


# ═══════════════════════════════════════════════════════
#  ИНДИВИДУАЛЬНЫЕ ЦЕНЫ
# ═══════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("indp:"))
async def cb_ind_price_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    pid = int(cb.data.split(":")[1])
    db = config.DB
    users = await db.execute_fetchall(
        "SELECT tg_id, username, full_name, phone FROM users ORDER BY full_name"
    )
    if not users:
        await cb.answer("Клиентов нет.", show_alert=True)
        return
    await cb.message.edit_text("👤 Выберите клиента:", reply_markup=users_page_kb(users, pid, 0))
    await cb.answer()


@router.callback_query(F.data.startswith("upage:"))
async def cb_upage(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    pid, page = int(parts[1]), int(parts[2])
    db = config.DB
    users = await db.execute_fetchall(
        "SELECT tg_id, username, full_name, phone FROM users ORDER BY full_name"
    )
    await cb.message.edit_text("👤 Выберите клиента:", reply_markup=users_page_kb(users, pid, page))
    await cb.answer()


@router.callback_query(F.data.startswith("indpu:"))
async def cb_ind_price_enter(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    pid, uid = int(parts[1]), int(parts[2])
    db = config.DB
    prod = await db.execute_fetchall("SELECT name, base_price, unit FROM products WHERE id=?", (pid,))
    user = await db.execute_fetchall("SELECT full_name FROM users WHERE tg_id=?", (uid,))
    cur = await db.execute_fetchall(
        "SELECT price FROM individual_prices WHERE user_id=? AND product_id=?", (uid, pid)
    )
    cur_text = f"{cur[0][0]} ₽" if cur else "нет"
    await state.update_data(edit_pid=pid, edit_uid=uid)
    await state.set_state(AdminStates.entering_ind_price)
    await cb.message.edit_text(
        f"📦 {html.escape(prod[0][0])} (база: {prod[0][1]} ₽/{prod[0][2]})\n"
        f"👤 {html.escape(user[0][0]) if user else uid}\n"
        f"Текущая инд. цена: {cur_text}\n\n"
        f"Введите новую инд. цену (0 = удалить):"
    )
    await cb.answer()


@router.message(AdminStates.entering_ind_price)
async def on_ind_price(message: Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
        assert price >= 0
    except (ValueError, AssertionError):
        await message.answer("Введите число (0 = удалить):")
        return
    d = await state.get_data()
    pid, uid = d["edit_pid"], d["edit_uid"]
    db = config.DB
    if price == 0:
        await db.execute("DELETE FROM individual_prices WHERE user_id=? AND product_id=?", (uid, pid))
        await db.commit()
        await state.clear()
        await message.answer(
            "🗑 Инд. цена удалена.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ К товару", callback_data=f"adm:prod:detail:{pid}")],
                [InlineKeyboardButton(text="◀️ Товары", callback_data="adm:products")],
            ]),
        )
    else:
        await db.execute("""
            INSERT INTO individual_prices (user_id, product_id, price) VALUES (?,?,?)
            ON CONFLICT(user_id, product_id) DO UPDATE SET price=excluded.price
        """, (uid, pid, price))
        await db.commit()
        prod = await db.execute_fetchall("SELECT name FROM products WHERE id=?", (pid,))
        user = await db.execute_fetchall("SELECT full_name FROM users WHERE tg_id=?", (uid,))
        await state.clear()
        await message.answer(
            f"✅ {user[0][0] if user else uid}: {prod[0][0]} = {price} ₽",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ К товару", callback_data=f"adm:prod:detail:{pid}")],
                [InlineKeyboardButton(text="◀️ Товары", callback_data="adm:products")],
            ]),
        )


@router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()


# ═══════════════════════════════════════════════════════
#  ПЛАНИРОВЩИК
# ═══════════════════════════════════════════════════════

async def run_scheduler(bot_instance: Bot):
    while True:
        now = datetime.now()
        try:
            db = config.DB
            if db is None:
                await asyncio.sleep(60)
                continue

            if now.day == 1 and now.hour == 9 and now.minute == 0:
                text = await build_report()
                for cid in set(ADMIN_IDS + NOTIFY_IDS):
                    try:
                        await bot_instance.send_message(cid, text, parse_mode=None)
                    except Exception:
                        pass

            if now.hour == 10 and now.minute == 0:
                from datetime import timedelta
                threshold = (now - timedelta(days=INACTIVE_DAYS)).isoformat()
                users = await db.execute_fetchall(
                    "SELECT tg_id FROM users WHERE last_order_at IS NOT NULL AND last_order_at < ?",
                    (threshold,),
                )
                for u in users:
                    try:
                        await bot_instance.send_message(
                            u[0],
                            "👋 Давно не заказывали лёд! Пора пополнить запасы?",
                            reply_markup=repeat_order_kb(),
                        )
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"Scheduler: {e}")
        await asyncio.sleep(60)import json
import asyncio
import logging
from datetime import datetime

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command

from config import ADMIN_IDS, NOTIFY_IDS, INACTIVE_DAYS
import config

logger = logging.getLogger(__name__)
router = Router()

STATUS_LABELS = {
    "new": "🆕 Новый",
    "in_progress": "🔄 В работе",
    "delivered": "✅ Доставлен",
    "cancelled": "❌ Отменён",
}
PAYMENT_LABELS = {
    "card": "💳 Безналичная",
    "cash": "💵 Наличные",
    "transfer": "📲 Перевод",
}
SUBCAT_LABELS = {
    "cubes": "🟦 Кубы",
    "bars": "🔷 Бруски",
    "special": "✨ Особые формы",
}

PER_PAGE = 6


# ═══════════════════════════════════════════════════════
#  КЛАВИАТУРЫ — ПАНЕЛЬ
# ═══════════════════════════════════════════════════════

def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Заказы", callback_data="adm:orders"),
            InlineKeyboardButton(text="📊 Отчёт", callback_data="adm:report"),
        ],
        [
            InlineKeyboardButton(text="📦 Товары", callback_data="adm:products"),
            InlineKeyboardButton(text="📢 Рассылка", callback_data="adm:broadcast"),
        ],
        [
            InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu:main"),
        ],
    ])


def admin_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="adm:back")],
    ])


def report_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Сбросить статистику", callback_data="adm:report:reset_confirm")],
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="adm:back")],
    ])


def report_reset_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, сбросить всё", callback_data="adm:report:reset_do"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="adm:report"),
        ],
    ])


# ═══════════════════════════════════════════════════════
#  КЛАВИАТУРЫ — ЗАКАЗЫ
# ═══════════════════════════════════════════════════════

def orders_tabs_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 В работе", callback_data="adm:orders:active"),
            InlineKeyboardButton(text="✅ Выполненные", callback_data="adm:orders:done"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")],
    ])


def clients_list_kb(clients: list, tab: str) -> InlineKeyboardMarkup:
    """Shared keyboard for both active and done client lists."""
    btns = []
    for uid, phone, name, cnt, total in clients:
        label_name = phone or name or str(uid)
        if tab == "active":
            label = f"{label_name} | {cnt} акт."
            cb = f"adm:cli:{uid}:active"
        else:
            label = f"{label_name} | {cnt} зак. | {total:.0f} ₽"
            cb = f"adm:cli:{uid}:done"
        btns.append([InlineKeyboardButton(text=label, callback_data=cb)])
    btns.append([InlineKeyboardButton(text="◀️ Заказы", callback_data="adm:orders")])
    btns.append([InlineKeyboardButton(text="◀️ Панель", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def client_orders_kb(orders: list, uid: int, tab: str) -> InlineKeyboardMarkup:
    """Order list for a specific client (active or done tab)."""
    btns = []
    for o in orders:
        st = STATUS_LABELS.get(o["status"], o["status"])
        btns.append([InlineKeyboardButton(
            text=f"#{o['id']} | {o['date']} | {o['total']:.0f} ₽ | {st}",
            callback_data=f"adm:odetail:{o['id']}:{tab}:{uid}",
        )])
    btns.append([InlineKeyboardButton(
        text="◀️ К списку клиентов",
        callback_data=f"adm:orders:{tab}",
    )])
    btns.append([
        InlineKeyboardButton(text="◀️ Заказы", callback_data="adm:orders"),
        InlineKeyboardButton(text="◀️ Панель", callback_data="adm:back"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def order_detail_kb(oid: int, uid: int, tab: str) -> InlineKeyboardMarkup:
    """Detail view for one order — active gets status buttons, done does not."""
    btns = []
    if tab == "active":
        btns.append([
            InlineKeyboardButton(text="✅ Доставлен", callback_data=f"adm:setstatus:{oid}:delivered:{uid}:{tab}"),
            InlineKeyboardButton(text="🔄 В работе", callback_data=f"adm:setstatus:{oid}:in_progress:{uid}:{tab}"),
        ])
        btns.append([
            InlineKeyboardButton(text="❌ Отменить", callback_data=f"adm:setstatus:{oid}:cancelled:{uid}:{tab}"),
        ])
    btns.append([InlineKeyboardButton(
        text="◀️ К заказам клиента",
        callback_data=f"adm:cli:{uid}:{tab}",
    )])
    btns.append([
        InlineKeyboardButton(text="◀️ Заказы", callback_data="adm:orders"),
        InlineKeyboardButton(text="◀️ Панель", callback_data="adm:back"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def notify_order_kb(oid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Доставлен", callback_data=f"adm:setstatus:{oid}:delivered:0:active"),
            InlineKeyboardButton(text="❌ Отменить", callback_data=f"adm:setstatus:{oid}:cancelled:0:active"),
        ],
    ])


def order_status_kb(oid: int) -> InlineKeyboardMarkup:
    """For NOTIFY_IDS messages."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🆕", callback_data=f"status:{oid}:new"),
            InlineKeyboardButton(text="🔄", callback_data=f"status:{oid}:in_progress"),
        ],
        [
            InlineKeyboardButton(text="✅", callback_data=f"status:{oid}:delivered"),
            InlineKeyboardButton(text="❌", callback_data=f"status:{oid}:cancelled"),
        ],
    ])


def repeat_order_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Повторить заказ", callback_data="menu:repeat")],
    ])


# ═══════════════════════════════════════════════════════
#  КЛАВИАТУРЫ — ТОВАРЫ (общая пагинация)
# ═══════════════════════════════════════════════════════

def products_categories_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧊 Дикий лёд", callback_data="adm:prod:cat:wild")],
        [InlineKeyboardButton(text="⚖️ Лёд на развес", callback_data="adm:prod:cat:weight")],
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="adm:back")],
    ])


def wild_subcats_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟦 Кубы", callback_data="adm:prod:sub:cubes")],
        [InlineKeyboardButton(text="🔷 Бруски", callback_data="adm:prod:sub:bars")],
        [InlineKeyboardButton(text="✨ Особые формы", callback_data="adm:prod:sub:special")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:products")],
    ])


def _paginate_products_adm(products: list, page: int, back_cb: str, add_cb: str) -> InlineKeyboardMarkup:
    """Paginated product list for admin."""
    total_pages = max(1, (len(products) + PER_PAGE - 1) // PER_PAGE)
    chunk = products[page * PER_PAGE:(page + 1) * PER_PAGE]
    btns = []
    for p in chunk:
        price_s = f"{p['price']} ₽/{p['unit']}" if p["price"] > 0 else "по запросу"
        btns.append([InlineKeyboardButton(
            text=f"{p['name']} — {price_s}",
            callback_data=f"adm:prod:detail:{p['id']}",
        )])
    # Pagination nav
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"adm:prod:pg:{page-1}:{add_cb}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"adm:prod:pg:{page+1}:{add_cb}"))
        btns.append(nav)
    btns.append([InlineKeyboardButton(text="➕ Добавить товар", callback_data=add_cb)])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def product_detail_adm_kb(pid: int, back_cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить цену", callback_data=f"adm:prod:price:{pid}")],
        [InlineKeyboardButton(text="👤 Инд. цена для клиента", callback_data=f"indp:{pid}")],
        [InlineKeyboardButton(text="❌ Удалить товар", callback_data=f"adm:prod:del:{pid}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)],
    ])


def unit_select_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="шт", callback_data="adm:prod:unit:шт"),
            InlineKeyboardButton(text="кг", callback_data="adm:prod:unit:кг"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:products")],
    ])


def users_page_kb(users: list, pid: int, page: int) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(users) + PER_PAGE - 1) // PER_PAGE)
    chunk = users[page * PER_PAGE:(page + 1) * PER_PAGE]
    btns = []
    for u in chunk:
        name = u[2] or "—"
        btns.append([InlineKeyboardButton(
            text=f"{name} | {u[3] or '—'}",
            callback_data=f"indpu:{pid}:{u[0]}",
        )])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"upage:{pid}:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"upage:{pid}:{page + 1}"))
    if nav:
        btns.append(nav)
    btns.append([InlineKeyboardButton(text="◀️ К товару", callback_data=f"adm:prod:detail:{pid}")])
    btns.append([InlineKeyboardButton(text="◀️ Панель", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


# ═══════════════════════════════════════════════════════
#  КЛАВИАТУРА — ПАГИНАЦИЯ ТОВАРОВ ДЛЯ ПОЛЬЗОВАТЕЛЯ
# ═══════════════════════════════════════════════════════

def paginate_products_user(products: list, page: int, back_cb: str, page_cb_prefix: str) -> InlineKeyboardMarkup:
    """Paginated product list for regular users."""
    total_pages = max(1, (len(products) + PER_PAGE - 1) // PER_PAGE)
    chunk = products[page * PER_PAGE:(page + 1) * PER_PAGE]
    btns = []
    for p in chunk:
        price_s = f" · {p['price']} ₽/{p['unit']}" if p["price"] > 0 else " · цена по запросу"
        btns.append([InlineKeyboardButton(
            text=f"{p['name']}{price_s}",
            callback_data=f"prod:{p['id']}",
        )])
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"{page_cb_prefix}:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"{page_cb_prefix}:{page+1}"))
        btns.append(nav)
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=btns)


# ═══════════════════════════════════════════════════════
#  STATES
# ═══════════════════════════════════════════════════════

class AdminStates(StatesGroup):
    broadcast_text = State()
    broadcast_photo = State()
    entering_base_price = State()
    entering_ind_price = State()
    adding_product_name = State()
    adding_product_price = State()


# ═══════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


def _back_cb_for_product(cat: str, subcat) -> str:
    if cat == "weight":
        return "adm:prod:cat:weight"
    return f"adm:prod:sub:{subcat}"


def _add_cb_for(cat: str, subcat) -> str:
    sub = subcat if subcat else "none"
    return f"adm:prod:add:{cat}:{sub}"


async def _build_order_detail_text(db, oid: int) -> str:
    rows = await db.execute_fetchall(
        "SELECT id, user_id, status, delivery_date, delivery_time, address, phone, "
        "payment_method, total, items_json, created_at FROM orders WHERE id=?",
        (oid,),
    )
    if not rows:
        return "Заказ не найден."
    o = rows[0]
    u = await db.execute_fetchall("SELECT full_name, username FROM users WHERE tg_id=?", (o[1],))
    name = u[0][0] if u else str(o[1])
    username = f"@{u[0][1]}" if u and u[0][1] else "—"
    items = json.loads(o[9])
    lines = []
    for i in items:
        lines.append(f"  • {i['name']} × {i['qty']} {i['unit']} = {i['price'] * i['qty']:.0f} ₽")
        if i.get("note"):
            lines.append(f"    📝 {i['note']}")
    st = STATUS_LABELS.get(o[2], o[2])
    pay = PAYMENT_LABELS.get(o[7], o[7])
    return (
        f"📋 Заказ #{o[0]}\n"
        f"👤 {name} | {username}\n"
        f"📱 {o[6] or '—'}\n"
        f"📅 {o[3]} {o[4]}\n"
        f"📍 {o[5] or '—'}\n"
        f"💳 {pay}\n"
        f"📌 {st}\n\n"
        + "\n".join(lines)
        + f"\n\n💰 Итого: {o[8]:.0f} ₽"
    )


async def _get_products(cat: str, subcat) -> list:
    db = config.DB
    if cat == "weight":
        rows = await db.execute_fetchall(
            "SELECT id, name, base_price, unit FROM products "
            "WHERE category='weight' AND active=1 ORDER BY sort_order, id"
        )
    else:
        rows = await db.execute_fetchall(
            "SELECT id, name, base_price, unit FROM products "
            "WHERE category='wild' AND subcategory=? AND active=1 ORDER BY sort_order, id",
            (subcat,),
        )
    return [{"id": r[0], "name": r[1], "price": r[2], "unit": r[3]} for r in rows]


async def _show_product_list_adm(cb: CallbackQuery, cat: str, subcat, page: int = 0):
    if cat == "weight":
        title = "⚖️ <b>Лёд на развес</b>"
        back_cb = "adm:products"
    else:
        title = f"🧊 <b>{SUBCAT_LABELS.get(subcat, subcat)}</b>"
        back_cb = "adm:prod:cat:wild"

    products = await _get_products(cat, subcat)
    add_cb = _add_cb_for(cat, subcat)
    count_str = f"Товаров: {len(products)}" if products else "Товаров нет"
    await cb.message.edit_text(
        f"{title}\n\n{count_str}",
        reply_markup=_paginate_products_adm(products, page, back_cb, add_cb),
        parse_mode="HTML",
    )


# ═══════════════════════════════════════════════════════
#  ВХОД В ПАНЕЛЬ
# ═══════════════════════════════════════════════════════

@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("🔧 Админ-панель:", reply_markup=admin_kb())


@router.callback_query(F.data == "adm:back")
async def cb_admin_back(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await cb.message.edit_text("🔧 Админ-панель:", reply_markup=admin_kb())
    await cb.answer()


# ═══════════════════════════════════════════════════════
#  ЗАКАЗЫ — ТАБЫ
# ═══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm:orders")
async def cb_orders(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text("📋 Заказы:", reply_markup=orders_tabs_kb())
    await cb.answer()


# ── Активные — по клиентам ──

@router.callback_query(F.data == "adm:orders:active")
async def cb_orders_active(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = config.DB
    rows = await db.execute_fetchall("""
        SELECT o.user_id, u.phone, u.full_name, COUNT(o.id), COALESCE(SUM(o.total), 0)
        FROM orders o
        LEFT JOIN users u ON o.user_id = u.tg_id
        WHERE o.status IN ('new', 'in_progress')
        GROUP BY o.user_id
        ORDER BY MAX(o.id) DESC
    """)
    if not rows:
        await cb.message.edit_text("🔄 Активных заказов нет.", reply_markup=orders_tabs_kb())
        await cb.answer()
        return
    clients = [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
    await cb.message.edit_text(
        "🔄 <b>Активные заказы — по клиентам:</b>",
        reply_markup=clients_list_kb(clients, "active"),
        parse_mode="HTML",
    )
    await cb.answer()


# ── Выполненные — по клиентам ──

@router.callback_query(F.data == "adm:orders:done")
async def cb_orders_done(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = config.DB
    rows = await db.execute_fetchall("""
        SELECT o.user_id, u.phone, u.full_name, COUNT(o.id), COALESCE(SUM(o.total), 0)
        FROM orders o
        LEFT JOIN users u ON o.user_id = u.tg_id
        WHERE o.status IN ('delivered', 'cancelled')
        GROUP BY o.user_id
        ORDER BY SUM(o.total) DESC
    """)
    if not rows:
        await cb.message.edit_text("✅ Выполненных заказов нет.", reply_markup=orders_tabs_kb())
        await cb.answer()
        return
    clients = [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
    await cb.message.edit_text(
        "✅ <b>Выполненные — по клиентам:</b>",
        reply_markup=clients_list_kb(clients, "done"),
        parse_mode="HTML",
    )
    await cb.answer()


# ── Заказы конкретного клиента (active или done) ──

@router.callback_query(F.data.startswith("adm:cli:"))
async def cb_client_orders(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    uid = int(parts[2])
    tab = parts[3]  # "active" or "done"

    db = config.DB
    if tab == "active":
        statuses = ("new", "in_progress")
    else:
        statuses = ("delivered", "cancelled")

    rows = await db.execute_fetchall(
        "SELECT id, status, total, delivery_date FROM orders "
        "WHERE user_id=? AND status IN (?,?) ORDER BY id DESC",
        (uid, *statuses),
    )
    u = await db.execute_fetchall("SELECT full_name, phone FROM users WHERE tg_id=?", (uid,))
    name = u[0][0] if u else str(uid)
    phone = u[0][1] if u else "—"

    if not rows:
        label = "активных" if tab == "active" else "выполненных"
        await cb.message.edit_text(
            f"У {name} нет {label} заказов.",
            reply_markup=orders_tabs_kb(),
        )
        await cb.answer()
        return

    orders = [{"id": r[0], "status": r[1], "total": r[2], "date": r[3] or "—"} for r in rows]
    tab_label = "🔄 Активные" if tab == "active" else "✅ Выполненные"
    await cb.message.edit_text(
        f"👤 <b>{name}</b>\n📱 {phone}\n\n{tab_label} заказы:",
        reply_markup=client_orders_kb(orders, uid, tab),
        parse_mode="HTML",
    )
    await cb.answer()


# ── Детали заказа ──

@router.callback_query(F.data.startswith("adm:odetail:"))
async def cb_order_detail(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    parts = cb.data.split(":")
    oid = int(parts[2])
    tab = parts[3]
    uid = int(parts[4]) if len(parts) > 4 else 0

    db = config.DB
    text = await _build_order_detail_text(db, oid)
    await cb.message.edit_text(text, reply_markup=order_detail_kb(oid, uid, tab), parse_mode=None)
    await cb.answer()


# ── Смена статуса ──

@router.callback_query(F.data.startswith("adm:setstatus:"))
async def cb_adm_setstatus(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    parts = cb.data.split(":")
    oid = int(parts[2])
    new_status = parts[3]
    uid = int(parts[4]) if len(parts) > 4 else 0
    tab = parts[5] if len(parts) > 5 else "active"

    db = config.DB
    await db.execute("UPDATE orders SET status=? WHERE id=?", (new_status, oid))
    await db.commit()
    label = STATUS_LABELS.get(new_status, new_status)
    await cb.answer(f"Статус: {label}", show_alert=True)

    # After status change: if moved out of active — go back to client list
    if tab == "active" and new_status in ("delivered", "cancelled"):
        # Refresh active client list
        rows = await db.execute_fetchall("""
            SELECT o.user_id, u.phone, u.full_name, COUNT(o.id), COALESCE(SUM(o.total), 0)
            FROM orders o
            LEFT JOIN users u ON o.user_id = u.tg_id
            WHERE o.status IN ('new', 'in_progress')
            GROUP BY o.user_id
            ORDER BY MAX(o.id) DESC
        """)
        if not rows:
            await cb.message.edit_text("🔄 Активных заказов нет.", reply_markup=orders_tabs_kb())
        else:
            clients = [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
            await cb.message.edit_text(
                "🔄 <b>Активные заказы — по клиентам:</b>",
                reply_markup=clients_list_kb(clients, "active"),
                parse_mode="HTML",
            )
    else:
        # Refresh the order detail
        text = await _build_order_detail_text(db, oid)
        await cb.message.edit_text(text, reply_markup=order_detail_kb(oid, uid, tab), parse_mode=None)


@router.callback_query(F.data.startswith("status:"))
async def cb_status(cb: CallbackQuery):
    """For NOTIFY_IDS messages."""
    if cb.from_user.id not in ADMIN_IDS and cb.from_user.id not in NOTIFY_IDS:
        await cb.answer("Нет доступа", show_alert=True)
        return
    db = config.DB
    parts = cb.data.split(":")
    oid, st = int(parts[1]), parts[2]
    await db.execute("UPDATE orders SET status=? WHERE id=?", (st, oid))
    await db.commit()
    label = STATUS_LABELS.get(st, st)
    await cb.answer(f"Статус: {label}", show_alert=True)
    lines = [ln for ln in cb.message.text.split("\n") if not ln.startswith("📌 Статус:")]
    lines.append(f"📌 Статус: {label}")
    await cb.message.edit_text("\n".join(lines), reply_markup=order_status_kb(oid))


# ═══════════════════════════════════════════════════════
#  ОТЧЁТ
# ═══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm:report")
async def cb_report(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    text = await build_report()
    await cb.message.edit_text(text, reply_markup=report_kb(), parse_mode=None)
    await cb.answer()


@router.message(Command("report"))
async def cmd_report(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(await build_report(), parse_mode=None)


@router.callback_query(F.data == "adm:report:reset_confirm")
async def cb_report_reset_confirm(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "⚠️ Вы уверены?\n\n"
        "Это действие удалит ВСЕ заказы из базы данных.\n"
        "Восстановить их будет невозможно.",
        reply_markup=report_reset_confirm_kb(),
    )
    await cb.answer()


@router.callback_query(F.data == "adm:report:reset_do")
async def cb_report_reset_do(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    db = config.DB
    await db.execute("DELETE FROM orders")
    await db.commit()
    await cb.answer("✅ Статистика сброшена", show_alert=True)
    text = await build_report()
    await cb.message.edit_text(text, reply_markup=report_kb(), parse_mode=None)


async def build_report() -> str:
    db = config.DB
    now = datetime.now()
    today_str = now.strftime("%d.%m.%Y %H:%M")

    all_st = await db.execute_fetchall(
        "SELECT COUNT(*), COALESCE(SUM(total), 0), COUNT(DISTINCT user_id) "
        "FROM orders WHERE status != 'cancelled'"
    )
    all_cnt, all_rev, all_cli = all_st[0]

    m_start = f"{now.year}-{now.month:02d}-01"
    m_end = (
        f"{now.year}-{now.month + 1:02d}-01"
        if now.month < 12
        else f"{now.year + 1}-01-01"
    )
    m_st = await db.execute_fetchall(
        "SELECT COUNT(*), COALESCE(SUM(total), 0), COUNT(DISTINCT user_id) "
        "FROM orders WHERE created_at >= ? AND created_at < ? AND status != 'cancelled'",
        (m_start, m_end),
    )
    m_cnt, m_rev, m_cli = m_st[0]

    active_st = await db.execute_fetchall(
        "SELECT COUNT(*), COALESCE(SUM(total), 0) "
        "FROM orders WHERE status IN ('new', 'in_progress')"
    )
    active_cnt, active_rev = active_st[0]

    lines = [
        f"📊 Отчёт на {today_str}\n",
        "━━━ За всё время ━━━",
        f"💰 Оборот: {all_rev:.0f} ₽",
        f"📦 Заказов выполнено: {all_cnt}",
        f"👥 Уникальных клиентов: {all_cli}",
        "",
        f"━━━ {now.month:02d}.{now.year} ━━━",
        f"💰 Оборот: {m_rev:.0f} ₽",
        f"📦 Заказов: {m_cnt}",
        f"👥 Клиентов: {m_cli}",
        "",
        "━━━ Сейчас в работе ━━━",
        f"🔄 Заказов: {active_cnt}",
        f"💵 На сумму: {active_rev:.0f} ₽",
    ]

    top_cl = await db.execute_fetchall("""
        SELECT u.full_name, u.phone, COUNT(o.id), SUM(o.total)
        FROM orders o JOIN users u ON o.user_id = u.tg_id
        WHERE o.status != 'cancelled'
        GROUP BY o.user_id ORDER BY SUM(o.total) DESC LIMIT 5
    """)
    if top_cl:
        lines.append("\n━━━ Топ-5 клиентов ━━━")
        for i, c in enumerate(top_cl, 1):
            lines.append(f"  {i}. {c[0]} | {c[2]} зак. | {c[3]:.0f} ₽")

    items_rows = await db.execute_fetchall(
        "SELECT items_json FROM orders WHERE status != 'cancelled'"
    )
    totals: dict = {}
    for r in items_rows:
        for it in json.loads(r[0]):
            k = it["name"]
            totals.setdefault(k, {"qty": 0, "unit": it.get("unit", "шт"), "rev": 0})
            totals[k]["qty"] += it["qty"]
            totals[k]["rev"] += it["price"] * it["qty"]
    if totals:
        sorted_items = sorted(totals.items(), key=lambda x: x[1]["rev"], reverse=True)
        lines.append("\n━━━ Товары (всё время) ━━━")
        for n, d in sorted_items:
            lines.append(f"  • {n}: {d['qty']} {d['unit']} | {d['rev']:.0f} ₽")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════
#  РАССЫЛКА
# ═══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm:broadcast")
async def cb_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(AdminStates.broadcast_text)
    await cb.message.edit_text(
        "📢 Рассылка\n\nШаг 1/2 — введите текст сообщения\n(/cancel для отмены):"
    )
    await cb.answer()


@router.message(AdminStates.broadcast_text)
async def on_broadcast_text(message: Message, state: FSMContext):
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=admin_kb())
        return
    if not message.text:
        await message.answer("Отправьте текст сообщения:")
        return
    await state.update_data(broadcast_text=message.text)
    await state.set_state(AdminStates.broadcast_photo)
    await message.answer(
        "Шаг 2/2 — прикрепите фото к рассылке\n"
        "(или отправьте /skip чтобы разослать без фото):"
    )


@router.message(AdminStates.broadcast_photo)
async def on_broadcast_photo(message: Message, state: FSMContext):
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=admin_kb())
        return

    d = await state.get_data()
    text = d.get("broadcast_text", "")
    photo_id = None

    if message.photo:
        photo_id = message.photo[-1].file_id
    elif message.text and message.text.strip() == "/skip":
        photo_id = None
    else:
        await message.answer("Отправьте фото или /skip, /cancel для отмены:")
        return

    await state.clear()
    db = config.DB
    users = await db.execute_fetchall("SELECT tg_id FROM users WHERE phone IS NOT NULL AND phone != ''")
    sent, failed = 0, 0
    for u in users:
        try:
            if photo_id:
                await message.bot.send_photo(u[0], photo=photo_id, caption=text)
            else:
                await message.bot.send_message(u[0], text)
            sent += 1
        except Exception:
            failed += 1

    await message.answer(
        f"📢 Рассылка завершена\n\n✅ Доставлено: {sent} из {sent + failed}\n❌ Не доставлено: {failed}",
        reply_markup=admin_kb(),
    )


# ═══════════════════════════════════════════════════════
#  УПРАВЛЕНИЕ ТОВАРАМИ
# ═══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm:products")
async def cb_adm_products(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    await cb.message.edit_text(
        "📦 <b>Управление товарами</b>\n\nВыберите категорию:",
        reply_markup=products_categories_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "adm:prod:cat:wild")
async def cb_adm_prod_wild(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_text(
        "🧊 <b>Дикий лёд</b>\n\nВыберите подкатегорию:",
        reply_markup=wild_subcats_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "adm:prod:cat:weight")
async def cb_adm_prod_weight(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await _show_product_list_adm(cb, "weight", None)
    await cb.answer()


@router.callback_query(F.data.startswith("adm:prod:sub:"))
async def cb_adm_prod_sub(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    subcat = cb.data.split(":")[3]
    await _show_product_list_adm(cb, "wild", subcat)
    await cb.answer()


# ── Пагинация страниц товаров (адм) ──
# callback: adm:prod:pg:{page}:{add_cb}
# add_cb содержит двоеточия, поэтому склеиваем всё после 3-го ":"

@router.callback_query(F.data.startswith("adm:prod:pg:"))
async def cb_adm_prod_page(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    # format: adm:prod:pg:{page}:{cat}:{subcat_or_none}
    parts = cb.data.split(":")
    page = int(parts[3])
    # add_cb starts from index 4 joined back
    add_cb = ":".join(parts[4:])
    # add_cb format: adm:prod:add:{cat}:{subcat}
    add_parts = add_cb.split(":")
    cat = add_parts[3]
    subcat = add_parts[4] if add_parts[4] != "none" else None
    await _show_product_list_adm(cb, cat, subcat, page)
    await cb.answer()


@router.callback_query(F.data.startswith("adm:prod:detail:"))
async def cb_adm_prod_detail(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    pid = int(cb.data.split(":")[3])
    db = config.DB
    r = await db.execute_fetchall(
        "SELECT name, base_price, unit, category, subcategory FROM products WHERE id=?", (pid,)
    )
    if not r:
        await cb.answer("Товар не найден.", show_alert=True)
        return
    name, base_price, unit, cat, subcat = r[0]
    ind = await db.execute_fetchall("""
        SELECT u.full_name, ip.price FROM individual_prices ip
        JOIN users u ON ip.user_id = u.tg_id WHERE ip.product_id = ?
    """, (pid,))
    price_str = f"{base_price} ₽/{unit}" if base_price > 0 else "по запросу"
    text = f"📦 <b>{name}</b>\n💰 Базовая цена: {price_str}"
    if ind:
        text += "\n\n👤 <b>Инд. цены:</b>"
        for i in ind:
            text += f"\n  • {i[0]}: {i[1]} ₽"
    back_cb = _back_cb_for_product(cat, subcat)
    await cb.message.edit_text(text, reply_markup=product_detail_adm_kb(pid, back_cb), parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data.startswith("adm:prod:price:"))
async def cb_adm_prod_price(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    pid = int(cb.data.split(":")[3])
    db = config.DB
    r = await db.execute_fetchall("SELECT name, base_price, unit FROM products WHERE id=?", (pid,))
    if not r:
        await cb.answer("Товар не найден.", show_alert=True)
        return
    await state.update_data(edit_pid=pid)
    await state.set_state(AdminStates.entering_base_price)
    await cb.message.edit_text(
        f"📦 <b>{r[0][0]}</b>\nТекущая цена: {r[0][1]} ₽/{r[0][2]}\n\n"
        f"Введите новую цену (0 = по запросу):",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(AdminStates.entering_base_price)
async def on_base_price(message: Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
        assert price >= 0
    except (ValueError, AssertionError):
        await message.answer("Введите число (0 или больше):")
        return
    d = await state.get_data()
    pid = d["edit_pid"]
    db = config.DB
    await db.execute("UPDATE products SET base_price=? WHERE id=?", (price, pid))
    await db.commit()
    r = await db.execute_fetchall("SELECT name, unit FROM products WHERE id=?", (pid,))
    await state.clear()
    price_str = f"{price} ₽/{r[0][1]}" if price > 0 else "по запросу"
    await message.answer(
        f"✅ Цена обновлена: <b>{r[0][0]}</b> — {price_str}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К товару", callback_data=f"adm:prod:detail:{pid}")],
            [InlineKeyboardButton(text="◀️ Товары", callback_data="adm:products")],
        ]),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("adm:prod:del:"))
async def cb_adm_prod_del(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    pid = int(cb.data.split(":")[3])
    db = config.DB
    r = await db.execute_fetchall("SELECT category, subcategory, name FROM products WHERE id=?", (pid,))
    if not r:
        await cb.answer("Товар не найден.", show_alert=True)
        return
    cat, subcat, name = r[0]
    await db.execute("DELETE FROM individual_prices WHERE product_id=?", (pid,))
    await db.execute("DELETE FROM products WHERE id=?", (pid,))
    await db.commit()
    await cb.answer(f"«{name}» удалён.", show_alert=True)
    await _show_product_list_adm(cb, cat, subcat)


@router.callback_query(F.data.startswith("adm:prod:add:"))
async def cb_adm_prod_add(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    cat = parts[3]
    subcat = parts[4] if parts[4] != "none" else None
    await state.update_data(new_prod_cat=cat, new_prod_subcat=subcat)
    await state.set_state(AdminStates.adding_product_name)
    await cb.message.edit_text(
        "➕ <b>Добавление товара</b>\n\nШаг 1/3 — Введите название:",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(AdminStates.adding_product_name)
async def on_add_product_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        await message.answer("Название не может быть пустым:")
        return
    await state.update_data(new_prod_name=name)
    await state.set_state(None)
    await message.answer(
        f"Шаг 2/3 — Выберите единицу измерения для <b>{name}</b>:",
        reply_markup=unit_select_kb(),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("adm:prod:unit:"))
async def cb_adm_prod_unit(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    unit = cb.data.split(":")[3]
    d = await state.get_data()
    if "new_prod_name" not in d:
        await cb.answer("Сессия истекла. Начните заново.", show_alert=True)
        await state.clear()
        return
    await state.update_data(new_prod_unit=unit)
    await state.set_state(AdminStates.adding_product_price)
    await cb.message.edit_text(
        f"Шаг 3/3 — Введите цену за <b>{unit}</b>\n(0 = цена по запросу):",
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(AdminStates.adding_product_price)
async def on_add_product_price(message: Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
        assert price >= 0
    except (ValueError, AssertionError):
        await message.answer("Введите число (0 или больше):")
        return

    d = await state.get_data()
    cat = d["new_prod_cat"]
    subcat = d.get("new_prod_subcat")
    name = d["new_prod_name"]
    unit = d["new_prod_unit"]

    db = config.DB
    sort_rows = await db.execute_fetchall(
        "SELECT COALESCE(MAX(sort_order), 0) FROM products WHERE category=?", (cat,)
    )
    sort_order = sort_rows[0][0] + 1

    await db.execute(
        "INSERT INTO products(category, subcategory, name, unit, base_price, sort_order) VALUES(?,?,?,?,?,?)",
        (cat, subcat, name, unit, price, sort_order),
    )
    await db.commit()
    await state.clear()

    price_str = f"{price} ₽/{unit}" if price > 0 else "по запросу"
    back_cb = _back_cb_for_product(cat, subcat)
    await message.answer(
        f"✅ Товар добавлен!\n\n📦 <b>{name}</b>\n💰 {price_str}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К списку", callback_data=back_cb)],
            [InlineKeyboardButton(text="◀️ Товары", callback_data="adm:products")],
        ]),
        parse_mode="HTML",
    )


# ═══════════════════════════════════════════════════════
#  ИНДИВИДУАЛЬНЫЕ ЦЕНЫ
# ═══════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("indp:"))
async def cb_ind_price_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    pid = int(cb.data.split(":")[1])
    db = config.DB
    users = await db.execute_fetchall(
        "SELECT tg_id, username, full_name, phone FROM users ORDER BY full_name"
    )
    if not users:
        await cb.answer("Клиентов нет.", show_alert=True)
        return
    await cb.message.edit_text("👤 Выберите клиента:", reply_markup=users_page_kb(users, pid, 0))
    await cb.answer()


@router.callback_query(F.data.startswith("upage:"))
async def cb_upage(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    pid, page = int(parts[1]), int(parts[2])
    db = config.DB
    users = await db.execute_fetchall(
        "SELECT tg_id, username, full_name, phone FROM users ORDER BY full_name"
    )
    await cb.message.edit_text("👤 Выберите клиента:", reply_markup=users_page_kb(users, pid, page))
    await cb.answer()


@router.callback_query(F.data.startswith("indpu:"))
async def cb_ind_price_enter(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    pid, uid = int(parts[1]), int(parts[2])
    db = config.DB
    prod = await db.execute_fetchall("SELECT name, base_price, unit FROM products WHERE id=?", (pid,))
    user = await db.execute_fetchall("SELECT full_name FROM users WHERE tg_id=?", (uid,))
    cur = await db.execute_fetchall(
        "SELECT price FROM individual_prices WHERE user_id=? AND product_id=?", (uid, pid)
    )
    cur_text = f"{cur[0][0]} ₽" if cur else "нет"
    await state.update_data(edit_pid=pid, edit_uid=uid)
    await state.set_state(AdminStates.entering_ind_price)
    await cb.message.edit_text(
        f"📦 {prod[0][0]} (база: {prod[0][1]} ₽/{prod[0][2]})\n"
        f"👤 {user[0][0] if user else uid}\n"
        f"Текущая инд. цена: {cur_text}\n\n"
        f"Введите новую инд. цену (0 = удалить):"
    )
    await cb.answer()


@router.message(AdminStates.entering_ind_price)
async def on_ind_price(message: Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
        assert price >= 0
    except (ValueError, AssertionError):
        await message.answer("Введите число (0 = удалить):")
        return
    d = await state.get_data()
    pid, uid = d["edit_pid"], d["edit_uid"]
    db = config.DB
    if price == 0:
        await db.execute("DELETE FROM individual_prices WHERE user_id=? AND product_id=?", (uid, pid))
        await db.commit()
        await state.clear()
        await message.answer(
            "🗑 Инд. цена удалена.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ К товару", callback_data=f"adm:prod:detail:{pid}")],
                [InlineKeyboardButton(text="◀️ Товары", callback_data="adm:products")],
            ]),
        )
    else:
        await db.execute("""
            INSERT INTO individual_prices (user_id, product_id, price) VALUES (?,?,?)
            ON CONFLICT(user_id, product_id) DO UPDATE SET price=excluded.price
        """, (uid, pid, price))
        await db.commit()
        prod = await db.execute_fetchall("SELECT name FROM products WHERE id=?", (pid,))
        user = await db.execute_fetchall("SELECT full_name FROM users WHERE tg_id=?", (uid,))
        await state.clear()
        await message.answer(
            f"✅ {user[0][0] if user else uid}: {prod[0][0]} = {price} ₽",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ К товару", callback_data=f"adm:prod:detail:{pid}")],
                [InlineKeyboardButton(text="◀️ Товары", callback_data="adm:products")],
            ]),
        )


@router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()


# ═══════════════════════════════════════════════════════
#  ПЛАНИРОВЩИК
# ═══════════════════════════════════════════════════════

async def run_scheduler(bot_instance: Bot):
    while True:
        now = datetime.now()
        try:
            db = config.DB
            if db is None:
                await asyncio.sleep(60)
                continue

            if now.day == 1 and now.hour == 9 and now.minute == 0:
                text = await build_report()
                for cid in set(ADMIN_IDS + NOTIFY_IDS):
                    try:
                        await bot_instance.send_message(cid, text, parse_mode=None)
                    except Exception:
                        pass

            if now.hour == 10 and now.minute == 0:
                from datetime import timedelta
                threshold = (now - timedelta(days=INACTIVE_DAYS)).isoformat()
                users = await db.execute_fetchall(
                    "SELECT tg_id FROM users WHERE last_order_at IS NOT NULL AND last_order_at < ?",
                    (threshold,),
                )
                for u in users:
                    try:
                        await bot_instance.send_message(
                            u[0],
                            "👋 Давно не заказывали лёд! Пора пополнить запасы?",
                            reply_markup=repeat_order_kb(),
                        )
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"Scheduler: {e}")
        await asyncio.sleep(60)

