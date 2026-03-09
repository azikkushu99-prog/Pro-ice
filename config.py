import os

BOT_TOKEN = "8658572130:AAFgjaODd-AFRU6XfjJYkWaPAPluyoKmCsE"
ADMIN_IDS = [785219206, 2043409859, 947457920]
NOTIFY_IDS = [785219206, 2043409859, 947457920]
INACTIVE_DAYS = int(os.getenv("INACTIVE_DAYS", "14"))
MIN_ORDER_AMOUNT = int(os.getenv("MIN_ORDER_AMOUNT", "0"))
DB_PATH = os.getenv("DB_PATH", "ice_bot.db")

# Shared DB connection — init in bot.py, used in admin.py
DB = None