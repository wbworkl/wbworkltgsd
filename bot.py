import os
import sys
import json
import uuid
import logging
import asyncio
import aiohttp
import html
import shutil
import random
import time
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
    from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
    from telegram.error import BadRequest
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Установите библиотеку: pip install python-telegram-bot aiohttp")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
TOKEN = "8605216745:AAGHALFssq5GJbDO00F8aTMDaH5fGt9cjeQ"
ADMIN_IDS = [8472956215, 8133517773]
BOT_OWNER_ID = 8472956215, 8133517773
ADMIN_USERNAME = "wbworkl"

# Реквизиты для пополнения через администратора
ADMIN_PAYMENT_DETAILS = {
    "card": "Реквизиты спрашивать у администратора",
    "bank": "",
    "phone": "",
    "name": ""
}

# Курс рубля к USDT
RUB_TO_USDT_RATE = 80.0

# CryptoBot API настройки
CRYPTO_BOT_TOKEN = "551696:AA2hVm9hWLWDqye8s0h0MzMU9ZyBJAuS6Pw"
CRYPTO_BOT_API_URL = "https://pay.crypt.bot/api"
CRYPTO_BOT_TEST_MODE = False

# Файлы данных
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

USERS_FILE = os.path.join(DATA_DIR, "users.json")
CATALOG_FILE = os.path.join(DATA_DIR, "catalog.json")
ORDERS_FILE = os.path.join(DATA_DIR, "orders.json")
INVOICES_FILE = os.path.join(DATA_DIR, "invoices.json")
ADMINS_FILE = os.path.join(DATA_DIR, "admins.json")
PURCHASES_FILE = os.path.join(DATA_DIR, "purchases.json")
PRODUCT_FILES_DIR = os.path.join(DATA_DIR, "product_files")
PROMOCODES_FILE = os.path.join(DATA_DIR, "promocodes.json")
REQUESTS_FILE = os.path.join(DATA_DIR, "requests.json")
PREORDERS_FILE = os.path.join(DATA_DIR, "preorders.json")
os.makedirs(PRODUCT_FILES_DIR, exist_ok=True)

# Глобальные переменные
users = {}
catalog = []
orders = []
invoices = {}
admins = []
purchases = {}
promocodes = {}
requests_dict = {}
preorders = []

# ========== КЭШИРОВАНИЕ ==========
_categories_cache = None
_categories_cache_time = 0
_subcategories_cache = {}
_types_cache = {}
_products_cache = {}
_available_products_cache = None
_available_products_cache_time = 0
CACHE_TTL = 30

def invalidate_caches():
    global _categories_cache, _categories_cache_time, _subcategories_cache, _types_cache, _products_cache
    global _available_products_cache, _available_products_cache_time
    _categories_cache = None
    _categories_cache_time = 0
    _subcategories_cache.clear()
    _types_cache.clear()
    _products_cache.clear()
    _available_products_cache = None
    _available_products_cache_time = 0

def is_cache_valid(cache_time: float) -> bool:
    return time.time() - cache_time < CACHE_TTL

# ========== ЭКРАНИРОВАНИЕ ДЛЯ MARKDOWN ==========
def escape_markdown(text: str) -> str:
    """Экранирует спецсимволы для Telegram Markdown (legacy)."""
    escape_chars = r'_*[]()~>#+-=|{}.!'
    return ''.join(f'\\{c}' if c in escape_chars else c for c in str(text))

# ========== CRYPTOBOT API ==========
class CryptoBotAPI:
    def __init__(self, token: str, test_mode: bool = True):
        self.token = token
        self.test_mode = test_mode
        self.base_url = CRYPTO_BOT_API_URL
        self.session = None
        self._request_cache = {}
        self._cache_time = 0

    async def create_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None):
        try:
            await self.create_session()
            url = f"{self.base_url}/{endpoint}"
            headers = {
                "Crypto-Pay-API-Token": self.token,
                "Content-Type": "application/json"
            }
            params = {}
            if self.test_mode:
                params['test'] = 'true'

            if method.upper() == "GET":
                async with self.session.get(url, headers=headers, params=params) as response:
                    return await response.json()
            elif method.upper() == "POST":
                async with self.session.post(url, headers=headers, json=data, params=params) as response:
                    return await response.json()
            else:
                raise ValueError(f"Неизвестный метод: {method}")
        except Exception as e:
            logger.error(f"Ошибка запроса к CryptoBot API: {e}")
            return {"ok": False, "error": str(e)}

    async def create_invoice(self, amount: float, description: str = "Пополнение баланса"):
        try:
            data = {
                "asset": "USDT",
                "amount": str(amount),
                "description": description,
                "hidden_message": "Спасибо за оплату!",
                "paid_btn_name": "openBot",
                "paid_btn_url": "https://t.me/your_bot",
                "payload": str(uuid.uuid4())[:16],
                "allow_comments": False,
                "allow_anonymous": False,
                "expires_in": 3600
            }
            result = await self._make_request("POST", "createInvoice", data)
            if result.get("ok") and result.get("result"):
                invoice = result["result"]
                return {
                    "invoice_id": invoice.get("invoice_id"),
                    "status": invoice.get("status"),
                    "hash": invoice.get("hash"),
                    "asset": invoice.get("asset"),
                    "amount": invoice.get("amount"),
                    "pay_url": invoice.get("pay_url"),
                    "description": invoice.get("description"),
                    "created_at": invoice.get("created_at"),
                    "payload": data["payload"]
                }
            else:
                logger.error(f"Ошибка создания счета: {result}")
                return None
        except Exception as e:
            logger.error(f"Ошибка создания счета: {e}")
            return None

    async def check_invoice_status(self, invoice_id: str):
        try:
            result = await self._make_request("GET", f"getInvoices?invoice_ids={invoice_id}")
            if result.get("ok") and result.get("result") and result["result"]["items"]:
                invoice = result["result"]["items"][0]
                return {
                    "invoice_id": invoice.get("invoice_id"),
                    "status": invoice.get("status"),
                    "paid_at": invoice.get("paid_at"),
                    "amount": invoice.get("amount"),
                    "asset": invoice.get("asset")
                }
            else:
                logger.error(f"Ошибка проверки счета: {result}")
                return None
        except Exception as e:
            logger.error(f"Ошибка проверки счета: {e}")
            return None

crypto_bot = CryptoBotAPI(CRYPTO_BOT_TOKEN, CRYPTO_BOT_TEST_MODE)

# ========== ЗАГРУЗКА И СОХРАНЕНИЕ ДАННЫХ ==========
def atomic_json_dump(data, filepath):
    temp_file = f"{filepath}.tmp"
    try:
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, filepath)
        return True
    except Exception as e:
        logger.error(f"Ошибка атомарной записи {filepath}: {e}")
        try:
            os.remove(temp_file)
        except:
            pass
        return False

def load_data():
    global users, catalog, orders, invoices, admins, purchases, promocodes, requests_dict, preorders
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                users = json.load(f)
            logger.info(f"Загружено {len(users)} пользователей")
        else:
            users = {}
            save_users()

        if os.path.exists(CATALOG_FILE):
            with open(CATALOG_FILE, 'r', encoding='utf-8') as f:
                catalog = json.load(f)
            logger.info(f"Загружено {len(catalog)} товаров")
        else:
            catalog = []
            save_catalog()

        if os.path.exists(ORDERS_FILE):
            with open(ORDERS_FILE, 'r', encoding='utf-8') as f:
                orders = json.load(f)
            logger.info(f"Загружено {len(orders)} заказов")
        else:
            orders = []
            save_orders()

        if os.path.exists(INVOICES_FILE):
            with open(INVOICES_FILE, 'r', encoding='utf-8') as f:
                invoices = json.load(f)
            logger.info(f"Загружено {len(invoices)} счетов")
        else:
            invoices = {}
            save_invoices()

        if os.path.exists(ADMINS_FILE):
            with open(ADMINS_FILE, 'r', encoding='utf-8') as f:
                loaded_admins = json.load(f)
                if BOT_OWNER_ID not in loaded_admins:
                    loaded_admins.append(BOT_OWNER_ID)
                admins = loaded_admins
            logger.info(f"Загружено {len(admins)} администраторов")
        else:
            admins = [BOT_OWNER_ID]
            save_admins()

        if os.path.exists(PURCHASES_FILE):
            with open(PURCHASES_FILE, 'r', encoding='utf-8') as f:
                purchases = json.load(f)
            logger.info(f"Загружено данных о покупках для {len(purchases)} пользователей")
        else:
            purchases = {}
            save_purchases()

        if os.path.exists(PROMOCODES_FILE):
            with open(PROMOCODES_FILE, 'r', encoding='utf-8') as f:
                promocodes = json.load(f)
            logger.info(f"Загружено {len(promocodes)} промокодов")
        else:
            promocodes = {}
            save_promocodes()

        if os.path.exists(REQUESTS_FILE):
            with open(REQUESTS_FILE, 'r', encoding='utf-8') as f:
                requests_dict = json.load(f)
            logger.info(f"Загружено {len(requests_dict)} заявок на пополнение")
        else:
            requests_dict = {}
            save_requests()

        if os.path.exists(PREORDERS_FILE):
            with open(PREORDERS_FILE, 'r', encoding='utf-8') as f:
                preorders = json.load(f)
            logger.info(f"Загружено {len(preorders)} предзаказов")
        else:
            preorders = []
            save_preorders()

        invalidate_caches()
    except Exception as e:
        logger.error(f"Ошибка загрузки данных: {e}")
        users = {}
        catalog = []
        orders = []
        invoices = {}
        admins = [BOT_OWNER_ID]
        purchases = {}
        promocodes = {}
        requests_dict = {}
        preorders = []

def save_users():
    atomic_json_dump(users, USERS_FILE)

def save_catalog():
    atomic_json_dump(catalog, CATALOG_FILE)
    invalidate_caches()

def save_orders():
    atomic_json_dump(orders, ORDERS_FILE)

def save_invoices():
    atomic_json_dump(invoices, INVOICES_FILE)

def save_admins():
    atomic_json_dump(admins, ADMINS_FILE)

def save_purchases():
    atomic_json_dump(purchases, PURCHASES_FILE)

def save_promocodes():
    atomic_json_dump(promocodes, PROMOCODES_FILE)

def save_requests():
    atomic_json_dump(requests_dict, REQUESTS_FILE)

def save_preorders():
    atomic_json_dump(preorders, PREORDERS_FILE)

def clean_username(username: str) -> str:
    """Очищает username от лишних символов, но сохраняет подчёркивания."""
    if not username:
        return "unknown"
    # Убираем @ в начале, если есть, и некоторые спецсимволы, но подчёркивание оставляем
    username = username.lstrip('@')
    # Удаляем нежелательные символы, но оставляем буквы, цифры, подчёркивание и дефис
    allowed = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-')
    cleaned = ''.join(c for c in username if c in allowed)
    return cleaned if cleaned else "unknown"

def ensure_user_registered(user_id, user_data=None):
    user_id_str = str(user_id)
    if user_id_str not in users:
        if user_data:
            users[user_id_str] = user_data
        else:
            users[user_id_str] = {
                "id": user_id,
                "username": "unknown",
                "first_name": "User",
                "balance_usdt": 0.0,
                "joined": datetime.now().isoformat(),
                "orders": [],
                "files_purchased": [],
                "crypto_invoices": []
            }
        save_users()
    return users[user_id_str]

def update_user_info(user_id: int, username: str = None, first_name: str = None):
    user_id_str = str(user_id)
    if user_id_str not in users:
        ensure_user_registered(user_id)
    user_data = users[user_id_str]
    if username is not None:
        # Сохраняем оригинальный username (очищенный от @, но с подчёркиваниями)
        user_data['username'] = clean_username(username)
    if first_name is not None:
        # Имя тоже экранируем при выводе, но сохраняем как есть
        user_data['first_name'] = first_name
    save_users()
    return user_data

# ========== ПОКУПКИ ==========
def add_user_purchase(user_id: int, purchase_data: dict):
    user_id_str = str(user_id)
    if user_id_str not in purchases:
        purchases[user_id_str] = []
    purchase_id = str(uuid.uuid4())[:8]
    purchase_data["purchase_id"] = purchase_id
    purchase_data["purchased_at"] = datetime.now().isoformat()
    purchases[user_id_str].append(purchase_data)
    save_purchases()
    return purchase_id

def get_user_purchases(user_id: int):
    user_id_str = str(user_id)
    if user_id_str not in purchases:
        return []
    user_purchases = purchases[user_id_str]
    user_purchases.sort(key=lambda x: x.get("purchased_at", ""), reverse=True)
    return user_purchases

def get_user_purchase_by_id(user_id: int, purchase_id: str):
    user_purchases = get_user_purchases(user_id)
    for purchase in user_purchases:
        if purchase.get("purchase_id") == purchase_id:
            return purchase
    return None

def get_purchase_file_path(purchase_data: dict):
    if "file_path" in purchase_data:
        return purchase_data["file_path"]
    elif "bundle_files" in purchase_data:
        bundle_files = purchase_data.get("bundle_files", [])
        if bundle_files and len(bundle_files) > 0:
            return bundle_files[0].get("path")
    return None

# ========== АДМИНИСТРАТОРЫ ==========
def is_admin(user_id: int) -> bool:
    return user_id in admins

def is_bot_owner(user_id: int) -> bool:
    return user_id == BOT_OWNER_ID

def add_admin(user_id: int) -> bool:
    if user_id not in admins:
        admins.append(user_id)
        save_admins()
        return True
    return False

def remove_admin(user_id: int) -> bool:
    if user_id in admins and user_id != BOT_OWNER_ID:
        admins.remove(user_id)
        save_admins()
        return True
    return False

# ========== ФУНКЦИИ ДЛЯ ТОВАРОВ ==========
def create_placeholder_product(category: str, subcategory: str = "", type_: str = "") -> dict:
    return {
        "id": f"placeholder_{uuid.uuid4().hex[:8]}",
        "name": f"__placeholder__{category}_{subcategory}_{type_}",
        "price": 0.0,
        "description": "Служебная запись",
        "category": category,
        "subcategory": subcategory,
        "type": type_,
        "has_file": False,
        "is_bundle": False,
        "quantity": 0,
        "created_at": datetime.now().isoformat(),
        "sold": False,
        "is_placeholder": True
    }

def is_product_available(product) -> bool:
    if product.get('is_placeholder', False):
        return False
    if product.get('sold', False):
        return False
    if not product.get('is_bundle', False):
        quantity = product.get('quantity', 1)
        if quantity <= 0:
            return False
        if product.get('has_file'):
            if product.get('file_path') is None:
                return False
            return True
    if product.get('is_bundle', False):
        bundle_files = product.get('bundle_files', [])
        if not bundle_files or len(bundle_files) == 0:
            return False
        available_count = get_available_files_count(bundle_files)
        if available_count <= 0:
            return False
    return True

def get_available_products():
    global _available_products_cache, _available_products_cache_time
    if _available_products_cache is not None and is_cache_valid(_available_products_cache_time):
        return _available_products_cache
    available = [p for p in catalog if is_product_available(p)]
    _available_products_cache = available
    _available_products_cache_time = time.time()
    return available

def get_unique_product_name(base_name: str, existing_names: Set[str]) -> str:
    if base_name not in existing_names:
        return base_name
    counter = 1
    while True:
        new_name = f"{base_name} ({counter})"
        if new_name not in existing_names:
            return new_name
        counter += 1

def get_random_files_from_bundle(bundle_files: List[Dict], count: int) -> List[Dict]:
    available_files = [f for f in bundle_files if not f.get('sold', False)]
    if not available_files:
        return []
    if count >= len(available_files):
        return available_files.copy()
    files_copy = available_files.copy()
    random.shuffle(files_copy)
    return files_copy[:count]

def mark_files_as_sold(bundle_files: List[Dict], files_to_sell: List[Dict]):
    sold_file_names = {f['name'] for f in files_to_sell}
    for file_info in bundle_files:
        if file_info['name'] in sold_file_names:
            file_info['sold'] = True
            file_info['sold_at'] = datetime.now().isoformat()

def get_available_files_count(bundle_files: List[Dict]) -> int:
    if not bundle_files:
        return 0
    available_count = 0
    for file_info in bundle_files:
        if not file_info.get('sold', False):
            available_count += 1
    return available_count

def get_categories() -> List[str]:
    global _categories_cache, _categories_cache_time
    if _categories_cache is not None and is_cache_valid(_categories_cache_time):
        return _categories_cache
    categories = set()
    for product in catalog:
        if product.get('category'):
            categories.add(product['category'])
    result = sorted(categories)
    _categories_cache = result
    _categories_cache_time = time.time()
    return result

def get_subcategories(category: str) -> List[str]:
    cache_key = f"subcats_{category}"
    if cache_key in _subcategories_cache:
        cached_data, cache_time = _subcategories_cache[cache_key]
        if is_cache_valid(cache_time):
            return cached_data
    subcategories = set()
    for product in catalog:
        if product.get('category') == category and product.get('subcategory'):
            subcategories.add(product['subcategory'])
    result = sorted(subcategories)
    _subcategories_cache[cache_key] = (result, time.time())
    return result

def get_types(category: str, subcategory: str = None) -> List[str]:
    cache_key = f"types_{category}_{subcategory if subcategory else 'None'}"
    if cache_key in _types_cache:
        cached_data, cache_time = _types_cache[cache_key]
        if is_cache_valid(cache_time):
            return cached_data
    types = set()
    for product in catalog:
        if product.get('category') == category:
            if subcategory is not None and product.get('subcategory') != subcategory:
                continue
            if product.get('type'):
                types.add(product['type'])
    result = sorted(types)
    _types_cache[cache_key] = (result, time.time())
    return result

def get_products_by_path(category: str = None, subcategory: str = None, type_: str = None) -> List[Dict]:
    cache_key = f"products_{category}_{subcategory if subcategory else 'None'}_{type_ if type_ else 'None'}"
    if cache_key in _products_cache:
        cached_data, cache_time = _products_cache[cache_key]
        if is_cache_valid(cache_time):
            return cached_data
    filtered_products = []
    for product in catalog:
        if category and product.get('category') != category:
            continue
        if subcategory and product.get('subcategory') != subcategory:
            continue
        if type_ and product.get('type') != type_:
            continue
        filtered_products.append(product)
    _products_cache[cache_key] = (filtered_products, time.time())
    return filtered_products

def generate_unique_bundle_name(base_name: str) -> str:
    existing_names = {p['name'] for p in catalog if 'bundle' in p['name'].lower()}
    if base_name not in existing_names:
        return base_name
    counter = 1
    while True:
        new_name = f"{base_name} #{counter}"
        if new_name not in existing_names:
            return new_name
        counter += 1

def get_category_types_keyboard(category: str):
    types = get_types(category, None)
    keyboard = []
    for type_ in types:
        keyboard.append([InlineKeyboardButton(f"📝 {type_}", callback_data=f"type_no_subcat_{category}_{type_}_")])
    has_no_type = any(
        p for p in get_available_products()
        if p.get('category') == category and not p.get('type')
    )
    if has_no_type:
        keyboard.append([InlineKeyboardButton("📦 Без типа", callback_data=f"category_{category}")])
    keyboard.append([InlineKeyboardButton("🔙 К категориям", callback_data="back_to_categories")])
    keyboard.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(keyboard)

# ========== INLINE КЛАВИАТУРЫ ==========
def get_main_inline_keyboard(user_id: int = None):
    """Главное меню – убраны лишние кнопки"""
    keyboard = [
        [InlineKeyboardButton("🛍️ Каталог", callback_data="catalog_menu")],
        [InlineKeyboardButton("👤 Личный кабинет", callback_data="personal_account")],
        [InlineKeyboardButton("💳 Пополнить USDT", callback_data="deposit_menu")],
        [InlineKeyboardButton("📊 Доступные товары", callback_data="available_products")],
        [InlineKeyboardButton("🆘 Поддержка", callback_data="support")]
    ]
    if user_id and is_admin(user_id):
        keyboard.append([InlineKeyboardButton("👑 Админ-панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)

def get_personal_account_keyboard():
    """Клавиатура личного кабинета с перенесёнными кнопками"""
    keyboard = [
        [InlineKeyboardButton("📋 Мои счета", callback_data="my_invoices")],
        [InlineKeyboardButton("📦 Мои покупки", callback_data="my_purchases")],
        [InlineKeyboardButton("📦 Мои предзаказы", callback_data="my_preorders")],
        [InlineKeyboardButton("🎟️ Промокод", callback_data="promo_code")],
        [InlineKeyboardButton("💳 Пополнить баланс", callback_data="deposit_menu")],
        [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_admin_keyboard():
    keyboard = [
        [InlineKeyboardButton("➕ Создать товар", callback_data="admin_create_product")],
        [InlineKeyboardButton("📁 Прикрепить файл к товару", callback_data="admin_attach_file")],
        [InlineKeyboardButton("📂 Управление категориями", callback_data="admin_manage_categories")],
        [InlineKeyboardButton("📦 Массовая загрузка в тип", callback_data="admin_bulk_upload_to_type")],
        [InlineKeyboardButton("📈 Пополнить количество", callback_data="admin_restock")],
        [InlineKeyboardButton("🗑️ Удалить товар", callback_data="admin_delete_product")],
        [InlineKeyboardButton("💰 Пополнить баланс (админ)", callback_data="admin_add_balance")],
        [InlineKeyboardButton("💰 Списать баланс (админ)", callback_data="admin_remove_balance")],
        [InlineKeyboardButton("📊 CryptoBot Статистика", callback_data="admin_crypto_stats")],
        [InlineKeyboardButton("👥 Управление администраторами", callback_data="admin_manage_admins")],
        [InlineKeyboardButton("🎟️ Управление промокодами", callback_data="admin_manage_promocodes")],
        [InlineKeyboardButton("📋 Заявки на пополнение", callback_data="admin_requests_list")],
        [InlineKeyboardButton("📦 Предзаказы", callback_data="admin_preorders_list")],
        [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_catalog_categories_keyboard():
    categories = get_categories()
    keyboard = []
    for category in categories:
        subcategories = get_subcategories(category)
        if subcategories:
            keyboard.append([InlineKeyboardButton(f"📁 {category} ▶", callback_data=f"cat_{category}")])
        else:
            types = get_types(category, None)
            if types:
                keyboard.append([InlineKeyboardButton(f"📁 {category} ▶", callback_data=f"cat_{category}")])
            else:
                keyboard.append([InlineKeyboardButton(f"📦 {category}", callback_data=f"category_{category}")])
    keyboard.append([InlineKeyboardButton("📦 Все товары", callback_data="category_all")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(keyboard)

def get_subcategories_keyboard(category: str):
    subcategories = get_subcategories(category)
    keyboard = []
    for subcategory in subcategories:
        types = get_types(category, subcategory)
        if types:
            keyboard.append([InlineKeyboardButton(f"📂 {subcategory} ▶", callback_data=f"subcat_{category}_{subcategory}")])
        else:
            keyboard.append([InlineKeyboardButton(f"📦 {subcategory}", callback_data=f"subcategory_{category}_{subcategory}")])
    has_no_subcategory = any(
        p for p in get_available_products()
        if p.get('category') == category and not p.get('subcategory')
    )
    if has_no_subcategory:
        keyboard.append([InlineKeyboardButton("📦 Без подкатегории", callback_data=f"category_{category}")])
    category_types = get_types(category, None)
    if category_types:
        keyboard.append([InlineKeyboardButton("📝 Типы в категории", callback_data=f"category_types_{category}")])
    keyboard.append([InlineKeyboardButton("🔙 К категориям", callback_data="back_to_categories")])
    keyboard.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(keyboard)

def get_types_keyboard(category: str, subcategory: str):
    types = get_types(category, subcategory)
    keyboard = []
    for type_ in types:
        keyboard.append([InlineKeyboardButton(f"📝 {type_}", callback_data=f"type_{category}_{subcategory}_{type_}")])
    has_no_type = any(
        p for p in get_available_products()
        if p.get('category') == category and p.get('subcategory') == subcategory and not p.get('type')
    )
    if has_no_type:
        keyboard.append([InlineKeyboardButton("📦 Без типа", callback_data=f"subcategory_{category}_{subcategory}")])
    keyboard.append([InlineKeyboardButton("🔙 К подкатегориям", callback_data=f"cat_{category}")])
    keyboard.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(keyboard)

def get_products_by_path_keyboard(category=None, subcategory=None, type_=None):
    filtered_products = get_products_by_path(category, subcategory, type_)
    available_products = [p for p in filtered_products if is_product_available(p)]
    keyboard = []
    for product in available_products:
        emoji = "📦" if product.get('is_bundle', False) else "📁" if product.get('has_file') else "🛍️"
        quantity = product.get('quantity', 1)
        if product.get('is_bundle', False):
            available_files = get_available_files_count(product.get('bundle_files', []))
            btn_text = f"{emoji} {product['name']} - {product['price']} USDT/шт. ({available_files} доступно)"
        elif quantity == 1 and product.get('has_file'):
            btn_text = f"{emoji} {product['name']} - {product['price']} USDT"
        else:
            btn_text = f"{emoji} {product['name']} - {product['price']} USDT ({quantity} шт.)"
        if product.get('has_file') and product.get('file_path'):
            btn_text += " 📎"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"product:{product['id']}")])
    if not keyboard:
        keyboard.append([InlineKeyboardButton("📭 Нет доступных товаров", callback_data="no_products")])
    back_buttons = []
    if type_:
        if subcategory:
            back_buttons.append(InlineKeyboardButton("🔙 К типам", callback_data=f"subcat_{category}_{subcategory}"))
        else:
            back_buttons.append(InlineKeyboardButton("🔙 К типам", callback_data=f"category_types_{category}"))
    elif subcategory:
        back_buttons.append(InlineKeyboardButton("🔙 К подкатегориям", callback_data=f"cat_{category}"))
    elif category and category != "all":
        back_buttons.append(InlineKeyboardButton("🔙 К категориям", callback_data="back_to_categories"))
    else:
        back_buttons.append(InlineKeyboardButton("🔙 К категориям", callback_data="back_to_categories"))
    back_buttons.append(InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu"))
    if len(back_buttons) > 1:
        keyboard.append([back_buttons[0], back_buttons[1]])
    else:
        keyboard.append(back_buttons)
    return InlineKeyboardMarkup(keyboard)

def get_payment_methods_keyboard():
    keyboard = [
        [InlineKeyboardButton("💎 Криптовалюта (CryptoBot)", callback_data="deposit_crypto")],
        [InlineKeyboardButton("💳 Через администратора", callback_data="deposit_admin")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_deposit_crypto_keyboard():
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="deposit_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_back_to_admin_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")]
    ])

def get_back_to_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
    ])

def get_quantity_selection_keyboard(product_id: str, max_quantity: int):
    keyboard = []
    quick_choices = [1, 2, 3, 5, 10]
    row = []
    for qty in quick_choices:
        if qty <= max_quantity:
            row.append(InlineKeyboardButton(str(qty), callback_data=f"buy_qty:{product_id}:{qty}"))
    if row:
        keyboard.append(row)
    if max_quantity > 10:
        keyboard.append([InlineKeyboardButton("🔢 Ввести своё количество", callback_data=f"custom_qty:{product_id}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"product_back:{product_id}")])
    keyboard.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(keyboard)

def get_insufficient_balance_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Пополнить баланс", callback_data="deposit_menu")],
        [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
    ])

def get_admin_management_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить администратора", callback_data="admin_add_admin")],
        [InlineKeyboardButton("➖ Удалить администратора", callback_data="admin_remove_admin")],
        [InlineKeyboardButton("👥 Список администраторов", callback_data="admin_list_admins")],
        [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")]
    ])

def get_category_management_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Создать новую категорию", callback_data="admin_add_category")],
        [InlineKeyboardButton("📂 Создать подкатегорию в категорию", callback_data="admin_add_subcategory")],
        [InlineKeyboardButton("📝 Создать тип в категории (без подкатегории)", callback_data="admin_add_type_to_category")],
        [InlineKeyboardButton("📝 Создать тип в подкатегории", callback_data="admin_add_type_to_subcategory")],
        [InlineKeyboardButton("📋 Просмотреть структуру каталога", callback_data="admin_view_structure")],
        [InlineKeyboardButton("🗑️ Удалить категорию", callback_data="admin_delete_category")],
        [InlineKeyboardButton("🗑️ Удалить подкатегорию", callback_data="admin_delete_subcategory")],
        [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")],
        [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
    ])

def get_confirm_delete_category_keyboard(category: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Да, удалить категорию", callback_data=f"final_delete_category:{category}"),
            InlineKeyboardButton("❌ Нет, отменить", callback_data="admin_manage_categories")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_categories")]
    ])

def get_confirm_delete_subcategory_keyboard(category: str, subcategory: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Да, удалить подкатегорию", callback_data=f"final_delete_subcategory:{category}:{subcategory}"),
            InlineKeyboardButton("❌ Нет, отменить", callback_data="admin_manage_categories")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data=f"select_category_for_delete_subcat:{category}")]
    ])

def get_back_to_categories_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 К управлению категориями", callback_data="admin_manage_categories")]
    ])

def get_my_purchases_keyboard(page: int = 0, total_pages: int = 1, purchase_id: str = None):
    keyboard = []
    if purchase_id:
        keyboard.append([InlineKeyboardButton("📥 Скачать файл", callback_data=f"download_purchase:{purchase_id}")])
        keyboard.append([InlineKeyboardButton("🔙 К моим покупкам", callback_data="my_purchases")])
    else:
        if total_pages > 1:
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"purchases_page:{page-1}"))
            nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
            if page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"purchases_page:{page+1}"))
            keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(keyboard)

def get_purchase_detail_keyboard(purchase_id: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Скачать файл", callback_data=f"download_purchase:{purchase_id}")],
        [InlineKeyboardButton("🔙 К моим покупкам", callback_data="my_purchases")]
    ])

# ========== КЛАВИАТУРЫ ДЛЯ ПРОМОКОДОВ ==========
def get_admin_promocodes_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Создать денежный промокод", callback_data="admin_create_balance_promo")],
        [InlineKeyboardButton("➕ Создать товарный промокод", callback_data="admin_create_item_promo")],
        [InlineKeyboardButton("📋 Список промокодов", callback_data="admin_list_promocodes")],
        [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")]
    ])

def get_promocode_cancel_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Отмена", callback_data="back_to_menu")]
    ])

# ========== КЛАВИАТУРЫ ДЛЯ ЗАЯВОК ==========
def get_admin_requests_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_requests_list")],
        [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")]
    ])

def get_back_to_requests_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 К заявкам", callback_data="admin_requests_list")]
    ])

def get_confirm_request_keyboard(request_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_request:{request_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_request:{request_id}")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_requests_list")]
    ])

# ========== КЛАВИАТУРЫ ДЛЯ ПРЕДЗАКАЗОВ ==========
def get_admin_preorders_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin_preorders_list")],
        [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")]
    ])

def get_back_to_preorders_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 К предзаказам", callback_data="admin_preorders_list")]
    ])

def get_confirm_preorder_keyboard(preorder_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_preorder:{preorder_id}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"reject_preorder:{preorder_id}")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_preorders_list")]
    ])

def get_preorder_data_input_keyboard(product_id: str, quantity: int):
    """Клавиатура для ввода данных предзаказа после выбора количества"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data=f"product:{product_id}")]
    ])

# ========== КОМАНДЫ ПОЛЬЗОВАТЕЛЯ ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    states_to_clear = [
        'awaiting_usdt_amount', 'awaiting_product_data', 'awaiting_file_for_product',
        'awaiting_admin_add_balance', 'awaiting_addbalance_self', 'awaiting_addbalance_other',
        'awaiting_restock', 'awaiting_bulk_upload_params', 'awaiting_category_name',
        'awaiting_subcategory_name', 'awaiting_type_name', 'awaiting_delete_category',
        'awaiting_custom_quantity', 'awaiting_admin_id', 'awaiting_type_to_category',
        'awaiting_promo_code', 'awaiting_rub_amount', 'awaiting_request_id',
        'awaiting_preorder_quantity', 'awaiting_preorder_data', 'awaiting_admin_remove_balance'
    ]
    for key in states_to_clear:
        if key in context.user_data:
            del context.user_data[key]

    user = update.effective_user
    user_id = user.id
    update_user_info(user_id=user_id, username=user.username, first_name=user.first_name)
    user_data = ensure_user_registered(user_id)

    available_products = get_available_products()
    file_products = [p for p in available_products if p.get('has_file')]
    regular_products = [p for p in available_products if not p.get('has_file')]
    bundle_products = [p for p in available_products if p.get('is_bundle', False)]

    safe_first_name = escape_markdown(user_data.get('first_name', 'Пользователь'))
    safe_username = escape_markdown(user_data.get('username', 'unknown'))
    welcome_text = (
        f"👋 **Привет, {safe_first_name}!**\n\n"
        f"💰 **Ваш баланс:** {user_data.get('balance_usdt', 0.0):.2f} USDT\n"
        f"🛍️ **Доступно товаров:** {len(available_products)}\n"
        f"📦 **Наборов с файлами:** {len(bundle_products)}\n"
        f"📁 **Отдельных файлов:** {len(file_products) - len(bundle_products)}\n"
        f"📦 **Обычных:** {len(regular_products)}\n\n"
        "💎 **Пополнение через CryptoBot:**\n"
        "• Только USDT (TRC20)\n"
        "• Мгновенное зачисление\n\n"
        "💳 **Пополнение через администратора:**\n"
        "• Введите сумму в рублях, она конвертируется в USDT по курсу 80 руб = 1 USDT\n"
        "• После создания заявки свяжитесь с администратором для оплаты\n\n"
        "🎯 **Выберите действие:**"
    )
    if is_admin(user_id):
        welcome_text += f"\n👑 **Вы администратор!**"

    if update.message:
        await update.message.reply_text(
            welcome_text,
            parse_mode='Markdown',
            reply_markup=get_main_inline_keyboard(user_id)
        )
    elif update.callback_query:
        query = update.callback_query
        await safe_edit_message_text(
            query=query,
            text=welcome_text,
            parse_mode='Markdown',
            reply_markup=get_main_inline_keyboard(user_id)
        )

async def catalog_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    available_products = get_available_products()
    if not available_products:
        response_text = "🛒 **Каталог пуст**\n\nАдминистратор скоро добавит товары."
        if update.message:
            await update.message.reply_text(
                response_text,
                parse_mode='Markdown',
                reply_markup=get_back_to_menu_keyboard()
            )
        else:
            query = update.callback_query
            await safe_edit_message_text(
                query=query,
                text=response_text,
                parse_mode='Markdown',
                reply_markup=get_back_to_menu_keyboard()
            )
        return

    categories = get_categories()
    categories_text = "📂 **Выберите категорию товаров:**\n\n"
    for i, category in enumerate(categories, 1):
        category_products = [p for p in available_products if p.get('category') == category]
        bundle_count = len([p for p in category_products if p.get('is_bundle', False)])
        file_count = len([p for p in category_products if p.get('has_file') and not p.get('is_bundle', False)])
        regular_count = len([p for p in category_products if not p.get('has_file')])
        categories_text += f"{i}. **{escape_markdown(category)}**\n"
        categories_text += f"   📦 Товаров: {len(category_products)}\n"
        categories_text += f"   📦 Наборов: {bundle_count}\n"
        categories_text += f"   📁 Файловых: {file_count}\n"
        categories_text += f"   🛍️ Обычных: {regular_count}\n\n"
    categories_text += "🛒 **Выберите категорию:**"

    if update.message:
        await update.message.reply_text(
            categories_text,
            parse_mode='Markdown',
            reply_markup=get_catalog_categories_keyboard()
        )
    elif update.callback_query:
        query = update.callback_query
        await safe_edit_message_text(
            query=query,
            text=categories_text,
            parse_mode='Markdown',
            reply_markup=get_catalog_categories_keyboard()
        )

async def catalog_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    await catalog_handler(update, context)

async def catalog_category_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category_name: str):
    query = update.callback_query
    await safe_answer_query(query)
    if category_name == "all":
        filtered_products = get_available_products()
        category_title = "Все товары"
    else:
        filtered_products = [p for p in get_available_products() if p.get('category') == category_name]
        category_title = category_name
    if not filtered_products:
        await safe_edit_message_text(
            query=query,
            text=f"📭 **В категории '{escape_markdown(category_title)}' нет доступных товаров.**",
            parse_mode='Markdown',
            reply_markup=get_catalog_categories_keyboard()
        )
        return
    catalog_text = f"🛍️ **Каталог: {escape_markdown(category_title)}**\n\n"
    for i, product in enumerate(filtered_products, 1):
        emoji = "📦" if product.get('is_bundle', False) else "📁" if product.get('has_file') else "🛍️"
        quantity = product.get('quantity', 1)
        catalog_text += f"{i}. {emoji} **{escape_markdown(product['name'])}**\n"
        catalog_text += f"   💰 **Цена:** {product['price']} USDT"
        if product.get('is_bundle', False):
            available_files = get_available_files_count(product.get('bundle_files', []))
            catalog_text += f" за штуку\n"
            catalog_text += f"   📦 **Доступно файлов:** {available_files}\n"
        else:
            catalog_text += f"\n"
            catalog_text += f"   📦 **В наличии:** {quantity} шт.\n"
        if product.get('description'):
            desc = escape_markdown(product['description'][:50] + "..." if len(product['description']) > 50 else product['description'])
            catalog_text += f"   📝 {desc}\n"
        catalog_text += "\n"
    catalog_text += "🛒 **Выберите товар для покупки:**"
    await safe_edit_message_text(
        query=query,
        text=catalog_text,
        parse_mode='Markdown',
        reply_markup=get_products_by_path_keyboard(category_name if category_name != "all" else None)
    )

async def category_types_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    await safe_answer_query(query)
    types = get_types(category, None)
    if not types:
        await safe_edit_message_text(
            query=query,
            text=f"📭 **В категории '{escape_markdown(category)}' нет типов.**\n\nПереходим к товарам без типа...",
            parse_mode='Markdown',
            reply_markup=get_subcategories_keyboard(category)
        )
        return
    catalog_text = f"📁 **Категория: {escape_markdown(category)}**\n\n"
    catalog_text += "📝 **Выберите тип товаров:**\n\n"
    types_with_counts = []
    for type_ in types:
        count = len([
            p for p in get_available_products()
            if p.get('category') == category and p.get('type') == type_ and not p.get('subcategory')
        ])
        types_with_counts.append((type_, count))
    types_with_counts.sort(key=lambda x: x[1], reverse=True)
    for i, (type_, count) in enumerate(types_with_counts, 1):
        catalog_text += f"{i}. **{escape_markdown(type_)}** - {count} товар(ов)\n"
    catalog_text += "\n📦 **Без типа** - для просмотра товаров без типа"
    await safe_edit_message_text(
        query=query,
        text=catalog_text,
        parse_mode='Markdown',
        reply_markup=get_category_types_keyboard(category)
    )

async def subcategory_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    await safe_answer_query(query)
    subcategories = get_subcategories(category)
    if not subcategories:
        category_types = get_types(category, None)
        if category_types:
            await category_types_handler(update, context, category)
        else:
            await catalog_category_handler(update, context, category)
        return
    catalog_text = f"📂 **Категория: {escape_markdown(category)}**\n\n"
    catalog_text += "📁 **Выберите подкатегорию:**\n\n"
    subcategories_with_counts = []
    for subcat in subcategories:
        count = len([
            p for p in get_available_products()
            if p.get('category') == category and p.get('subcategory') == subcat
        ])
        subcategories_with_counts.append((subcat, count))
    subcategories_with_counts.sort(key=lambda x: x[1], reverse=True)
    for i, (subcat, count) in enumerate(subcategories_with_counts, 1):
        catalog_text += f"{i}. **{escape_markdown(subcat)}** - {count} товар(ов)\n"
    catalog_text += "\n📦 **Без подкатегории** - для просмотра товаров без подкатегории"
    category_types = get_types(category, None)
    if category_types:
        catalog_text += "\n\n📝 **Типы в категории** - для просмотра типов без подкатегории"
    await safe_edit_message_text(
        query=query,
        text=catalog_text,
        parse_mode='Markdown',
        reply_markup=get_subcategories_keyboard(category)
    )

async def catalog_subcategory_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str, subcategory: str):
    query = update.callback_query
    await safe_answer_query(query)
    types = get_types(category, subcategory)
    if not types:
        filtered_products = get_products_by_path(category, subcategory)
        available_products = [p for p in filtered_products if is_product_available(p)]
        if not available_products:
            await safe_edit_message_text(
                query=query,
                text=f"📭 **В подкатегории '{escape_markdown(subcategory)}' нет доступных товаров.**",
                parse_mode='Markdown',
                reply_markup=get_subcategories_keyboard(category)
            )
            return
        catalog_text = f"🛍️ **Категория: {escape_markdown(category)}**\n"
        catalog_text += f"📂 **Подкатегория: {escape_markdown(subcategory)}**\n\n"
        for i, product in enumerate(available_products, 1):
            emoji = "📦" if product.get('is_bundle', False) else "📁" if product.get('has_file') else "🛍️"
            quantity = product.get('quantity', 1)
            catalog_text += f"{i}. {emoji} **{escape_markdown(product['name'])}**\n"
            catalog_text += f"   💰 **Цена:** {product['price']} USDT"
            if product.get('is_bundle', False):
                available_files = get_available_files_count(product.get('bundle_files', []))
                catalog_text += f" за штуку\n"
                catalog_text += f"   📦 **Доступно файлов:** {available_files}\n"
            else:
                catalog_text += f"\n"
                catalog_text += f"   📦 **В наличии:** {quantity} шт.\n"
            if product.get('description'):
                desc = escape_markdown(product['description'][:50] + "..." if len(product['description']) > 50 else product['description'])
                catalog_text += f"   📝 {desc}\n"
            catalog_text += "\n"
        catalog_text += "🛒 **Выберите товар для покупки:**"
        await safe_edit_message_text(
            query=query,
            text=catalog_text,
            parse_mode='Markdown',
            reply_markup=get_products_by_path_keyboard(category, subcategory)
        )
    else:
        catalog_text = f"📂 **Категория: {escape_markdown(category)}**\n"
        catalog_text += f"📁 **Подкатегория: {escape_markdown(subcategory)}**\n\n"
        catalog_text += "📝 **Выберите тип товаров:**\n\n"
        types_with_counts = []
        for type_ in types:
            count = len([
                p for p in get_available_products()
                if p.get('category') == category and p.get('subcategory') == subcategory and p.get('type') == type_
            ])
            types_with_counts.append((type_, count))
        types_with_counts.sort(key=lambda x: x[1], reverse=True)
        for i, (type_, count) in enumerate(types_with_counts, 1):
            catalog_text += f"{i}. **{escape_markdown(type_)}** - {count} товар(ов)\n"
        catalog_text += "\n📦 **Без типа** - для просмотра товаров без типа"
        await safe_edit_message_text(
            query=query,
            text=catalog_text,
            parse_mode='Markdown',
            reply_markup=get_types_keyboard(category, subcategory)
        )

async def catalog_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str, subcategory: str, type_: str):
    query = update.callback_query
    await safe_answer_query(query)
    filtered_products = get_products_by_path(category, subcategory, type_)
    available_products = [p for p in filtered_products if is_product_available(p)]
    if not available_products:
        await safe_edit_message_text(
            query=query,
            text=f"📭 **В типе '{escape_markdown(type_)}' нет доступных товаров.**",
            parse_mode='Markdown',
            reply_markup=get_types_keyboard(category, subcategory)
        )
        return
    catalog_text = f"🛍️ **Категория: {escape_markdown(category)}**\n"
    catalog_text += f"📂 **Подкатегория:** {escape_markdown(subcategory)}\n"
    catalog_text += f"📝 **Тип: {escape_markdown(type_)}**\n\n"
    for i, product in enumerate(available_products, 1):
        emoji = "📦" if product.get('is_bundle', False) else "📁" if product.get('has_file') else "🛍️"
        quantity = product.get('quantity', 1)
        catalog_text += f"{i}. {emoji} **{escape_markdown(product['name'])}**\n"
        catalog_text += f"   💰 **Цена:** {product['price']} USDT"
        if product.get('is_bundle', False):
            available_files = get_available_files_count(product.get('bundle_files', []))
            catalog_text += f" за штуку\n"
            catalog_text += f"   📦 **Доступно файлов:** {available_files}\n"
        else:
            catalog_text += f"\n"
            catalog_text += f"   📦 **В наличии:** {quantity} шт.\n"
        if product.get('description'):
            desc = escape_markdown(product['description'][:50] + "..." if len(product['description']) > 50 else product['description'])
            catalog_text += f"   📝 {desc}\n"
        catalog_text += "\n"
    catalog_text += "🛒 **Выберите товар для покупки:**"
    await safe_edit_message_text(
        query=query,
        text=catalog_text,
        parse_mode='Markdown',
        reply_markup=get_products_by_path_keyboard(category, subcategory, type_)
    )

async def catalog_type_no_subcategory_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str, type_: str):
    query = update.callback_query
    await safe_answer_query(query)
    filtered_products = get_products_by_path(category, None, type_)
    available_products = [p for p in filtered_products if is_product_available(p)]
    if not available_products:
        await safe_edit_message_text(
            query=query,
            text=f"📭 **В типе '{escape_markdown(type_)}' (без подкатегории) нет доступных товаров.**",
            parse_mode='Markdown',
            reply_markup=get_category_types_keyboard(category)
        )
        return
    catalog_text = f"🛍️ **Категория: {escape_markdown(category)}**\n"
    catalog_text += f"📝 **Тип: {escape_markdown(type_)}** (без подкатегории)\n\n"
    for i, product in enumerate(available_products, 1):
        emoji = "📦" if product.get('is_bundle', False) else "📁" if product.get('has_file') else "🛍️"
        quantity = product.get('quantity', 1)
        catalog_text += f"{i}. {emoji} **{escape_markdown(product['name'])}**\n"
        catalog_text += f"   💰 **Цена:** {product['price']} USDT"
        if product.get('is_bundle', False):
            available_files = get_available_files_count(product.get('bundle_files', []))
            catalog_text += f" за штуку\n"
            catalog_text += f"   📦 **Доступно файлов:** {available_files}\n"
        else:
            catalog_text += f"\n"
            catalog_text += f"   📦 **В наличии:** {quantity} шт.\n"
        if product.get('description'):
            desc = escape_markdown(product['description'][:50] + "..." if len(product['description']) > 50 else product['description'])
            catalog_text += f"   📝 {desc}\n"
        catalog_text += "\n"
    catalog_text += "🛒 **Выберите товар для покупки:**"
    await safe_edit_message_text(
        query=query,
        text=catalog_text,
        parse_mode='Markdown',
        reply_markup=get_products_by_path_keyboard(category, None, type_)
    )

async def personal_account_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user = query.from_user
    user_id = user.id
    update_user_info(user_id=user_id, username=user.username, first_name=user.first_name)
    user_data = ensure_user_registered(user_id)
    balance_usdt = user_data.get("balance_usdt", 0.0)
    user_orders = [o for o in orders if str(o.get('user_id')) == str(user_id)]
    orders_count = len(user_orders)
    user_invoices = [inv for inv in invoices.values() if str(inv.get('user_id')) == str(user_id)]
    active_invoices = len([inv for inv in user_invoices if inv.get('status') == 'active'])
    paid_invoices = len([inv for inv in user_invoices if inv.get('status') == 'paid'])
    purchased_files = user_data.get('files_purchased', [])
    user_purchases = get_user_purchases(user_id)
    purchases_count = len(user_purchases)
    safe_username = escape_markdown(user_data.get('username', 'не указан'))
    safe_first_name = escape_markdown(user_data.get('first_name', user.first_name))

    response_text = (
        f"👤 **Личный кабинет**\n\n"
        f"🆔 **Ваш ID:** `{user_id}`\n"
        f"👤 **Имя:** {safe_first_name}\n"
        f"📧 **Username:** @{safe_username}\n"
        f"📅 **Дата регистрации:** {user_data.get('joined', 'Неизвестно')[:10]}\n\n"
        f"💰 **Баланс:** {balance_usdt:.2f} USDT\n"
        f"📦 **Всего заказов:** {orders_count}\n"
        f"🛍️ **Мои покупки:** {purchases_count}\n"
        f"💎 **Крипто-платежи:**\n"
        f"   ⏳ Ожидает оплаты: {active_invoices}\n"
        f"   ✅ Оплачено: {paid_invoices}"
    )
    if is_admin(user_id):
        response_text += f"\n\n👑 **Вы администратор!**"

    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=get_personal_account_keyboard()
    )

async def available_products_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    available_products = get_available_products()
    file_products = [p for p in available_products if p.get('has_file')]
    regular_products = [p for p in available_products if not p.get('has_file')]
    bundle_products = [p for p in available_products if p.get('is_bundle', False)]

    if not available_products:
        response_text = "📭 **Нет доступных товаров**\n\nВсе товары распроданы."
    else:
        available_products.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        response_text = (
            f"📊 **Статистика товаров:**\n\n"
            f"🛍️ **Всего доступно:** {len(available_products)} шт.\n"
            f"📦 **Наборов файлов:** {len(bundle_products)} шт.\n"
            f"📁 **Отдельных файлов:** {len(file_products) - len(bundle_products)} шт.\n"
            f"📦 **Обычных товаров:** {len(regular_products)} шт.\n\n"
            f"🔥 **Самые свежие товары:**\n"
        )
        for i, product in enumerate(available_products[:10], 1):
            emoji = "📦" if product.get('is_bundle', False) else "📁" if product.get('has_file') else "🛍️"
            quantity = product.get('quantity', 1)
            response_text += f"{i}. {emoji} **{escape_markdown(product['name'])}**\n"
            response_text += f"   💰 **Цена:** {product['price']} USDT"
            if product.get('is_bundle', False):
                available_files = get_available_files_count(product.get('bundle_files', []))
                response_text += f" за штуку\n"
                response_text += f"   📦 **Доступно файлов:** {available_files}\n"
            else:
                response_text += f"\n"
                response_text += f"   📦 **В наличии:** {quantity} шт.\n"
            if product.get('description'):
                desc = escape_markdown(product['description'][:50] + "..." if len(product['description']) > 50 else product['description'])
                response_text += f"   📝 {desc}\n"
            response_text += "\n"
        if len(available_products) > 10:
            response_text += f"\n📈 *И еще {len(available_products) - 10} товаров...*\n"
    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=get_back_to_menu_keyboard()
    )

async def support_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    response_text = (
        "🆘 **Поддержка**\n\n"
        "По всем вопросам:\n\n"
        f"👑 **Администратор:** @{ADMIN_USERNAME}\n"
        "💎 **CryptoBot поддержка:** @CryptoBot\n\n"
        "🔄 **По проблемам с оплатой:**\n"
        "1. Сообщите номер счета\n"
        "2. Укажите ваш ID\n"
        "3. Приложите скриншот"
    )
    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=get_back_to_menu_keyboard()
    )

async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    response_text = f"🆔 **Ваш ID:** `{user_id}`"
    await update.message.reply_text(
        response_text,
        parse_mode='Markdown',
        reply_markup=get_back_to_menu_keyboard()
    )

async def invoices_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    user_invoices = [inv for inv in invoices.values() if str(inv.get('user_id')) == str(user_id)]

    if not user_invoices:
        response_text = "📊 **У вас нет счетов**\n\n💳 **Чтобы создать счет, нажмите '💳 Пополнить USDT'**"
        await safe_edit_message_text(
            query=query,
            text=response_text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Новый счет", callback_data="deposit_menu")],
                [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
            ])
        )
        return

    user_invoices.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    active_invoices = [inv for inv in user_invoices if inv.get('status') == 'active']
    paid_invoices = [inv for inv in user_invoices if inv.get('status') == 'paid']
    expired_invoices = [inv for inv in user_invoices if inv.get('status') == 'expired']

    response_text = "📊 **Ваши счета:**\n\n"
    if active_invoices:
        response_text += f"⏳ **Активные счета ({len(active_invoices)}):**\n"
        for i, invoice in enumerate(active_invoices[:3], 1):
            invoice_id = str(invoice.get('invoice_id', 'N/A'))
            response_text += f"{i}. 💰 **{invoice.get('amount', 0)} USDT** - ID: `{invoice_id}`\n\n"
    if paid_invoices:
        response_text += f"✅ **Оплаченные счета ({len(paid_invoices)}):**\n"
        for i, invoice in enumerate(paid_invoices[:3], 1):
            response_text += f"{i}. 💰 **{invoice.get('amount', 0)} USDT** - {invoice.get('paid_at', 'N/A')[:19]}\n"
    if expired_invoices:
        response_text += f"\n❌ **Истекшие счета ({len(expired_invoices)}):**\n"
        for i, invoice in enumerate(expired_invoices[:2], 1):
            response_text += f"{i}. 💰 **{invoice.get('amount', 0)} USDT**\n"

    keyboard_buttons = []
    if active_invoices:
        keyboard_buttons.append([InlineKeyboardButton("❌ Отменить активные счета", callback_data="cancel_invoices")])
    keyboard_buttons.append([InlineKeyboardButton("💳 Новый счет", callback_data="deposit_menu")])
    keyboard_buttons.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")])
    keyboard = InlineKeyboardMarkup(keyboard_buttons)

    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=keyboard
    )

# ========== МОИ ПОКУПКИ ==========
async def my_purchases_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    user_purchases = get_user_purchases(user_id)

    if not user_purchases:
        response_text = "🛍️ **У вас еще нет покупок**\n\nПосле покупки товаров они будут отображаться здесь.\nНажмите '🛍️ Каталог' чтобы начать покупки!"
        await safe_edit_message_text(
            query=query,
            text=response_text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🛍️ Каталог", callback_data="catalog_menu")],
                [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
            ])
        )
        return

    items_per_page = 5
    total_pages = (len(user_purchases) + items_per_page - 1) // items_per_page
    if page >= total_pages:
        page = total_pages - 1
    if page < 0:
        page = 0

    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_purchases = user_purchases[start_idx:end_idx]

    response_text = f"🛍️ **Мои покупки** (Страница {page + 1}/{total_pages})\n\n"
    response_text += f"📊 **Всего покупок:** {len(user_purchases)}\n\n"
    keyboard_buttons = []

    for i, purchase in enumerate(page_purchases, start_idx + 1):
        purchase_id = purchase.get("purchase_id", "N/A")[:8]
        product_name = escape_markdown(purchase.get("product_name", "Неизвестный товар"))
        purchase_date = purchase.get("purchased_at", "")
        try:
            if purchase_date:
                date_obj = datetime.fromisoformat(purchase_date)
                formatted_date = date_obj.strftime("%d.%m.%Y %H:%M")
            else:
                formatted_date = "Неизвестно"
        except:
            formatted_date = "Неизвестно"

        if purchase.get("is_bundle", False):
            emoji = "📦"
            item_text = f"{len(purchase.get('bundle_files', []))} файлов"
        elif purchase.get("has_file", False):
            emoji = "📁"
            item_text = "1 файл"
        else:
            emoji = "🛍️"
            item_text = "Товар"

        response_text += f"{i}. {emoji} **{product_name}**\n"
        response_text += f"   📅 **Дата:** {formatted_date}\n"
        response_text += f"   🆔 **ID:** `{purchase_id}`\n"
        response_text += f"   📦 **Тип:** {item_text}\n\n"

        keyboard_buttons.append([InlineKeyboardButton(
            f"{emoji} {product_name} ({formatted_date})",
            callback_data=f"show_purchase:{purchase.get('purchase_id')}"
        )])

    if total_pages > 1:
        response_text += f"📄 **Используйте кнопки ниже для навигации**\n"

    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"purchases_page:{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"purchases_page:{page+1}"))
        keyboard_buttons.append(nav_buttons)

    keyboard_buttons.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")])
    keyboard = InlineKeyboardMarkup(keyboard_buttons)

    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=keyboard
    )

async def show_purchase_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, purchase_id: str):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    purchase = get_user_purchase_by_id(user_id, purchase_id)

    if not purchase:
        await safe_edit_message_text(
            query=query,
            text="❌ **Покупка не найдена**\n\nЭта покупка не существует или была удалена.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К моим покупкам", callback_data="my_purchases")]
            ])
        )
        return

    product_name = escape_markdown(purchase.get("product_name", "Неизвестный товар"))
    purchase_date = purchase.get("purchased_at", "")
    price = purchase.get("price", 0)
    quantity = purchase.get("quantity", 1)

    try:
        if purchase_date:
            date_obj = datetime.fromisoformat(purchase_date)
            formatted_date = date_obj.strftime("%d.%m.%Y %H:%M")
        else:
            formatted_date = "Неизвестно"
    except:
        formatted_date = "Неизвестно"

    response_text = f"🛍️ **Детали покупки**\n\n"
    response_text += f"📦 **Товар:** {product_name}\n"
    response_text += f"💰 **Цена:** {price} USDT\n"
    response_text += f"📊 **Количество:** {quantity} шт.\n"
    response_text += f"💵 **Общая стоимость:** {price * quantity} USDT\n"
    response_text += f"📅 **Дата покупки:** {formatted_date}\n"
    response_text += f"🆔 **ID покупки:** `{purchase_id}`\n\n"

    if purchase.get("is_bundle", False):
        bundle_files = purchase.get("bundle_files", [])
        response_text += f"📦 **Набор файлов:** {len(bundle_files)} файлов\n"
        response_text += "📁 **Файлы в наборе:**\n"
        for i, file_info in enumerate(bundle_files, 1):
            file_name = escape_markdown(file_info.get("name", f"Файл {i}"))
            response_text += f"   {i}. {file_name}\n"
    elif purchase.get("has_file", False):
        file_name = escape_markdown(purchase.get("file_name", "Файл"))
        response_text += f"📁 **Файл:** {file_name}\n"

    response_text += "\nНажмите '📥 Скачать файл' чтобы получить файл."

    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=get_purchase_detail_keyboard(purchase_id)
    )

async def download_purchase_file(update: Update, context: ContextTypes.DEFAULT_TYPE, purchase_id: str):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    purchase = get_user_purchase_by_id(user_id, purchase_id)

    if not purchase:
        await safe_answer_query(query, "❌ Покупка не найдена", show_alert=True)
        return

    try:
        if purchase.get("is_bundle", False):
            bundle_files = purchase.get("bundle_files", [])
            if not bundle_files:
                await safe_answer_query(query, "❌ В наборе нет файлов", show_alert=True)
                return

            for i, file_info in enumerate(bundle_files, 1):
                file_path = file_info.get("path")
                file_name = file_info.get("name", f"Файл_{i}.txt")
                if not file_path or not os.path.exists(file_path):
                    logger.warning(f"Файл не найден: {file_path}")
                    continue
                try:
                    with open(file_path, 'rb') as f:
                        caption = f"📦 Файл {i}/{len(bundle_files)} из покупки: {purchase.get('product_name', 'Набор')}"
                        await context.bot.send_document(
                            chat_id=user_id,
                            document=f,
                            caption=caption,
                            filename=file_name
                        )
                        await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Ошибка отправки файла {file_name}: {e}")
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"❌ Ошибка отправки файла {file_name}: {str(e)[:100]}"
                    )
            await safe_answer_query(query, f"✅ Отправлено {len(bundle_files)} файлов!", show_alert=True)

        elif purchase.get("has_file", False) and not purchase.get("is_bundle", False):
            file_path = purchase.get("file_path")
            file_name = purchase.get("file_name", "Файл.txt")
            if not file_path or not os.path.exists(file_path):
                await safe_answer_query(query, "❌ Файл не найден", show_alert=True)
                return
            with open(file_path, 'rb') as f:
                await context.bot.send_document(
                    chat_id=user_id,
                    document=f,
                    caption=f"📦 Файл из покупки: {purchase.get('product_name', 'Товар')}",
                    filename=file_name
                )
            await safe_answer_query(query, "✅ Файл отправлен!", show_alert=True)
        else:
            await safe_answer_query(query, "❌ У этого товара нет файлов для скачивания", show_alert=True)

        await show_purchase_detail(update, context, purchase_id)
    except Exception as e:
        logger.error(f"Ошибка отправки файлов покупки: {e}")
        await safe_answer_query(query, f"❌ Ошибка отправки файлов: {str(e)[:100]}", show_alert=True)

# ========== ОПЛАТА ==========
async def deposit_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    if 'awaiting_usdt_amount' in context.user_data:
        del context.user_data['awaiting_usdt_amount']
    user_id = query.from_user.id
    ensure_user_registered(user_id)

    response_text = (
        "💳 **Выберите способ пополнения баланса:**\n\n"
        "💎 **Криптовалюта через CryptoBot:**\n"
        "• Только USDT (TRC20)\n"
        "• Мгновенное зачисление\n\n"
        "💳 **Через администратора:**\n"
        "• Перевод по реквизитам\n"
        "• Требует подтверждения\n"
        "• Курс: 80 руб = 1 USDT"
    )
    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=get_payment_methods_keyboard()
    )

async def deposit_admin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await deposit_admin_start_handler(update, context)

async def deposit_admin_start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    ensure_user_registered(user_id)

    await safe_edit_message_text(
        query=query,
        text="💳 **Пополнение через администратора**\n\n"
             "Введите сумму в **рублях** (целое число или десятичная дробь).\n"
             f"Курс: {RUB_TO_USDT_RATE} руб = 1 USDT\n\n"
             "После ввода будет создана заявка, которую администратор рассмотрит.\n"
             "Обязательно свяжитесь с администратором для получения реквизитов и укажите номер заявки.\n\n"
             "Или нажмите '🔙 Назад' для отмены.",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="deposit_menu")]
        ])
    )
    context.user_data['awaiting_rub_amount'] = True

async def process_rub_amount(update: Update, context: ContextTypes.DEFAULT_TYPE, rub_amount: float):
    user_id = update.effective_user.id
    user_data = ensure_user_registered(user_id)
    usdt_amount = rub_amount / RUB_TO_USDT_RATE
    request_id = str(uuid.uuid4())[:8]

    request_data = {
        "request_id": request_id,
        "user_id": user_id,
        "username": user_data.get('username', 'unknown'),
        "first_name": user_data.get('first_name', 'User'),
        "rub_amount": rub_amount,
        "usdt_amount": usdt_amount,
        "status": "pending",
        "created_at": datetime.now().isoformat(),
        "processed_at": None,
        "processed_by": None
    }
    requests_dict[request_id] = request_data
    save_requests()

    await update.message.reply_text(
        f"✅ **Заявка на пополнение создана!**\n\n"
        f"🆔 **Номер заявки:** `{request_id}`\n"
        f"💰 **Сумма в рублях:** {rub_amount:.2f} руб\n"
        f"💵 **Сумма в USDT:** {usdt_amount:.2f} USDT\n\n"
        f"📞 **Свяжитесь с администратором:** @{ADMIN_USERNAME}\n"
        f"📋 **Обязательно укажите номер заявки и сумму при обращении.**\n\n"
        f"После подтверждения администратором средства поступят на ваш баланс.",
        parse_mode='Markdown',
        reply_markup=get_main_inline_keyboard(user_id)
    )

    for admin_id in admins:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"📋 **Новая заявка на пополнение!**\n\n"
                     f"🆔 **Номер:** `{request_id}`\n"
                     f"👤 **Пользователь:** {escape_markdown(user_data.get('first_name'))} (@{escape_markdown(user_data.get('username'))})\n"
                     f"🆔 **ID:** `{user_id}`\n"
                     f"💰 **Сумма в рублях:** {rub_amount:.2f} руб\n"
                     f"💵 **Сумма в USDT:** {usdt_amount:.2f} USDT\n\n"
                     f"Для обработки заявки перейдите в админ-панель → Заявки на пополнение.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Перейти к заявкам", callback_data="admin_requests_list")]
                ])
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить админа {admin_id}: {e}")

    if 'awaiting_rub_amount' in context.user_data:
        del context.user_data['awaiting_rub_amount']

async def deposit_crypto_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    ensure_user_registered(user_id)

    response_text = (
        "💎 **Пополнение баланса USDT**\n\n"
        "💳 **Пополнение через CryptoBot:**\n"
        "• Только USDT (TRC20)\n"
        "• Мгновенное зачисление\n\n"
        "📝 **Введите сумму в USDT для пополнения:**\n"
        "Например: 10.5\n"
        "Минимум: 1 USDT\n"
        "Максимум: 1000 USDT\n\n"
        "Или нажмите '🔙 Назад' для отмены"
    )
    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=get_deposit_crypto_keyboard()
    )
    context.user_data['awaiting_usdt_amount'] = True

async def create_usdt_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE, amount: float):
    try:
        if hasattr(update, 'callback_query') and update.callback_query:
            query = update.callback_query
            user_id = query.from_user.id
            chat_id = query.message.chat_id
            is_callback = True
        else:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
            is_callback = False

        description = f"Пополнение баланса {amount} USDT для пользователя {user_id}"
        invoice_data = await crypto_bot.create_invoice(amount=amount, description=description)

        if not invoice_data:
            error_msg = "❌ **Ошибка создания счета!**\n\nПожалуйста, попробуйте позже."
            if is_callback:
                await safe_edit_message_text(
                    query=query,
                    text=error_msg,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Попробовать снова", callback_data="deposit_crypto")],
                        [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
                    ])
                )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=error_msg,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Попробовать снова", callback_data="deposit_crypto")],
                        [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
                    ])
                )
            return

        invoice_id = invoice_data['invoice_id']
        pay_url = invoice_data['pay_url']
        created_at = datetime.now().isoformat()
        expires_at = (datetime.now() + timedelta(hours=1)).isoformat()

        invoices[str(invoice_id)] = {
            "invoice_id": invoice_id,
            "user_id": user_id,
            "amount": amount,
            "currency": "USDT",
            "status": "active",
            "created_at": created_at,
            "expires_at": expires_at,
            "pay_url": pay_url,
            "description": description
        }
        save_invoices()

        user_data = ensure_user_registered(user_id)
        if "crypto_invoices" not in user_data:
            user_data["crypto_invoices"] = []
        user_data["crypto_invoices"].append({
            "invoice_id": invoice_id,
            "amount": amount,
            "currency": "USDT",
            "status": "pending",
            "created_at": created_at,
            "expires_at": expires_at
        })
        save_users()

        response_text = (
            f"✅ **Счет на сумму {amount} USDT создан!**\n\n"
            f"💳 **Для оплаты перейдите по ссылке:**\n"
            f"👉 {pay_url}\n\n"
            f"📋 **Детали счета:**\n"
            f"• Сумма: `{amount}` USDT\n"
            f"• ID счета: `{invoice_id}`\n"
            f"• Статус: `ожидает оплаты`\n"
            f"• Срок действия: 1 час"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Оплатить", url=pay_url)],
            [InlineKeyboardButton("❌ Отменить счет", callback_data=f"cancel_invoice:{invoice_id}")],
            [InlineKeyboardButton("📋 Мои счета", callback_data="my_invoices")],
            [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
        ])

        if is_callback:
            await safe_edit_message_text(
                query=query,
                text=response_text,
                parse_mode='Markdown',
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=response_text,
                parse_mode='Markdown',
                reply_markup=keyboard,
                disable_web_page_preview=True
            )

        asyncio.create_task(check_invoice_periodically(invoice_id, context))
    except Exception as e:
        logger.error(f"Ошибка при создании счета: {e}")
        error_msg = f"❌ **Ошибка:** {str(e)}"
        if hasattr(update, 'callback_query') and update.callback_query:
            await safe_edit_message_text(
                query=update.callback_query,
                text=error_msg,
                parse_mode='Markdown',
                reply_markup=get_main_inline_keyboard()
            )
        else:
            await update.message.reply_text(
                error_msg,
                parse_mode='Markdown',
                reply_markup=get_main_inline_keyboard()
            )

async def check_invoice_periodically(invoice_id: str, context: ContextTypes.DEFAULT_TYPE):
    try:
        max_checks = 360
        check_interval = 10
        for i in range(max_checks):
            await asyncio.sleep(check_interval)
            if str(invoice_id) not in invoices:
                return
            invoice = invoices[str(invoice_id)]
            expires_at_str = invoice.get('expires_at')
            if expires_at_str:
                try:
                    expires_at = datetime.fromisoformat(expires_at_str)
                    if datetime.now() > expires_at:
                        await process_expired_invoice(invoice_id, context)
                        return
                except ValueError:
                    pass
            status_data = await crypto_bot.check_invoice_status(invoice_id)
            if status_data:
                status = status_data.get("status")
                if status == "paid":
                    await process_paid_invoice(invoice_id, status_data, context)
                    return
                elif status == "expired":
                    await process_expired_invoice(invoice_id, context)
                    return
        if str(invoice_id) in invoices and invoices[str(invoice_id)]["status"] == "active":
            await process_expired_invoice(invoice_id, context)
    except Exception as e:
        logger.error(f"Ошибка при проверке счета {invoice_id}: {e}")

async def process_paid_invoice(invoice_id: str, status_data: dict, context: ContextTypes.DEFAULT_TYPE):
    try:
        if str(invoice_id) not in invoices:
            return
        invoice = invoices[str(invoice_id)]
        user_id = invoice["user_id"]
        amount = invoice["amount"]

        invoice["status"] = "paid"
        invoice["paid_at"] = datetime.now().isoformat()
        invoice["crypto_data"] = status_data
        save_invoices()

        user_data = ensure_user_registered(user_id)
        user_data["balance_usdt"] = user_data.get("balance_usdt", 0.0) + amount

        if "crypto_invoices" in user_data:
            for inv in user_data["crypto_invoices"]:
                if inv.get("invoice_id") == invoice_id:
                    inv["status"] = "paid"
                    inv["paid_at"] = datetime.now().isoformat()
                    break
        save_users()

        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"✅ **Оплата получена!**\n\n"
                    f"💰 **Зачислено:** {amount:.2f} USDT\n"
                    f"💵 **Текущий баланс:** {user_data['balance_usdt']:.2f} USDT\n\n"
                    f"🎉 **Спасибо за оплату!**"
                ),
                parse_mode='Markdown',
                reply_markup=get_main_inline_keyboard(user_id)
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")

        logger.info(f"Счет {invoice_id} оплачен. Пользователю {user_id} зачислено {amount} USDT")
    except Exception as e:
        logger.error(f"Ошибка обработки оплаченного счета {invoice_id}: {e}")

async def process_expired_invoice(invoice_id: str, context: ContextTypes.DEFAULT_TYPE):
    try:
        if str(invoice_id) in invoices:
            invoice = invoices[str(invoice_id)]
            user_id = invoice["user_id"]

            invoice["status"] = "expired"
            invoice["expired_at"] = datetime.now().isoformat()
            save_invoices()

            user_data = ensure_user_registered(user_id)
            if "crypto_invoices" in user_data:
                for inv in user_data["crypto_invoices"]:
                    if inv.get("invoice_id") == invoice_id:
                        inv["status"] = "expired"
                        inv["expired_at"] = datetime.now().isoformat()
                        break
                save_users()

            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"⏱ **Счет истек!**\n\n"
                        f"📋 **ID счета:** `{invoice_id}`\n"
                        f"💰 **Сумма:** {invoice['amount']} USDT\n\n"
                        f"💡 **Чтобы создать новый счет, нажмите '💳 Пополнить USDT'**"
                    ),
                    parse_mode='Markdown',
                    reply_markup=get_main_inline_keyboard(user_id)
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка обработки истекшего счета {invoice_id}: {e}")

# ========== ОТМЕНА СЧЕТОВ ==========
async def cancel_invoice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, invoice_id: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        pass
    user_id = query.from_user.id
    invoice_id_str = str(invoice_id)

    if invoice_id_str not in invoices:
        await safe_answer_query(query, "❌ Счет не найден", show_alert=True)
        return
    invoice = invoices[invoice_id_str]
    if str(invoice.get('user_id')) != str(user_id):
        await safe_answer_query(query, "❌ Этот счет вам не принадлежит", show_alert=True)
        return
    if invoice.get('status') != 'active':
        await safe_answer_query(query, f"❌ Нельзя отменить счет со статусом: {invoice.get('status')}", show_alert=True)
        return

    invoice["status"] = "cancelled"
    invoice["cancelled_at"] = datetime.now().isoformat()
    save_invoices()

    user_data = ensure_user_registered(user_id)
    if "crypto_invoices" in user_data:
        for inv in user_data["crypto_invoices"]:
            if inv.get("invoice_id") == invoice_id:
                inv["status"] = "cancelled"
                inv["cancelled_at"] = datetime.now().isoformat()
                break
        save_users()

    await safe_answer_query(query, "✅ Счет успешно отменен", show_alert=True)
    load_data()

    user_invoices = [inv for inv in invoices.values() if str(inv.get('user_id')) == str(user_id)]
    active_invoices = [inv for inv in user_invoices if inv.get('status') == 'active']

    if active_invoices:
        await cancel_invoices_list_handler(update, context)
    else:
        await safe_edit_message_text(
            query=query,
            text="✅ **Все счета отменены.**\n\n"
                "💡 **Вы можете создать новый счет через '💳 Пополнить USDT'**",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Новый счет", callback_data="deposit_menu")],
                [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
            ])
        )

async def cancel_invoices_list_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    load_data()
    user_invoices = [inv for inv in invoices.values() if str(inv.get('user_id')) == str(user_id)]
    active_invoices = [inv for inv in user_invoices if inv.get('status') == 'active']

    if not active_invoices:
        await safe_edit_message_text(
            query=query,
            text="📭 **Нет активных счетов для отмены**\n\nУ вас нет активных счетов, которые можно отменить.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Новый счет", callback_data="deposit_menu")],
                [InlineKeyboardButton("📋 Мои счета", callback_data="my_invoices")],
                [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
            ])
        )
        return

    response_text = "❌ **Отмена активных счетов**\n\n"
    response_text += f"У вас {len(active_invoices)} активных счетов:\n\n"
    for i, invoice in enumerate(active_invoices, 1):
        invoice_id = str(invoice.get('invoice_id', 'N/A'))
        created_at = invoice.get('created_at', 'N/A')
        try:
            expires_at = datetime.fromisoformat(invoice.get('expires_at', ''))
            time_left = expires_at - datetime.now()
            hours_left = time_left.total_seconds() // 3600
            minutes_left = (time_left.total_seconds() % 3600) // 60
            if time_left.total_seconds() > 0:
                time_left_str = f" (осталось: {int(hours_left)}ч {int(minutes_left)}м)"
            else:
                time_left_str = " (истек)"
        except:
            time_left_str = ""
        response_text += f"{i}. 💰 **{invoice.get('amount', 0)} USDT** - ID: `{invoice_id}`{time_left_str}\n"
        response_text += f"   📅 Создан: {created_at[:19]}\n\n"

    response_text += "⚠️ **Внимание:**\n"
    response_text += "• Отмена счета не возвращает деньги\n"
    response_text += "• Счет просто удаляется из системы\n\n"
    response_text += "Выберите счет для отмены:"

    keyboard_buttons = []
    for invoice in active_invoices:
        invoice_id = str(invoice.get('invoice_id'))
        amount = invoice.get('amount', 0)
        keyboard_buttons.append([
            InlineKeyboardButton(
                f"❌ Отменить счет на {amount} USDT",
                callback_data=f"cancel_invoice:{invoice_id}"
            )
        ])
    keyboard_buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="my_invoices")])
    keyboard = InlineKeyboardMarkup(keyboard_buttons)

    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=keyboard
    )

# ========== ПОКУПКА ТОВАРОВ ==========
async def show_product_details(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: str):
    query = update.callback_query
    await safe_answer_query(query)
    load_data()

    selected_product = None
    for product in catalog:
        if product['id'] == product_id:
            selected_product = product
            break
    if not selected_product:
        logger.error(f"Товар с ID {product_id} не найден")
        await safe_answer_query(query, "❌ Товар не найден", show_alert=True)
        return

    user_id = query.from_user.id
    user_data = ensure_user_registered(user_id)

    # Проверяем доступность товара
    product_available = is_product_available(selected_product)

    emoji = "📦" if selected_product.get('is_bundle', False) else "📁" if selected_product.get('has_file') else "🛍️"
    product_text = f"{emoji} **{escape_markdown(selected_product['name'])}**\n\n"
    product_text += f"💰 **Цена:** {selected_product['price']} USDT"

    if selected_product.get('is_bundle', False):
        available_files = get_available_files_count(selected_product.get('bundle_files', []))
        product_text += f" за штуку\n"
        product_text += f"📦 **Доступно файлов в наборе:** {available_files}\n"
        product_text += f"📊 **Всего файлов в наборе:** {len(selected_product.get('bundle_files', []))}\n"
    else:
        quantity = selected_product.get('quantity', 1)
        product_text += f"\n📦 **В наличии:** {quantity} шт.\n"

    if selected_product.get('description'):
        product_text += f"\n📝 **Описание:**\n{escape_markdown(selected_product['description'])}\n"

    user_balance = user_data.get('balance_usdt', 0.0)
    price_per_item = selected_product['price']

    product_text += f"\n👤 **Ваш баланс:** {user_balance:.2f} USDT"

    # Кнопки: купить (если доступно) и оформить предзаказ (всегда)
    keyboard = []

    # Если товар доступен для покупки, добавляем кнопку выбора количества
    if product_available:
        if selected_product.get('is_bundle', False):
            max_available = get_available_files_count(selected_product.get('bundle_files', []))
        else:
            max_available = selected_product.get('quantity', 1)
        
        if max_available > 0:
            if price_per_item > 0:
                max_affordable = int(user_balance // price_per_item)
            else:
                max_affordable = 0

            max_quantity = min(max_available, max_affordable)

            if max_quantity > 0:
                keyboard.append([InlineKeyboardButton("🛒 Купить", callback_data=f"buy_product:{product_id}")])
            else:
                product_text += f"\n\n⚠️ **Недостаточно средств для покупки!**"
        else:
            product_text += f"\n\n⚠️ **Товар временно отсутствует в наличии!**"
    else:
        product_text += f"\n\n⚠️ **Товар временно отсутствует в наличии, но можно оформить предзаказ.**"

    # Кнопка предзаказа всегда доступна
    keyboard.append([InlineKeyboardButton("📦 Оформить предзаказ", callback_data=f"preorder_product:{product_id}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_catalog")])
    keyboard.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")])

    await safe_edit_message_text(
        query=query,
        text=product_text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_quantity_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: str, quantity: int):
    query = update.callback_query
    await safe_answer_query(query)
    load_data()

    selected_product = None
    for product in catalog:
        if product['id'] == product_id:
            selected_product = product
            break
    if not selected_product:
        logger.error(f"Товар с ID {product_id} не найден")
        await safe_answer_query(query, "❌ Товар не найден", show_alert=True)
        return

    user_id = query.from_user.id
    user_data = ensure_user_registered(user_id)

    if not is_product_available(selected_product):
        logger.warning(f"Товар {selected_product['id']} недоступен")
        await safe_answer_query(query, "❌ Товар временно недоступен!", show_alert=True)
        return

    if selected_product.get('is_bundle', False):
        max_quantity = get_available_files_count(selected_product.get('bundle_files', []))
    else:
        max_quantity = selected_product.get('quantity', 1)

    if quantity <= 0:
        await safe_answer_query(query, "❌ Количество должно быть больше 0", show_alert=True)
        return
    if quantity > max_quantity:
        await safe_answer_query(query, f"❌ Максимальное доступное количество: {max_quantity}", show_alert=True)
        return

    total_price = selected_product['price'] * quantity
    balance = user_data.get('balance_usdt', 0.0)

    if balance < total_price:
        await safe_answer_query(query, f"❌ Недостаточно средств! Нужно: {total_price} USDT", show_alert=True)
        insufficient_text = (
            f"💰 **Недостаточно средств для покупки!**\n\n"
            f"🛍️ **Товар:** {escape_markdown(selected_product['name'])}\n"
            f"📦 **Количество:** {quantity} шт.\n"
            f"💰 **Цена за штуку:** {selected_product['price']} USDT\n"
            f"💵 **Общая стоимость:** {total_price} USDT\n"
            f"👤 **Ваш баланс:** {balance:.2f} USDT\n"
            f"📉 **Недостает:** {total_price - balance:.2f} USDT\n\n"
            f"💳 **Пожалуйста, пополните баланс для завершения покупки.**"
        )
        await safe_edit_message_text(
            query=query,
            text=insufficient_text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Пополнить баланс", callback_data="deposit_menu")],
                [InlineKeyboardButton("🔙 Назад", callback_data=f"product:{product_id}")]
            ])
        )
        return

    emoji = "📦" if selected_product.get('is_bundle', False) else "📁" if selected_product.get('has_file') else "🛍️"
    confirm_text = f"✅ **Подтверждение покупки**\n\n"
    confirm_text += f"{emoji} **Товар:** {escape_markdown(selected_product['name'])}\n"
    confirm_text += f"💰 **Цена за штуку:** {selected_product['price']} USDT\n"
    confirm_text += f"📊 **Количество:** {quantity} шт.\n"
    confirm_text += f"💵 **Общая стоимость:** {total_price} USDT\n"
    confirm_text += f"👤 **Ваш баланс:** {balance:.2f} USDT\n"
    confirm_text += f"💳 **Баланс после покупки:** {balance - total_price:.2f} USDT\n\n"
    confirm_text += "**Подтверждаете покупку?**"

    await safe_edit_message_text(
        query=query,
        text=confirm_text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить покупку", callback_data=f"confirm_purchase:{product_id}:{quantity}")],
            [InlineKeyboardButton("🔙 Назад", callback_data=f"product:{product_id}")]
        ])
    )

async def process_purchase(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: str, quantity: int):
    query = update.callback_query
    await safe_answer_query(query)
    load_data()

    selected_product = None
    for product in catalog:
        if product['id'] == product_id:
            selected_product = product
            break
    if not selected_product:
        logger.error(f"Товар с ID {product_id} не найден")
        await safe_answer_query(query, "❌ Товар не найден", show_alert=True)
        return

    user_id = query.from_user.id
    user_data = ensure_user_registered(user_id)

    if not is_product_available(selected_product):
        logger.warning(f"Товар {selected_product['id']} недоступен")
        await safe_answer_query(query, "❌ Товар временно недоступен!", show_alert=True)
        return

    if selected_product.get('is_bundle', False):
        max_quantity = get_available_files_count(selected_product.get('bundle_files', []))
    else:
        max_quantity = selected_product.get('quantity', 1)

    if quantity <= 0 or quantity > max_quantity:
        await safe_answer_query(query, "❌ Некорректное количество", show_alert=True)
        return

    total_price = selected_product['price'] * quantity
    balance = user_data.get('balance_usdt', 0.0)

    if balance < total_price:
        await safe_answer_query(query, f"❌ Недостаточно средств! Нужно: {total_price} USDT, у вас: {balance:.2f} USDT", show_alert=True)
        insufficient_text = (
            f"💰 **Недостаточно средств!**\n\n"
            f"🛍️ **Товар:** {escape_markdown(selected_product['name'])}\n"
            f"📦 **Количество:** {quantity} шт.\n"
            f"💵 **Общая стоимость:** {total_price} USDT\n"
            f"👤 **Ваш баланс:** {balance:.2f} USDT\n"
            f"📉 **Недостает:** {total_price - balance:.2f} USDT\n\n"
            f"💳 **Пожалуйста, пополните баланс для завершения покупки.**"
        )
        await safe_edit_message_text(
            query=query,
            text=insufficient_text,
            parse_mode='Markdown',
            reply_markup=get_insufficient_balance_keyboard()
        )
        return

    order_id = str(uuid.uuid4())[:8]

    if selected_product.get('is_bundle', False) and selected_product.get('bundle_files'):
        try:
            bundle_files = selected_product.get('bundle_files', [])
            available_files = [f for f in bundle_files if not f.get('sold', False)]
            if len(available_files) < quantity:
                await safe_answer_query(query, f"❌ Недостаточно доступных файлов. Доступно: {len(available_files)}", show_alert=True)
                return

            files_to_sell = get_random_files_from_bundle(available_files, quantity)
            mark_files_as_sold(bundle_files, files_to_sell)

            purchase_data = {
                "order_id": order_id,
                "product_id": selected_product['id'],
                "product_name": selected_product['name'],
                "price": selected_product['price'],
                "quantity": quantity,
                "total_price": total_price,
                "is_bundle": True,
                "has_file": True,
                "bundle_files": files_to_sell.copy(),
                "category": selected_product.get('category'),
                "subcategory": selected_product.get('subcategory'),
                "type": selected_product.get('type')
            }
            add_user_purchase(user_id, purchase_data)
            user_data['balance_usdt'] = balance - total_price
            save_catalog()
            save_orders()
            save_users()

            await safe_edit_message_text(
                query=query,
                text=f"🎉 **Покупка успешно завершена!**\n\n"
                    f"📦 **Набор:** {escape_markdown(selected_product['name'])}\n"
                    f"📁 **Куплено файлов:** {quantity}\n"
                    f"💰 **Цена за штуку:** {selected_product['price']} USDT\n"
                    f"💵 **Общая стоимость:** {total_price} USDT\n"
                    f"💳 **Остаток баланса:** {user_data['balance_usdt']:.2f} USDT\n"
                    f"📊 **Осталось доступных файлов:** {get_available_files_count(bundle_files)}\n\n"
                    f"📁 **Чтобы просмотреть купленные файлы, нажмите '📦 Мои покупки' в главном меню.**",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📦 Мои покупки", callback_data="my_purchases")],
                    [InlineKeyboardButton("🛍️ Каталог", callback_data="catalog_menu")],
                    [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
                ])
            )
            logger.info(f"Пользователь {user_id} купил {quantity} файлов из набора {selected_product['id']} за {total_price} USDT")
        except Exception as e:
            logger.error(f"Ошибка обработки набора товара: {e}")
            await safe_edit_message_text(
                query=query,
                text=f"❌ **Произошла ошибка при покупке:**\n\n`{str(e)}`",
                parse_mode='Markdown',
                reply_markup=get_main_inline_keyboard(user_id)
            )
        return

    elif selected_product.get('has_file') and selected_product.get('file_path'):
        try:
            if not os.path.exists(selected_product['file_path']):
                logger.error(f"Файл не найден: {selected_product['file_path']}")
                await safe_answer_query(query, "❌ Файл товара не найден на сервере", show_alert=True)
                return

            old_quantity = selected_product.get('quantity', 1)
            selected_product['quantity'] = old_quantity - quantity
            if selected_product['quantity'] <= 0:
                selected_product['sold'] = True

            order = {
                "order_id": order_id,
                "user_id": user_id,
                "product_id": selected_product['id'],
                "product_name": selected_product['name'],
                "price_usdt": total_price,
                "status": "completed",
                "created_at": datetime.now().isoformat(),
                "file_path": selected_product.get('file_path'),
                "file_name": selected_product.get('file_name'),
                "quantity": quantity,
                "is_bundle": False
            }
            orders.append(order)

            purchase_data = {
                "order_id": order_id,
                "product_id": selected_product['id'],
                "product_name": selected_product['name'],
                "price": selected_product['price'],
                "quantity": quantity,
                "total_price": total_price,
                "is_bundle": False,
                "has_file": True,
                "file_path": selected_product.get('file_path'),
                "file_name": selected_product.get('file_name'),
                "category": selected_product.get('category'),
                "subcategory": selected_product.get('subcategory'),
                "type": selected_product.get('type')
            }
            add_user_purchase(user_id, purchase_data)
            user_data['balance_usdt'] = balance - total_price
            if "orders" not in user_data:
                user_data["orders"] = []
            user_data['orders'].append(order_id)
            save_catalog()
            save_orders()
            save_users()

            await safe_edit_message_text(
                query=query,
                text=f"🎉 **Покупка успешно завершена!**\n\n"
                    f"🛍️ **Товар:** {escape_markdown(selected_product['name'])}\n"
                    f"💰 **Стоимость:** {total_price} USDT\n"
                    f"💳 **Остаток баланса:** {user_data['balance_usdt']:.2f} USDT\n"
                    f"📦 **Остаток товара:** {selected_product.get('quantity', 0)} шт.\n\n"
                    f"📁 **Чтобы скачать файл, нажмите '📦 Мои покупки' в главном меню.**",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📦 Мои покупки", callback_data="my_purchases")],
                    [InlineKeyboardButton("🛍️ Каталог", callback_data="catalog_menu")],
                    [InlineKeyboardButton("🔙 В главное меню", callback_data="back_to_menu")]
                ])
            )
            logger.info(f"Пользователь {user_id} купил товар {selected_product['id']} в количестве {quantity} шт. за {total_price} USDT")
        except Exception as e:
            logger.error(f"Ошибка обработки файлового товара: {e}")
            await safe_edit_message_text(
                query=query,
                text=f"❌ **Произошла ошибка при покупке:**\n\n`{str(e)}`",
                parse_mode='Markdown',
                reply_markup=get_main_inline_keyboard(user_id)
            )
        return

    else:
        old_quantity = selected_product.get('quantity', 10)
        selected_product['quantity'] = old_quantity - quantity
        if selected_product['quantity'] <= 0:
            selected_product['sold'] = True

        order = {
            "order_id": order_id,
            "user_id": user_id,
            "product_id": selected_product['id'],
            "product_name": selected_product['name'],
            "price_usdt": total_price,
            "status": "completed",
            "created_at": datetime.now().isoformat(),
            "quantity": quantity,
            "is_bundle": False
        }
        orders.append(order)

        purchase_data = {
            "order_id": order_id,
            "product_id": selected_product['id'],
            "product_name": selected_product['name'],
            "price": selected_product['price'],
            "quantity": quantity,
            "total_price": total_price,
            "is_bundle": False,
            "has_file": False,
            "category": selected_product.get('category'),
            "subcategory": selected_product.get('subcategory'),
            "type": selected_product.get('type')
        }
        add_user_purchase(user_id, purchase_data)
        user_data['balance_usdt'] = balance - total_price
        if "orders" not in user_data:
            user_data["orders"] = []
        user_data['orders'].append(order_id)
        save_catalog()
        save_orders()
        save_users()

        await safe_edit_message_text(
            query=query,
            text=f"✅ **Покупка совершена!**\n\n"
                f"🛍️ **Товар:** {escape_markdown(selected_product['name'])}\n"
                f"💰 **Стоимость:** {total_price} USDT\n"
                f"📋 **Номер заказа:** {order_id}\n"
                f"💳 **Остаток баланса:** {user_data['balance_usdt']:.2f} USDT\n"
                f"📦 **Остаток товара:** {selected_product.get('quantity', 0)} шт.\n\n"
                f"📞 **Администратор свяжется с вами в ближайшее время.**",
            parse_mode='Markdown',
            reply_markup=get_main_inline_keyboard(user_id)
        )
        logger.info(f"Пользователь {user_id} купил товар {selected_product['id']} в количестве {quantity} шт. за {total_price} USDT")

async def handle_product_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    data = query.data
    if not data.startswith("product:"):
        await safe_answer_query(query, "❌ Ошибка выбора товара", show_alert=True)
        return
    product_id = data.split(":", 1)[1]
    await show_product_details(update, context, product_id)

# ========== ПРОМОКОДЫ ==========
def generate_promo_code(length=8):
    chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
    return ''.join(random.choices(chars, k=length))

def get_available_item_products():
    return [p for p in catalog if p.get('has_file') and not p.get('is_bundle') and is_product_available(p)]

def get_promocodable_products():
    return [p for p in catalog if p.get('has_file') and is_product_available(p)]

async def activate_promocode(user_id: int, code: str, context: ContextTypes.DEFAULT_TYPE) -> Tuple[bool, str]:
    code_upper = code.upper()
    if code_upper not in promocodes:
        return False, "❌ Промокод не найден."
    promo = promocodes[code_upper]
    if not promo.get('active', True):
        return False, "❌ Промокод уже использован."

    # Проверка лимита
    max_uses = promo.get('max_uses', 0)
    used_count = promo.get('used_count', 0)
    if max_uses > 0 and used_count >= max_uses:
        return False, "❌ Промокод исчерпал лимит использований."

    user_data = ensure_user_registered(user_id)

    # Проверка, что пользователь ещё не активировал этот промокод
    if 'used_by' in promo and isinstance(promo['used_by'], list) and user_id in promo['used_by']:
        return False, "❌ Вы уже активировали этот промокод."

    if promo['type'] == 'balance':
        amount = promo['value']
        user_data['balance_usdt'] = user_data.get('balance_usdt', 0.0) + amount
        promo['used_count'] = used_count + 1
        if max_uses > 0 and promo['used_count'] >= max_uses:
            promo['active'] = False
        # Сохраняем историю использований
        if 'used_by' not in promo or not isinstance(promo['used_by'], list):
            promo['used_by'] = []
        if 'used_at' not in promo or not isinstance(promo['used_at'], list):
            promo['used_at'] = []
        promo['used_by'].append(user_id)
        promo['used_at'].append(datetime.now().isoformat())
        save_users()
        save_promocodes()
        return True, f"✅ Промокод активирован! Вам зачислено {amount} USDT."

    elif promo['type'] == 'item':
        product_id = promo['value']
        product = next((p for p in catalog if p['id'] == product_id), None)
        if not product or not is_product_available(product):
            return False, "❌ Товар по промокоду больше не доступен."

        if product.get('has_file') and product.get('file_path'):
            if not os.path.exists(product['file_path']):
                return False, "❌ Файл товара отсутствует на сервере."
            old_quantity = product.get('quantity', 1)
            product['quantity'] = old_quantity - 1
            if product['quantity'] <= 0:
                product['sold'] = True

            purchase_data = {
                "product_id": product['id'],
                "product_name": product['name'],
                "price": 0,
                "quantity": 1,
                "total_price": 0,
                "is_bundle": False,
                "has_file": True,
                "file_path": product.get('file_path'),
                "file_name": product.get('file_name'),
                "category": product.get('category'),
                "subcategory": product.get('subcategory'),
                "type": product.get('type')
            }
            add_user_purchase(user_id, purchase_data)

            promo['used_count'] = used_count + 1
            if max_uses > 0 and promo['used_count'] >= max_uses:
                promo['active'] = False
            if 'used_by' not in promo or not isinstance(promo['used_by'], list):
                promo['used_by'] = []
            if 'used_at' not in promo or not isinstance(promo['used_at'], list):
                promo['used_at'] = []
            promo['used_by'].append(user_id)
            promo['used_at'].append(datetime.now().isoformat())
            save_catalog()
            save_purchases()
            save_promocodes()

            try:
                with open(product['file_path'], 'rb') as f:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=f,
                        caption=f"🎁 Ваш товар по промокоду: {product['name']}",
                        filename=product.get('file_name', 'file.txt')
                    )
            except Exception as e:
                logger.error(f"Ошибка отправки файла по промокоду: {e}")
                return True, f"✅ Товар получен, но файл не удалось отправить. Обратитесь в поддержку."
            return True, f"✅ Промокод активирован! Товар **{escape_markdown(product['name'])}** отправлен."

        elif product.get('is_bundle'):
            bundle_files = product.get('bundle_files', [])
            available = [f for f in bundle_files if not f.get('sold')]
            if not available:
                return False, "❌ В наборе нет доступных файлов."
            file_to_give = random.choice(available)
            file_to_give['sold'] = True
            file_to_give['sold_at'] = datetime.now().isoformat()

            purchase_data = {
                "product_id": product['id'],
                "product_name": product['name'],
                "price": 0,
                "quantity": 1,
                "total_price": 0,
                "is_bundle": True,
                "has_file": True,
                "bundle_files": [file_to_give.copy()],
                "category": product.get('category'),
                "subcategory": product.get('subcategory'),
                "type": product.get('type')
            }
            add_user_purchase(user_id, purchase_data)

            promo['used_count'] = used_count + 1
            if max_uses > 0 and promo['used_count'] >= max_uses:
                promo['active'] = False
            if 'used_by' not in promo or not isinstance(promo['used_by'], list):
                promo['used_by'] = []
            if 'used_at' not in promo or not isinstance(promo['used_at'], list):
                promo['used_at'] = []
            promo['used_by'].append(user_id)
            promo['used_at'].append(datetime.now().isoformat())
            save_catalog()
            save_purchases()
            save_promocodes()

            try:
                with open(file_to_give['path'], 'rb') as f:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=f,
                        caption=f"🎁 Ваш файл из набора по промокоду: {product['name']}",
                        filename=file_to_give['name']
                    )
            except Exception as e:
                logger.error(f"Ошибка отправки файла из набора по промокоду: {e}")
                return True, f"✅ Файл получен, но не удалось отправить. Обратитесь в поддержку."
            return True, f"✅ Промокод активирован! Файл из набора **{escape_markdown(product['name'])}** отправлен."
        else:
            return False, "❌ Товар не поддерживает выдачу."
    return False, "❌ Неизвестный тип промокода."

async def promo_code_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    await safe_edit_message_text(
        query=query,
        text="🎟️ **Введите промокод**\n\n"
             "Напишите код промокода в сообщении.\n\n"
             "Или нажмите '🔙 Отмена' для возврата.",
        parse_mode='Markdown',
        reply_markup=get_promocode_cancel_keyboard()
    )
    context.user_data['awaiting_promo_code'] = True

# ========== АДМИН ФУНКЦИИ ==========
async def admin_panel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    total_products = len(catalog)
    file_products = len([p for p in catalog if p.get('has_file')])
    bundle_products = len([p for p in catalog if p.get('is_bundle', False)])
    sold_file_products = len([p for p in catalog if p.get('has_file') and p.get('sold', False)])
    active_file_products = len([p for p in catalog if p.get('has_file') and not p.get('sold', False)])

    total_bundle_files = 0
    available_bundle_files = 0
    for product in catalog:
        if product.get('is_bundle', False):
            bundle_files = product.get('bundle_files', [])
            total_bundle_files += len(bundle_files)
            available_bundle_files += get_available_files_count(bundle_files)

    pending_requests = sum(1 for req in requests_dict.values() if req.get('status') == 'pending')

    stats_text = (
        f"👑 **Панель администратора**\n\n"
        f"📊 **Статистика товаров:**\n"
        f"• Всего товаров: {total_products}\n"
        f"• Файловых товаров: {file_products}\n"
        f"• Наборов файлов: {bundle_products}\n"
        f"• Всего файлов в наборах: {total_bundle_files}\n"
        f"• Доступно файлов в наборах: {available_bundle_files}\n"
        f"• Продано файловых товаров: {sold_file_products}\n"
        f"• Активных файловых товаров: {active_file_products}\n\n"
        f"💎 **CryptoBot:**\n"
        f"• Активных счетов: {sum(1 for inv in invoices.values() if inv.get('status') == 'active')}\n"
        f"• Оплаченных счетов: {sum(1 for inv in invoices.values() if inv.get('status') == 'paid')}\n\n"
        f"📋 **Заявки на пополнение:** {pending_requests} ожидают\n"
        f"👥 **Администраторы:** {len(admins)} человек\n"
        f"🎟️ **Промокоды:** {len(promocodes)} всего, {sum(1 for p in promocodes.values() if p.get('active'))} активных"
    )

    await safe_edit_message_text(
        query=query,
        text=stats_text,
        parse_mode='Markdown',
        reply_markup=get_admin_keyboard()
    )

# ========== НОВАЯ АДМИН ФУНКЦИЯ: СПИСАНИЕ БАЛАНСА ==========
async def admin_remove_balance_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    await safe_edit_message_text(
        query=query,
        text="💰 **Списание баланса пользователя (Админ)**\n\n"
             "Введите данные в формате:\n"
             "`USER_ID СУММА_USDT`\n\n"
             "**Пример:**\n"
             "`8133517773 5.0`\n\n"
             "⚠️ **Внимание:** сумма будет списана с баланса пользователя.",
        parse_mode='Markdown',
        reply_markup=get_back_to_admin_keyboard()
    )
    context.user_data['awaiting_admin_remove_balance'] = True

async def process_admin_remove_balance(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    try:
        parts = text.split()
        if len(parts) != 2:
            await update.message.reply_text(
                "❌ **Неверный формат!**\n\nИспользуйте: `USER_ID СУММА_USDT`",
                parse_mode='Markdown'
            )
            return
        target_user_id = int(parts[0])
        amount = float(parts[1])
        if amount <= 0:
            await update.message.reply_text("❌ Сумма должна быть больше 0")
            return
        target_user_data = ensure_user_registered(target_user_id)
        old_balance = target_user_data.get('balance_usdt', 0.0)
        if old_balance < amount:
            await update.message.reply_text(
                f"❌ Недостаточно средств у пользователя! Баланс: {old_balance:.2f} USDT, требуется списать: {amount:.2f} USDT"
            )
            return
        target_user_data['balance_usdt'] = old_balance - amount
        save_users()

        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=(
                    f"💰 **С вашего баланса списано администратором!**\n\n"
                    f"📉 **Списано:** {amount:.2f} USDT\n"
                    f"💵 **Новый баланс:** {target_user_data['balance_usdt']:.2f} USDT\n\n"
                    f"По вопросам обратитесь к администратору @{ADMIN_USERNAME}."
                ),
                parse_mode='Markdown',
                reply_markup=get_main_inline_keyboard(target_user_id)
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {target_user_id}: {e}")

        await update.message.reply_text(
            f"✅ **Баланс пользователя {target_user_id} уменьшен!**\n\n"
            f"📉 **Списано:** {amount:.2f} USDT\n"
            f"💵 **Новый баланс:** {target_user_data['balance_usdt']:.2f} USDT",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )

        if 'awaiting_admin_remove_balance' in context.user_data:
            del context.user_data['awaiting_admin_remove_balance']
    except ValueError:
        await update.message.reply_text(
            "❌ **Ошибка!**\n\nUSER_ID должен быть числом\nСУММА_USDT должна быть числом",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Ошибка списания баланса админом: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")

# ========== УПРАВЛЕНИЕ КАТЕГОРИЯМИ ==========
async def admin_manage_categories_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    categories = get_categories()
    info_text = ""
    if categories:
        info_text = f"📊 **Статистика:**\n• Категорий: {len(categories)}\n"
        for category in categories:
            subcategories = get_subcategories(category)
            category_types = get_types(category, None)
            info_text += f"\n📁 **{escape_markdown(category)}:**\n"
            info_text += f"   📂 Подкатегорий: {len(subcategories)}\n"
            info_text += f"   📝 Типов в категории: {len(category_types)}\n"

    await safe_edit_message_text(
        query=query,
        text="📂 **Управление категориями**\n\n"
            "**Иерархия товаров:**\n"
            "1. 📁 Категория (первый уровень)\n"
            "2. 📂 Подкатегория (второй уровень, необязательно)\n"
            "3. 📝 Тип (третий уровень)\n\n"
            "⚠️ **Важно:** Тип можно создать:\n"
            "• В категории (без подкатегории)\n"
            "• В подкатегории\n\n"
            + info_text +
            "\n**Выберите действие:**",
        parse_mode='Markdown',
        reply_markup=get_category_management_keyboard()
    )

async def admin_add_category_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    await safe_edit_message_text(
        query=query,
        text="➕ **Создание новой категории**\n\n"
            "Категория - это первый уровень иерархии.\n"
            "Примеры категорий: Аккаунты WB, Прокси, Базы данных\n\n"
            "Введите название новой категории:\n\n"
            "📝 **Пример:** Аккаунты WB\n\n"
            "Или 'отмена' для отмены",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_categories")]
        ])
    )
    context.user_data['awaiting_category_name'] = True

async def admin_add_subcategory_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    categories = get_categories()
    if not categories:
        await safe_edit_message_text(
            query=query,
            text="❌ **Нет категорий!**\n\nСначала создайте категорию.",
            parse_mode='Markdown',
            reply_markup=get_back_to_categories_keyboard()
        )
        return

    keyboard = []
    for category in categories:
        keyboard.append([InlineKeyboardButton(f"📁 {category}", callback_data=f"select_category_for_subcat:{category}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_categories")])

    await safe_edit_message_text(
        query=query,
        text="📂 **Создание подкатегории в категории**\n\n"
            "Подкатегория - это второй уровень иерархии.\n"
            "Пример: В категории 'Аккаунты WB' можно создать подкатегории:\n"
            "• Мегафон\n• Tele2\n• МТС\n\n"
            "Выберите категорию для создания подкатегории:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def admin_add_type_to_category_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    categories = get_categories()
    if not categories:
        await safe_edit_message_text(
            query=query,
            text="❌ **Нет категорий!**\n\nСначала создайте категорию.",
            parse_mode='Markdown',
            reply_markup=get_back_to_categories_keyboard()
        )
        return

    keyboard = []
    for category in categories:
        keyboard.append([InlineKeyboardButton(f"📁 {category}", callback_data=f"select_category_for_type_to_category:{category}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_categories")])

    await safe_edit_message_text(
        query=query,
        text="📝 **Создание типа в категории (БЕЗ подкатегории)**\n\n"
            "Типы, созданные этим способом, будут напрямую привязаны к категории.\n"
            "Покупатели будут видеть их в меню 'Типы в категории'.\n\n"
            "Выберите категорию для создания типа:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def admin_add_type_to_subcategory_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    categories = get_categories()
    if not categories:
        await safe_edit_message_text(
            query=query,
            text="❌ **Нет категорий!**\n\nСначала создайте категорию.",
            parse_mode='Markdown',
            reply_markup=get_back_to_categories_keyboard()
        )
        return

    keyboard = []
    for category in categories:
        keyboard.append([InlineKeyboardButton(f"📁 {category} ▶", callback_data=f"select_category_for_type:{category}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_categories")])

    await safe_edit_message_text(
        query=query,
        text="📝 **Создание типа в подкатегории**\n\n"
            "Типы, созданные этим способом, будут привязаны к конкретной подкатегории.\n"
            "Покупатели будут видеть их в меню подкатегории.\n\n"
            "Выберите категорию:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def select_category_for_type_to_category_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    await safe_answer_query(query)
    await safe_edit_message_text(
        query=query,
        text=f"➕ **Добавление типа в категорию (без подкатегории): {escape_markdown(category)}**\n\n"
            "Введите название нового типа:\n\n"
            "📝 **Пример:** Женщины, Мужчины, Унисекс\n\n"
            "Или 'отмена' для отмены",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_add_type_to_category")]
        ])
    )
    context.user_data['awaiting_type_to_category'] = {'category': category, 'subcategory': None}

async def select_category_for_subcategory_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    await safe_answer_query(query)
    await safe_edit_message_text(
        query=query,
        text=f"➕ **Добавление подкатегории в категорию: {escape_markdown(category)}**\n\n"
            "Введите название новой подкатегории:\n\n"
            "📝 **Пример:** Мегафон, Tele2, МТС\n\n"
            "Или 'отмена' для отмены",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_add_subcategory")]
        ])
    )
    context.user_data['awaiting_subcategory_name'] = category

async def select_category_for_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    await safe_answer_query(query)
    subcategories = get_subcategories(category)
    if not subcategories:
        await safe_edit_message_text(
            query=query,
            text=f"❌ **В категории '{escape_markdown(category)}' нет подкатегорий!**\n\n"
                "Вы можете:\n"
                "1. Создать подкатегорию\n"
                "2. Создать тип прямо в категории (без подкатегории)",
            parse_mode='Markdown',
            reply_markup=get_back_to_categories_keyboard()
        )
        return

    keyboard = []
    for subcategory in subcategories:
        keyboard.append([InlineKeyboardButton(f"📂 {subcategory}", callback_data=f"select_subcategory_for_type:{category}:{subcategory}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_add_type_to_subcategory")])

    await safe_edit_message_text(
        query=query,
        text=f"📝 **Создание типа в подкатегории**\n"
            f"📁 **Категория:** {escape_markdown(category)}\n\n"
            "Тип - это третий уровень иерархии.\n"
            "Пример: В подкатегории 'Мегафон' можно создать типы:\n"
            "• Женщины\n• Мужчины\n• Унисекс\n\n"
            "Выберите подкатегорию:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def select_subcategory_for_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str, subcategory: str):
    query = update.callback_query
    await safe_answer_query(query)
    await safe_edit_message_text(
        query=query,
        text=f"➕ **Добавление типа в подкатегорию: {escape_markdown(subcategory)}**\n"
            f"📁 **Категория:** {escape_markdown(category)}\n\n"
            "Введите название нового типа:\n\n"
            "📝 **Пример:** Женщины, Мужчины, Унисекс\n\n"
            "Или 'отмена' для отмены",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data=f"select_category_for_type:{category}")]
        ])
    )
    context.user_data['awaiting_type_name'] = {'category': category, 'subcategory': subcategory}

async def admin_view_structure_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    categories = get_categories()
    if not categories:
        await safe_edit_message_text(
            query=query,
            text="📭 **Нет категорий!**\n\nСтруктура каталога пуста.",
            parse_mode='Markdown',
            reply_markup=get_back_to_categories_keyboard()
        )
        return

    structure_text = "📊 **Структура каталога:**\n\n"
    for i, category in enumerate(categories, 1):
        structure_text += f"{i}. 📁 **{escape_markdown(category)}**\n"
        subcategories = get_subcategories(category)
        category_types = get_types(category, None)
        if category_types:
            structure_text += f"   📝 **Типы в категории:**\n"
            for type_ in category_types:
                product_count = len([p for p in catalog if p.get('category') == category and p.get('type') == type_ and not p.get('subcategory')])
                structure_text += f"      • {escape_markdown(type_)} ({product_count} товаров)\n"
        if subcategories:
            for j, subcategory in enumerate(subcategories, 1):
                structure_text += f"   {j}. 📂 **{escape_markdown(subcategory)}**\n"
                types = get_types(category, subcategory)
                if types:
                    for k, type_ in enumerate(types, 1):
                        product_count = len(get_products_by_path(category, subcategory, type_))
                        structure_text += f"      {k}. 📝 **{escape_markdown(type_)}** ({product_count} товаров)\n"
                else:
                    product_count = len(get_products_by_path(category, subcategory))
                    structure_text += f"      • 📦 Товаров без типа: {product_count}\n"
        else:
            product_count = len(get_products_by_path(category))
            structure_text += f"   • 📦 Товаров без подкатегории: {product_count}\n"
        structure_text += "\n"

    total_products = len(catalog)
    structure_text += f"📦 **Всего товаров в каталоге:** {total_products}"

    await safe_edit_message_text(
        query=query,
        text=structure_text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить", callback_data="admin_view_structure")],
            [InlineKeyboardButton("🔙 К управлению категориями", callback_data="admin_manage_categories")]
        ])
    )

async def admin_delete_category_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    categories = get_categories()
    if not categories:
        await safe_edit_message_text(
            query=query,
            text="📭 **Нет категорий для удаления!**",
            parse_mode='Markdown',
            reply_markup=get_back_to_categories_keyboard()
        )
        return

    keyboard = []
    for category in categories:
        product_count = len(get_products_by_path(category))
        keyboard.append([InlineKeyboardButton(
            f"🗑️ {category} ({product_count} товаров)",
            callback_data=f"confirm_delete_category:{category}"
        )])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_categories")])

    await safe_edit_message_text(
        query=query,
        text="🗑️ **Удаление категории**\n\n"
            "⚠️ **Внимание:** Удаление категории также удалит все подкатегории, типы и товары в ней!\n\n"
            "Выберите категорию для удаления:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def confirm_delete_category_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    product_count = len(get_products_by_path(category))
    subcategories = get_subcategories(category)

    confirm_text = (
        f"⚠️ **Подтверждение удаления категории:** {escape_markdown(category)}\n\n"
        f"📊 **Статистика для удаления:**\n"
        f"• Категория: {category}\n"
        f"• Подкатегорий: {len(subcategories)}\n"
        f"• Товаров: {product_count}\n\n"
        f"❌ **Все данные будут безвозвратно удалены!**\n\n"
        f"Вы уверены что хотите удалить категорию '{category}'?"
    )

    await safe_edit_message_text(
        query=query,
        text=confirm_text,
        parse_mode='Markdown',
        reply_markup=get_confirm_delete_category_keyboard(category)
    )

async def final_delete_category_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    products_to_delete = get_products_by_path(category)
    deleted_count = 0
    for product in products_to_delete:
        if product.get('has_file') and product.get('file_path'):
            try:
                file_path = product.get('file_path')
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                logger.error(f"Ошибка удаления файла {product['file_path']}: {e}")
        catalog[:] = [p for p in catalog if p['id'] != product['id']]
        deleted_count += 1

    save_catalog()
    await safe_edit_message_text(
        query=query,
        text=f"✅ **Категория '{category}' успешно удалена!**\n\n"
            f"📊 **Удалено:**\n"
            f"• Категория: {category}\n"
            f"• Товаров: {deleted_count}\n\n"
            f"Все данные категории были безвозвратно удалены.",
        parse_mode='Markdown',
        reply_markup=get_back_to_categories_keyboard()
    )

# ========== УДАЛЕНИЕ ПОДКАТЕГОРИИ ==========
async def admin_delete_subcategory_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    categories = get_categories()
    if not categories:
        await safe_edit_message_text(
            query=query,
            text="📭 **Нет категорий для удаления подкатегорий!**\n\nСначала создайте категорию.",
            parse_mode='Markdown',
            reply_markup=get_back_to_categories_keyboard()
        )
        return

    keyboard = []
    for category in categories:
        keyboard.append([InlineKeyboardButton(f"📁 {category}", callback_data=f"select_category_for_delete_subcat:{category}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_categories")])

    await safe_edit_message_text(
        query=query,
        text="🗑️ **Удаление подкатегории**\n\nВыберите категорию, в которой хотите удалить подкатегорию:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def select_category_for_delete_subcat_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    subcategories = get_subcategories(category)
    if not subcategories:
        await safe_edit_message_text(
            query=query,
            text=f"❌ **В категории '{escape_markdown(category)}' нет подкатегорий для удаления!**",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К выбору категории", callback_data="admin_delete_subcategory")]
            ])
        )
        return

    keyboard = []
    for subcategory in subcategories:
        product_count = len(get_products_by_path(category, subcategory))
        keyboard.append([InlineKeyboardButton(
            f"🗑️ {subcategory} ({product_count} товаров)",
            callback_data=f"select_subcategory_for_delete:{category}:{subcategory}"
        )])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_delete_subcategory")])

    await safe_edit_message_text(
        query=query,
        text=f"🗑️ **Удаление подкатегории в категории '{escape_markdown(category)}'**\n\nВыберите подкатегорию для удаления:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def select_subcategory_for_delete_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str, subcategory: str):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    product_count = len(get_products_by_path(category, subcategory))

    confirm_text = (
        f"⚠️ **Подтверждение удаления подкатегории:**\n\n"
        f"📁 **Категория:** {escape_markdown(category)}\n"
        f"📂 **Подкатегория:** {escape_markdown(subcategory)}\n"
        f"📦 **Товаров в подкатегории:** {product_count}\n\n"
        f"❌ **Все данные подкатегории будут безвозвратно удалены!**\n\n"
        f"Вы уверены, что хотите удалить подкатегорию '{subcategory}'?"
    )

    await safe_edit_message_text(
        query=query,
        text=confirm_text,
        parse_mode='Markdown',
        reply_markup=get_confirm_delete_subcategory_keyboard(category, subcategory)
    )

async def final_delete_subcategory_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str, subcategory: str):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        await safe_answer_query(query, "❌ Нет доступа", show_alert=True)
        return

    products_to_delete = [p for p in catalog if p.get('category') == category and p.get('subcategory') == subcategory]
    deleted_count = 0
    for product in products_to_delete:
        if product.get('has_file') and product.get('file_path'):
            try:
                file_path = product.get('file_path')
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                logger.error(f"Ошибка удаления файла {product['file_path']}: {e}")
        catalog[:] = [p for p in catalog if p['id'] != product['id']]
        deleted_count += 1

    save_catalog()
    await safe_edit_message_text(
        query=query,
        text=f"✅ **Подкатегория '{subcategory}' успешно удалена!**\n\n"
            f"📊 **Удалено товаров:** {deleted_count}\n\n"
            f"Все данные подкатегории были безвозвратно удалены.",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К управлению категориями", callback_data="admin_manage_categories")]
        ])
    )

# ========== УПРАВЛЕНИЕ АДМИНИСТРАТОРАМИ ==========
async def admin_manage_admins_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_bot_owner(user_id):
        await safe_answer_query(query, "❌ Только владелец бота может управлять администраторами", show_alert=True)
        return

    await safe_edit_message_text(
        query=query,
        text="👥 **Управление администраторов**\n\nВыберите действие:",
        parse_mode='Markdown',
        reply_markup=get_admin_management_keyboard()
    )

async def admin_add_admin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_bot_owner(user_id):
        await safe_answer_query(query, "❌ Только владелец бота может добавлять администраторов", show_alert=True)
        return

    response_text = (
        "➕ **Добавление администратора**\n\n"
        "Введите ID пользователя, которого хотите сделать администратором:\n\n"
        "📝 **Как получить ID пользователя:**\n"
        "1. Попросите пользователя отправить команду /myid\n"
        "2. Или используйте команду /addadmin USER_ID\n\n"
        "Напишите ID пользователя:"
    )
    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_admins")]
        ])
    )
    context.user_data['awaiting_admin_id'] = 'add'

async def admin_remove_admin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_bot_owner(user_id):
        await safe_answer_query(query, "❌ Только владелец бота может удалять администраторов", show_alert=True)
        return

    response_text = (
        "➖ **Удаление администратора**\n\n"
        "Введите ID пользователя, которого хотите удалить из администраторов:\n\n"
        "📝 **Как узнать ID администраторов:**\n"
        "Нажмите '👥 Список администраторов'\n\n"
        "Напишите ID пользователя:"
    )
    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_admins")]
        ])
    )
    context.user_data['awaiting_admin_id'] = 'remove'

async def admin_list_admins_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_bot_owner(user_id):
        await safe_answer_query(query, "❌ Только владелец бота может просматривать список администраторов", show_alert=True)
        return

    response_text = "👥 **Список администраторов:**\n\n"
    for i, admin_id in enumerate(admins, 1):
        admin_info = users.get(str(admin_id), {})
        username = escape_markdown(admin_info.get('username', 'unknown'))
        first_name = escape_markdown(admin_info.get('first_name', f'Пользователь {admin_id}'))
        if admin_id == BOT_OWNER_ID:
            response_text += f"{i}. 👑 **Владелец бота:**\n"
        else:
            response_text += f"{i}. 👤 **Администратор:**\n"
        response_text += f"   👤 **Имя:** {first_name}\n"
        response_text += f"   📧 **Username:** @{username}\n"
        response_text += f"   🆔 **ID:** `{admin_id}`\n\n"
    response_text += f"📊 **Всего администраторов:** {len(admins)}"

    await safe_edit_message_text(
        query=query,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=get_admin_management_keyboard()
    )

async def addadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_bot_owner(user_id):
        await update.message.reply_text("⛔ Только владелец бота может добавлять администраторов")
        return
    if not context.args:
        await update.message.reply_text(
            "➕ **Добавление администратора**\n\n"
            "Используйте команду:\n"
            "`/addadmin USER_ID`\n\n"
            "**Пример:**\n"
            "`/addadmin 1234567890`",
            parse_mode='Markdown'
        )
        return
    try:
        new_admin_id = int(context.args[0])
        if new_admin_id in admins:
            await update.message.reply_text("❌ Этот пользователь уже является администратором")
            return
        if add_admin(new_admin_id):
            await update.message.reply_text(
                f"✅ **Пользователь {new_admin_id} добавлен в администраторы!**\n\nТеперь он имеет доступ к админ-панели.",
                parse_mode='Markdown'
            )
            try:
                await context.bot.send_message(
                    chat_id=new_admin_id,
                    text=f"👑 **Вы стали администратором бота!**\n\nВладелец бота назначил вас администратором.\nИспользуйте команду /admin для доступа к панели управления.",
                    parse_mode='Markdown',
                    reply_markup=get_main_inline_keyboard(new_admin_id)
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {new_admin_id}: {e}")
        else:
            await update.message.reply_text("❌ Ошибка добавления администратора")
    except ValueError:
        await update.message.reply_text("❌ Неверный формат ID. ID должен быть числом")

async def removeadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_bot_owner(user_id):
        await update.message.reply_text("⛔ Только владелец бота может удалять администраторов")
        return
    if not context.args:
        await update.message.reply_text(
            "➖ **Удаление администратора**\n\n"
            "Используйте команду:\n"
            "`/removeadmin USER_ID`\n\n"
            "**Пример:**\n"
            "`/removeadmin 1234567890`",
            parse_mode='Markdown'
        )
        return
    try:
        admin_id_to_remove = int(context.args[0])
        if admin_id_to_remove == BOT_OWNER_ID:
            await update.message.reply_text("❌ Нельзя удалить владельца бота")
            return
        if admin_id_to_remove not in admins:
            await update.message.reply_text("❌ Этот пользователь не является администратором")
            return
        if remove_admin(admin_id_to_remove):
            admin_info = users.get(str(admin_id_to_remove), {})
            username = escape_markdown(admin_info.get('username', 'unknown'))
            first_name = escape_markdown(admin_info.get('first_name', 'User'))
            await update.message.reply_text(
                f"✅ **Администратор {first_name} удален!**\n\n"
                f"👤 **Имя:** {first_name}\n"
                f"📧 **Username:** @{username}\n"
                f"🆔 **ID:** `{admin_id_to_remove}`\n\n"
                f"Пользователь больше не имеет прав администратора.",
                parse_mode='Markdown'
            )
            try:
                await context.bot.send_message(
                    chat_id=admin_id_to_remove,
                    text=f"👑 **Вы больше не администратор бота.**\n\nВладелец бота снял с вас права администратора.",
                    parse_mode='Markdown',
                    reply_markup=get_main_inline_keyboard(admin_id_to_remove)
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {admin_id_to_remove}: {e}")
        else:
            await update.message.reply_text("❌ Ошибка удаления администратора")
    except ValueError:
        await update.message.reply_text("❌ Неверный формат ID. ID должен быть числом")

# ========== СОЗДАНИЕ ТОВАРА ==========
async def admin_create_product_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    await safe_edit_message_text(
        query=query,
        text="➕ **Создание нового товара**\n\n"
            "Отправьте данные в формате:\n"
            "`Название|Цена_USDT|Описание|Категория|Подкатегория|Тип|has_file`\n\n"
            "**Примеры:**\n"
            "`Аккаунт ВБ|1.11|Доступ к аккаунту Wildberries|WB|Мегафон|Женщины|true`\n"
            "`Прокси США|2.50|Прокси серверы США|Прокси|||false`\n\n"
            "**Параметры:**\n"
            "• `Цена_USDT` - число, например: 1.11\n"
            "• `Подкатегория` - можно оставить пустым\n"
            "• `Тип` - можно оставить пустым\n"
            "• `has_file` - true/false (есть ли у товара файл)\n\n"
            "📝 **Отправьте данные товара:**\n"
            "Или нажмите '🔙 В админку' для отмены",
        parse_mode='Markdown',
        reply_markup=get_back_to_admin_keyboard()
    )
    context.user_data['awaiting_product_data'] = True

async def process_product_creation(update: Update, context: ContextTypes.DEFAULT_TYPE, product_data: str):
    try:
        if product_data.lower() in ['отмена', 'cancel', 'назад', 'back']:
            await update.message.reply_text(
                "❌ Создание товара отменено.",
                parse_mode='Markdown',
                reply_markup=get_admin_keyboard()
            )
            if 'awaiting_product_data' in context.user_data:
                del context.user_data['awaiting_product_data']
            return

        parts = [p.strip() for p in product_data.split('|')]
        if len(parts) < 7:
            await update.message.reply_text(
                "❌ **Недостаточно данных!**\n\n"
                "Нужно 7 параметров через символ |\n"
                "Формат: Название|Цена_USDT|Описание|Категория|Подкатегория|Тип|has_file\n\n"
                "**Пример:**\n"
                "`Аккаунт ВБ|1.11|Доступ к аккаунту Wildberries|WB|Мегафон|Женщины|true`",
                parse_mode='Markdown'
            )
            return

        name, price_usdt_str, description, category, subcategory, type_, has_file_str = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6]

        if not name or len(name) < 2:
            await update.message.reply_text("❌ **Название товара слишком короткое!** Минимум 2 символа.")
            return

        try:
            price_usdt = float(price_usdt_str.replace(',', '.'))
            if price_usdt <= 0:
                await update.message.reply_text("❌ **Цена должна быть больше 0!**")
                return
            if price_usdt > 10000:
                await update.message.reply_text("❌ **Цена слишком высокая!** Максимум 10000 USDT.")
                return
        except ValueError:
            await update.message.reply_text(
                "❌ **Неверный формат цены!**\n\n"
                "Используйте число, например: 1.11 или 10.5\n"
                "Десятичный разделитель может быть точкой или запятой.\n\n"
                f"**Вы ввели:** `{price_usdt_str}`",
                parse_mode='Markdown'
            )
            return

        if not description or len(description) < 5:
            await update.message.reply_text("❌ **Описание слишком короткое!** Минимум 5 символов.")
            return

        if not category or len(category) < 2:
            await update.message.reply_text("❌ **Категория слишком короткая!** Минимум 2 символа.")
            return

        has_file = has_file_str.lower() == 'true'
        if has_file_str.lower() not in ['true', 'false']:
            await update.message.reply_text(
                "❌ **Неверное значение has_file!**\n\n"
                "Допустимые значения: true или false\n"
                "**Пример:** Аккаунт ВБ|1.11|Описание|Категория|||true",
                parse_mode='Markdown'
            )
            return

        product_id = f"prod_{uuid.uuid4().hex[:8]}"
        new_product = {
            "id": product_id,
            "name": name,
            "price": price_usdt,
            "description": description,
            "category": category,
            "subcategory": subcategory if subcategory else "",
            "type": type_ if type_ else "",
            "has_file": has_file,
            "quantity": 1 if has_file else 10,
            "created_at": datetime.now().isoformat(),
            "sold": False
        }
        if has_file:
            new_product.update({
                "file_path": None,
                "file_name": None,
                "file_uploaded": False
            })

        catalog.append(new_product)
        save_catalog()

        response_text = (
            f"✅ **Товар создан успешно!**\n\n"
            f"🆔 **ID товара:** `{product_id}`\n"
            f"🛍️ **Название:** {escape_markdown(name)}\n"
            f"💰 **Цена:** {price_usdt} USDT\n"
            f"📝 **Описание:** {escape_markdown(description)}\n"
            f"📂 **Категория:** {escape_markdown(category)}\n"
            f"📁 **Подкатегория:** {escape_markdown(subcategory) if subcategory else 'Нет'}\n"
            f"📝 **Тип:** {escape_markdown(type_) if type_ else 'Нет'}\n"
            f"📄 **Файловый товар:** {'Да' if has_file else 'Нет'}\n"
            f"📦 **Количество:** {new_product['quantity']} шт."
        )

        if has_file:
            response_text += (
                f"\n\n📎 **Чтобы прикрепить файл:**\n"
                f"1. Перейдите в '📁 Прикрепить файл к товару'\n"
                f"2. Выберите товар с ID: `{product_id}`\n"
                f"3. Отправьте файл .txt\n\n"
                f"⚠️ **Товар не будет отображаться в каталоге до прикрепления файла!**"
            )

        await update.message.reply_text(
            response_text,
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )

        if 'awaiting_product_data' in context.user_data:
            del context.user_data['awaiting_product_data']
    except Exception as e:
        logger.error(f"Ошибка создания товара: {e}")
        await update.message.reply_text(
            f"❌ **Ошибка при создании товара:**\n\n"
            f"`{str(e)}`\n\n"
            f"**Проверьте формат данных и попробуйте снова.**\n"
            f"Формат: Название|Цена_USDT|Описание|Категория|Подкатегория|Тип|has_file\n\n"
            f"**Вы ввели:** `{product_data}`",
            parse_mode='Markdown'
        )

# ========== МАССОВАЯ ЗАГРУЗКА В ТИП ==========
async def admin_bulk_upload_to_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    categories = get_categories()
    if not categories:
        await safe_edit_message_text(
            query=query,
            text="❌ **Нет категорий!**\n\nСначала добавьте категорию.",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )
        return

    keyboard = []
    for category in categories:
        keyboard.append([InlineKeyboardButton(f"📁 {category} ▶", callback_data=f"bulk_select_category:{category}")])
    keyboard.append([InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")])

    await safe_edit_message_text(
        query=query,
        text="📦 **Массовая загрузка в тип**\n\nВыберите категорию:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def bulk_select_category_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    await safe_answer_query(query)
    subcategories = get_subcategories(category)
    category_types = get_types(category, None)

    keyboard = []
    if subcategories:
        for subcategory in subcategories:
            keyboard.append([InlineKeyboardButton(f"📂 {subcategory} ▶", callback_data=f"bulk_select_subcategory:{category}:{subcategory}")])
    if category_types:
        keyboard.append([InlineKeyboardButton(f"📝 Типы в категории (без подкатегории)", callback_data=f"bulk_category_types:{category}")])

    if not keyboard:
        await safe_edit_message_text(
            query=query,
            text=f"❌ **В категории '{escape_markdown(category)}' нет подкатегорий или типов!**\n\nСначала добавьте подкатегорию или тип в категорию.",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )
        return

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_bulk_upload_to_type")])

    await safe_edit_message_text(
        query=query,
        text=f"📦 **Массовая загрузка в тип**\n"
            f"📁 **Категория:** {escape_markdown(category)}\n\n"
            "Выберите подкатегорию или типы в категории:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def bulk_category_types_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    await safe_answer_query(query)
    category_types = get_types(category, None)
    if not category_types:
        await safe_edit_message_text(
            query=query,
            text=f"❌ **В категории '{escape_markdown(category)}' нет типов (без подкатегории)!**\n\nСначала добавьте тип в категорию.",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )
        return

    keyboard = []
    for type_ in category_types:
        product_count = len([p for p in catalog if p.get('category') == category and p.get('type') == type_ and not p.get('subcategory')])
        keyboard.append([InlineKeyboardButton(f"📝 {escape_markdown(type_)} ({product_count} товаров)", callback_data=f"bulk_select_category_type:{category}:{type_}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"bulk_select_category:{category}")])

    await safe_edit_message_text(
        query=query,
        text=f"📦 **Массовая загрузка в тип (без подкатегории)**\n"
            f"📁 **Категория:** {escape_markdown(category)}\n\n"
            "Выберите тип для загрузки:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def bulk_select_subcategory_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str, subcategory: str):
    query = update.callback_query
    await safe_answer_query(query)
    types = get_types(category, subcategory)
    if not types:
        await safe_edit_message_text(
            query=query,
            text=f"❌ **В подкатегории '{escape_markdown(subcategory)}' нет типов!**\n\nСначала добавьте тип.",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )
        return

    keyboard = []
    for type_ in types:
        product_count = len(get_products_by_path(category, subcategory, type_))
        keyboard.append([InlineKeyboardButton(f"📝 {escape_markdown(type_)} ({product_count} товаров)", callback_data=f"bulk_select_type:{category}:{subcategory}:{type_}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"bulk_select_category:{category}")])

    await safe_edit_message_text(
        query=query,
        text=f"📦 **Массовая загрузка в тип**\n"
            f"📁 **Категория:** {escape_markdown(category)}\n"
            f"📂 **Подкатегория:** {escape_markdown(subcategory)}\n\n"
            "Выберите тип для загрузка:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def bulk_select_category_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str, type_: str):
    query = update.callback_query
    await safe_answer_query(query)
    await safe_edit_message_text(
        query=query,
        text=f"📦 **Массовая загрузка в тип (без подкатегории)**\n"
            f"📁 **Категория:** {escape_markdown(category)}\n"
            f"📝 **Тип:** {escape_markdown(type_)}\n\n"
            "📝 **Введите параметры в формате:**\n"
            "`Цена_USDT|Описание`\n\n"
            "**Пример:**\n"
            "`1.11|Аккаунт Wildberries`\n\n"
            "📤 **После ввода параметров отправляйте .txt файлы.**\n"
            "Каждый файл будет создан как отдельный товар в выбранном типе.\n\n"
            "Или 'отмена' для отмены",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data=f"bulk_category_types:{category}")]
        ])
    )
    context.user_data['awaiting_bulk_upload_params'] = {
        'category': category,
        'subcategory': None,
        'type': type_
    }

async def bulk_select_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str, subcategory: str, type_: str):
    query = update.callback_query
    await safe_answer_query(query)
    await safe_edit_message_text(
        query=query,
        text=f"📦 **Массовая загрузка в тип**\n"
            f"📁 **Категория:** {escape_markdown(category)}\n"
            f"📂 **Подкатегория:** {escape_markdown(subcategory)}\n"
            f"📝 **Тип:** {escape_markdown(type_)}\n\n"
            "📝 **Введите параметры в формате:**\n"
            "`Цена_USDT|Описание`\n\n"
            "**Пример:**\n"
            "`1.11|Аккаунт Wildberries`\n\n"
            "📤 **После ввода параметров отправляйте .txt файлы.**\n"
            "Каждый файл будет создан как отдельный товар в выбранном типе.\n\n"
            "Или 'отмена' для отмены",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data=f"bulk_select_subcategory:{category}:{subcategory}")]
        ])
    )
    context.user_data['awaiting_bulk_upload_params'] = {
        'category': category,
        'subcategory': subcategory,
        'type': type_
    }

async def handle_bulk_documents(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    if 'awaiting_bulk_upload_params' not in context.user_data:
        await update.message.reply_text(
            "⚠️ **Сначала укажите параметры для загрузки.**\n\n"
            "Введите параметры в формате:\n"
            "`Цена_USDT|Описание`\n\n"
            "**Пример:**\n"
            "`1.11|Аккаунт Wildberries`",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")]
            ])
        )
        return

    params = context.user_data['awaiting_bulk_upload_params']
    category = params.get('category')
    subcategory = params.get('subcategory')
    type_ = params.get('type')
    document = update.message.document

    if not document.file_name.endswith('.txt'):
        await update.message.reply_text("❌ **Поддерживаются только .txt файлы!**")
        return

    try:
        existing_bundle = None
        bundle_name = f"Набор {type_}"
        for product in catalog:
            if (product.get('category') == category and
                product.get('type') == type_ and
                product.get('is_bundle', False) and
                bundle_name in product.get('name', '')):
                if subcategory is None and not product.get('subcategory'):
                    existing_bundle = product
                    break
                elif subcategory and product.get('subcategory') == subcategory:
                    existing_bundle = product
                    break

        if existing_bundle:
            bundle_files = existing_bundle.get('bundle_files', [])
            file_names = {f['name'] for f in bundle_files}
            if document.file_name in file_names:
                await update.message.reply_text(f"❌ **Файл '{document.file_name}' уже есть в наборе!**")
                return

            file = await context.bot.get_file(document.file_id)
            file_name = f"{existing_bundle['id']}_{len(bundle_files) + 1}.txt"
            file_path = os.path.join(PRODUCT_FILES_DIR, file_name)
            await file.download_to_drive(custom_path=file_path)

            bundle_files.append({
                "name": document.file_name,
                "path": file_path,
                "size": document.file_size,
                "added_at": datetime.now().isoformat(),
                "sold": False
            })
            existing_bundle['bundle_files'] = bundle_files
            save_catalog()

            available_files = get_available_files_count(bundle_files)
            subcategory_line = f"📂 Подкатегория: {escape_markdown(subcategory)}\n" if subcategory else ""

            await update.message.reply_text(
                f"✅ **Файл '{escape_markdown(document.file_name)}' добавлен в существующий набор!**\n\n"
                f"📦 **Набор:** {escape_markdown(existing_bundle['name'])}\n"
                f"📁 **Категория:** {escape_markdown(category)}\n"
                f"{subcategory_line}"
                f"📝 **Тип:** {escape_markdown(type_)}\n"
                f"💰 **Цена:** {params.get('price', 1.11)} USDT\n"
                f"📊 **Теперь файлов в наборе:** {len(bundle_files)}\n"
                f"📈 **Доступно файлов:** {available_files}\n\n"
                f"📤 **Можете загрузить следующий файл или нажмите '🔙 В админку' для завершения.**",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")]
                ])
            )
        else:
            price = params.get('price', 1.11)
            description = params.get('description', f'Набор файлов для типа {type_}')
            product_id = f"bundle_{uuid.uuid4().hex[:8]}"

            file = await context.bot.get_file(document.file_id)
            file_name = f"{product_id}_1.txt"
            file_path = os.path.join(PRODUCT_FILES_DIR, file_name)
            await file.download_to_drive(custom_path=file_path)

            new_bundle = {
                "id": product_id,
                "name": generate_unique_bundle_name(f"Набор {type_}"),
                "price": price,
                "description": description,
                "category": category,
                "subcategory": subcategory if subcategory else "",
                "type": type_,
                "has_file": True,
                "is_bundle": True,
                "quantity": 1,
                "created_at": datetime.now().isoformat(),
                "sold": False,
                "bundle_files": [
                    {
                        "name": document.file_name,
                        "path": file_path,
                        "size": document.file_size,
                        "added_at": datetime.now().isoformat(),
                        "sold": False
                    }
                ]
            }
            catalog.append(new_bundle)
            save_catalog()

            subcategory_text = f"📂 **Подкатегория:** {escape_markdown(subcategory)}\n" if subcategory else ""

            await update.message.reply_text(
                f"✅ **Создан новый набор! Файл '{escape_markdown(document.file_name)}' добавлен.**\n\n"
                f"📦 **Название набора:** {escape_markdown(new_bundle['name'])}\n"
                f"📁 **Категория:** {escape_markdown(category)}\n"
                f"{subcategory_text}"
                f"📝 **Тип:** {escape_markdown(type_)}\n"
                f"💰 **Цена:** {price} USDT за файл\n"
                f"📝 **Описание:** {escape_markdown(description)}\n"
                f"🆔 **ID набора:** `{product_id}`\n"
                f"📦 **Количество файлов:** 1\n\n"
                f"📤 **Можете загрузить следующий файл в этот же набор или нажмите '🔙 В админку' для завершения.**",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")]
                ])
            )
    except Exception as e:
        logger.error(f"Ошибка загрузки файла {document.file_name}: {e}")
        await update.message.reply_text(
            f"❌ **Ошибка загрузки файла:**\n\n`{str(e)}`",
            parse_mode='Markdown'
        )

# ========== ПРИКРЕПЛЕНИЕ ФАЙЛА ==========
async def admin_attach_file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    load_data()

    products_without_files = [p for p in catalog if p.get('has_file') and not p.get('file_path') and not p.get('sold', False)]
    if not products_without_files:
        await safe_edit_message_text(
            query=query,
            text="✅ **Все файлы загружены!**\n\nНет товаров, требующих прикрепления файла.",
            reply_markup=get_admin_keyboard()
        )
        return

    keyboard = []
    for product in products_without_files:
        keyboard.append([InlineKeyboardButton(
            f"{escape_markdown(product['name'])} ({product['price']} USDT)",
            callback_data=f"attach_to:{product['id']}"
        )])
    keyboard.append([InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")])

    await safe_edit_message_text(
        query=query,
        text="📁 **Прикрепление файла к товару**\n\nВыберите товар для прикрепления файла:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_document_for_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return

    if 'awaiting_file_for_product' in context.user_data:
        product_id = context.user_data['awaiting_file_for_product']
        load_data()
        product = next((p for p in catalog if p['id'] == product_id), None)
        if not product:
            await update.message.reply_text("❌ Товар не найден")
            if 'awaiting_file_for_product' in context.user_data:
                del context.user_data['awaiting_file_for_product']
            return

        document = update.message.document
        if not document.file_name.endswith('.txt'):
            await update.message.reply_text("❌ **Поддерживаются только .txt файлы!**")
            return

        try:
            file = await context.bot.get_file(document.file_id)
            file_name = f"{product_id}_{uuid.uuid4().hex[:8]}.txt"
            file_path = os.path.join(PRODUCT_FILES_DIR, file_name)
            await file.download_to_drive(custom_path=file_path)

            product['file_path'] = file_path
            product['file_name'] = document.file_name
            product['file_uploaded'] = True
            product['file_uploaded_at'] = datetime.now().isoformat()
            save_catalog()
            load_data()

            await update.message.reply_text(
                f"✅ <b>Файл успешно прикреплен!</b>\n\n"
                f"📦 <b>Товар:</b> {html.escape(product['name'])}\n"
                f"🆔 <b>ID товара:</b> <code>{html.escape(product_id)}</code>\n"
                f"📄 <b>Файл:</b> {html.escape(document.file_name)}",
                parse_mode='HTML',
                reply_markup=get_admin_keyboard()
            )

            if 'awaiting_file_for_product' in context.user_data:
                del context.user_data['awaiting_file_for_product']
        except Exception as e:
            logger.error(f"Ошибка прикрепления файла: {e}")
            await update.message.reply_text(
                f"❌ <b>Ошибка при загрузке файла:</b>\n\n<code>{html.escape(str(e))}</code>",
                parse_mode='HTML'
            )
    elif 'awaiting_bulk_upload_params' in context.user_data:
        await handle_bulk_documents(update, context)

# ========== ДРУГИЕ АДМИН ФУНКЦИИ ==========
async def admin_delete_product_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    load_data()

    if not catalog:
        await safe_edit_message_text(
            query=query,
            text="📭 **В каталоге нет товаров для удаления.**",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )
        return

    keyboard = []
    for product in catalog:
        if not product.get('sold', False):
            emoji = "📦" if product.get('is_bundle', False) else "📁" if product.get('has_file') else "🛍️"
            keyboard.append([InlineKeyboardButton(
                f"{emoji} {escape_markdown(product['name'])} ({product['price']} USDT)",
                callback_data=f"delete_product:{product['id']}"
            )])
    if not keyboard:
        keyboard.append([InlineKeyboardButton("📭 Нет товаров для удаления", callback_data="no_products_delete")])
    keyboard.append([InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")])

    await safe_edit_message_text(
        query=query,
        text="🗑️ **Удаление товара**\n\nВыберите товар для удаления:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def delete_product_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: str):
    query = update.callback_query
    await safe_answer_query(query)
    product = next((p for p in catalog if p['id'] == product_id), None)
    if not product:
        await safe_answer_query(query, "❌ Товар не найден", show_alert=True)
        return

    product_name = product['name']
    if product.get('sold', False):
        await safe_answer_query(query, "❌ Нельзя удалить проданный товар", show_alert=True)
        return

    if product.get('has_file') and product.get('file_path'):
        try:
            file_path = product.get('file_path')
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Файл удален: {file_path}")
        except Exception as e:
            logger.error(f"Ошибка удаления файла: {e}")

    catalog[:] = [p for p in catalog if p['id'] != product_id]
    save_catalog()
    global orders
    orders = [o for o in orders if o.get('product_id') != product_id]
    save_orders()

    for user_id_str, user_data in users.items():
        if 'files_purchased' in user_data:
            user_data['files_purchased'] = [fp for fp in user_data['files_purchased'] if fp.get('product_id') != product_id]
        if 'orders' in user_data:
            user_orders_to_remove = [o for o in orders if str(o.get('user_id')) == user_id_str and o.get('product_id') == product_id]
            for order in user_orders_to_remove:
                if 'orders' in user_data and order.get('order_id') in user_data['orders']:
                    user_data['orders'].remove(order.get('order_id'))
    save_users()

    await safe_answer_query(query, f"✅ Товар '{product_name}' удален", show_alert=True)
    await safe_edit_message_text(
        query=query,
        text=f"✅ **Товар успешно удален!**\n\n"
            f"🛍️ **Название:** {escape_markdown(product_name)}\n"
            f"🆔 **ID товара:** `{product_id}`\n\n"
            "Возвращаюсь в админ-панель...",
        parse_mode='Markdown',
        reply_markup=get_admin_keyboard()
    )

async def admin_add_balance_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    await safe_edit_message_text(
        query=query,
        text="💰 **Пополнение баланса пользователя (Админ)**\n\n"
            "Введите данные в формате:\n"
            "`USER_ID СУММА_USDT`\n\n"
            "**Пример:**\n"
            "`8133517773 10.5`",
        parse_mode='Markdown',
        reply_markup=get_back_to_admin_keyboard()
    )
    context.user_data['awaiting_admin_add_balance'] = True

async def process_admin_add_balance(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    try:
        parts = text.split()
        if len(parts) != 2:
            await update.message.reply_text(
                "❌ **Неверный формат!**\n\nИспользуйте: `USER_ID СУММА_USDT`",
                parse_mode='Markdown'
            )
            return
        target_user_id = int(parts[0])
        amount = float(parts[1])
        if amount <= 0:
            await update.message.reply_text("❌ Сумма должна быть больше 0")
            return
        target_user_data = ensure_user_registered(target_user_id)
        old_balance = target_user_data.get('balance_usdt', 0.0)
        target_user_data['balance_usdt'] = old_balance + amount
        save_users()

        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=(
                    f"✅ **Ваш баланс пополнен администратором!**\n\n"
                    f"💰 **Зачислено:** {amount:.2f} USDT\n"
                    f"💵 **Новый баланс:** {target_user_data['balance_usdt']:.2f} USDT"
                ),
                parse_mode='Markdown',
                reply_markup=get_main_inline_keyboard(target_user_id)
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {target_user_id}: {e}")

        await update.message.reply_text(
            f"✅ **Баланс пользователя {target_user_id} пополнен!**\n\n"
            f"💰 **Сумма:** {amount:.2f} USDT\n"
            f"💵 **Новый баланс:** {target_user_data['balance_usdt']:.2f} USDT",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )

        if 'awaiting_admin_add_balance' in context.user_data:
            del context.user_data['awaiting_admin_add_balance']
    except ValueError:
        await update.message.reply_text(
            "❌ **Ошибка!**\n\nUSER_ID должен быть числом\nСУММА_USDT должна быть числом",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Ошибка пополнения баланса админом: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")

async def admin_restock_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    load_data()

    keyboard = []
    for product in catalog:
        if not product.get('sold', False):
            emoji = "📦" if product.get('is_bundle', False) else "📁" if product.get('has_file') else "🛍️"
            quantity = product.get('quantity', 0)
            keyboard.append([InlineKeyboardButton(
                f"{emoji} {escape_markdown(product['name'])} - {quantity} шт.",
                callback_data=f"restock:{product['id']}"
            )])
    if not keyboard:
        keyboard.append([InlineKeyboardButton("📭 Нет товаров для пополнения", callback_data="no_products")])
    keyboard.append([InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")])

    await safe_edit_message_text(
        query=query,
        text="📦 **Пополнение количества товара**\n\nВыберите товар для пополнения:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def restock_product_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: str):
    query = update.callback_query
    await safe_answer_query(query)
    product = next((p for p in catalog if p['id'] == product_id), None)
    if not product:
        await safe_answer_query(query, "❌ Товар не найден", show_alert=True)
        return

    await safe_edit_message_text(
        query=query,
        text=f"📦 **Пополнение товара:** {escape_markdown(product['name'])}\n\n"
            f"📊 **Текущее количество:** {product.get('quantity', 0)} шт.\n\n"
            f"📝 **Введите новое количество:**",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_restock")]
        ])
    )
    context.user_data['awaiting_restock'] = {
        'product_id': product_id,
        'product_name': product['name']
    }

# ========== НОВЫЕ ФУНКЦИИ ДЛЯ ЗАЯВОК ==========
async def admin_requests_list_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        return

    load_data()
    if not requests_dict:
        await safe_edit_message_text(
            query=query,
            text="📋 **Нет заявок на пополнение.**",
            parse_mode='Markdown',
            reply_markup=get_admin_requests_keyboard()
        )
        return

    sorted_requests = sorted(
        requests_dict.values(),
        key=lambda x: (x.get('status') != 'pending', x.get('created_at', '')),
        reverse=False
    )

    text = "📋 **Заявки на пополнение:**\n\n"
    keyboard = []

    for req in sorted_requests:
        status_emoji = {
            'pending': '⏳',
            'approved': '✅',
            'rejected': '❌'
        }.get(req.get('status'), '❓')
        user_info = f"{escape_markdown(req.get('first_name'))} (@{escape_markdown(req.get('username'))})"
        text += f"{status_emoji} **{req.get('request_id')}** – {user_info}\n"
        text += f"   💰 {req.get('rub_amount'):.2f} RUB → {req.get('usdt_amount'):.2f} USDT\n"
        text += f"   🕐 {req.get('created_at')[:16]}\n\n"
        keyboard.append([InlineKeyboardButton(
            f"{status_emoji} Заявка {req.get('request_id')} – {escape_markdown(req.get('first_name'))}",
            callback_data=f"view_request:{req.get('request_id')}"
        )])

    keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="admin_requests_list")])
    keyboard.append([InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")])

    await safe_edit_message_text(
        query=query,
        text=text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def view_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, request_id: str):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        return

    load_data()
    req = requests_dict.get(request_id)
    if not req:
        await safe_answer_query(query, "❌ Заявка не найдена", show_alert=True)
        return

    status_emoji = {
        'pending': '⏳ Ожидает',
        'approved': '✅ Подтверждена',
        'rejected': '❌ Отклонена'
    }.get(req.get('status'), '❓ Неизвестно')

    text = (
        f"📋 **Заявка #{request_id}**\n\n"
        f"👤 **Пользователь:** {escape_markdown(req.get('first_name'))} (@{escape_markdown(req.get('username'))})\n"
        f"🆔 **ID:** `{req.get('user_id')}`\n"
        f"💰 **Сумма в RUB:** {req.get('rub_amount'):.2f} руб\n"
        f"💵 **Сумма в USDT:** {req.get('usdt_amount'):.2f} USDT\n"
        f"📅 **Создана:** {req.get('created_at')[:19]}\n"
        f"📊 **Статус:** {status_emoji}\n"
    )

    if req.get('processed_at'):
        text += f"⏱ **Обработана:** {req.get('processed_at')[:19]}\n"
    if req.get('processed_by'):
        text += f"👤 **Обработал:** `{req.get('processed_by')}`\n"

    if req.get('status') == 'pending':
        reply_markup = get_confirm_request_keyboard(request_id)
    else:
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К заявкам", callback_data="admin_requests_list")]
        ])

    await safe_edit_message_text(
        query=query,
        text=text,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def confirm_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, request_id: str):
    query = update.callback_query
    await safe_answer_query(query)
    admin_id = query.from_user.id
    if not is_admin(admin_id):
        return

    load_data()
    req = requests_dict.get(request_id)
    if not req:
        await safe_answer_query(query, "❌ Заявка не найдена", show_alert=True)
        return
    if req.get('status') != 'pending':
        await safe_answer_query(query, "❌ Заявка уже обработана", show_alert=True)
        return

    user_id = req.get('user_id')
    usdt_amount = req.get('usdt_amount')

    user_data = ensure_user_registered(user_id)
    user_data['balance_usdt'] = user_data.get('balance_usdt', 0.0) + usdt_amount
    save_users()

    req['status'] = 'approved'
    req['processed_at'] = datetime.now().isoformat()
    req['processed_by'] = admin_id
    save_requests()

    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"✅ **Ваша заявка на пополнение подтверждена!**\n\n"
                f"🆔 **Номер заявки:** `{request_id}`\n"
                f"💰 **Сумма в рублях:** {req.get('rub_amount'):.2f} руб\n"
                f"💵 **Зачислено USDT:** {usdt_amount:.2f} USDT\n"
                f"💳 **Новый баланс:** {user_data['balance_usdt']:.2f} USDT"
            ),
            parse_mode='Markdown',
            reply_markup=get_main_inline_keyboard(user_id)
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")

    await safe_answer_query(query, "✅ Заявка подтверждена, баланс пополнен", show_alert=True)
    await admin_requests_list_handler(update, context)

async def reject_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, request_id: str):
    query = update.callback_query
    await safe_answer_query(query)
    admin_id = query.from_user.id
    if not is_admin(admin_id):
        return

    load_data()
    req = requests_dict.get(request_id)
    if not req:
        await safe_answer_query(query, "❌ Заявка не найдена", show_alert=True)
        return
    if req.get('status') != 'pending':
        await safe_answer_query(query, "❌ Заявка уже обработана", show_alert=True)
        return

    req['status'] = 'rejected'
    req['processed_at'] = datetime.now().isoformat()
    req['processed_by'] = admin_id
    save_requests()

    try:
        await context.bot.send_message(
            chat_id=req.get('user_id'),
            text=(
                f"❌ **Ваша заявка на пополнение отклонена.**\n\n"
                f"🆔 **Номер заявки:** `{request_id}`\n"
                f"💰 **Сумма в рублях:** {req.get('rub_amount'):.2f} руб\n"
                f"💵 **Сумма в USDT:** {req.get('usdt_amount'):.2f} USDT\n\n"
                f"По вопросам обратитесь к администратору @{ADMIN_USERNAME}."
            ),
            parse_mode='Markdown',
            reply_markup=get_main_inline_keyboard(req.get('user_id'))
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить пользователя {req.get('user_id')}: {e}")

    await safe_answer_query(query, "❌ Заявка отклонена", show_alert=True)
    await admin_requests_list_handler(update, context)

# ========== НОВЫЕ ФУНКЦИИ ДЛЯ ПРОМОКОДОВ ==========
async def admin_manage_promocodes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        return

    await safe_edit_message_text(
        query=query,
        text="🎟️ **Управление промокодами**\n\nВыберите действие:",
        parse_mode='Markdown',
        reply_markup=get_admin_promocodes_keyboard()
    )

async def admin_create_balance_promo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        return

    await safe_edit_message_text(
        query=query,
        text="💰 **Создание денежного промокода**\n\nВведите сумму в USDT, которую получит пользователь:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_promocodes")]
        ])
    )
    context.user_data['awaiting_promo_balance_amount'] = True

async def admin_create_item_promo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        return

    products = get_promocodable_products()
    if not products:
        await safe_edit_message_text(
            query=query,
            text="❌ **Нет доступных товаров для создания промокода.**",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_promocodes")]
            ])
        )
        return

    keyboard = []
    for p in products:
        emoji = "📦" if p.get('is_bundle') else "📁"
        btn_text = f"{emoji} {escape_markdown(p['name'])} – {p['price']} USDT"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"promo_select_item:{p['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_promocodes")])

    await safe_edit_message_text(
        query=query,
        text="🎟️ **Создание товарного промокода**\n\nВыберите товар, который будет выдаваться по промокоду:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def promo_select_item_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: str):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        return

    product = next((p for p in catalog if p['id'] == product_id), None)
    if not product:
        await safe_answer_query(query, "❌ Товар не найден", show_alert=True)
        return

    context.user_data['promo_item_product'] = product_id
    await safe_edit_message_text(
        query=query,
        text=f"🎟️ **Выбран товар:** {escape_markdown(product['name'])}\n\nВведите код промокода (или оставьте пустым для автогенерации):",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_create_item_promo")]
        ])
    )
    context.user_data['awaiting_promo_item_code'] = True

async def admin_list_promocodes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        return

    if not promocodes:
        await safe_edit_message_text(
            query=query,
            text="📭 **Нет созданных промокодов.**",
            parse_mode='Markdown',
            reply_markup=get_admin_promocodes_keyboard()
        )
        return

    text = "🎟️ **Список промокодов:**\n\n"
    for code, promo in promocodes.items():
        status = "✅ Активен" if promo.get('active') else "❌ Использован"
        type_ = "💵 Денежный" if promo['type'] == 'balance' else "🎁 Товарный"
        value = promo['value']
        if promo['type'] == 'balance':
            value_str = f"{value} USDT"
        else:
            product = next((p for p in catalog if p['id'] == value), None)
            value_str = escape_markdown(product['name']) if product else "Товар удален"
        created = promo.get('created_at', '')[:10]
        used_by = promo.get('used_by')
        used_at = promo.get('used_at', [])
        used_info = ""
        if used_by and isinstance(used_by, list):
            used_info = "\n   Использовали:"
            for uid, at in zip(used_by, used_at):
                username = users.get(str(uid), {}).get('username', 'unknown')
                used_info += f"\n      • {at[:10]} – @{escape_markdown(username)} (ID: `{uid}`)"
        text += f"`{code}`\n"
        text += f"   {type_}: {value_str}\n"
        text += f"   {status}, создан {created}, активаций: {len(used_by) if isinstance(used_by, list) else 0}/{promo.get('max_uses', 0) if promo.get('max_uses', 0) > 0 else '∞'}"
        if used_info:
            text += used_info
        text += "\n\n"

    text += f"📊 **Всего промокодов:** {len(promocodes)}"

    await safe_edit_message_text(
        query=query,
        text=text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить", callback_data="admin_list_promocodes")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_manage_promocodes")]
        ])
    )

# ========== НОВЫЕ ФУНКЦИИ ДЛЯ ПРЕДЗАКАЗОВ ==========
async def my_preorders_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список предзаказов пользователя"""
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id

    user_preorders = [p for p in preorders if p.get('user_id') == user_id]
    if not user_preorders:
        await safe_edit_message_text(
            query=query,
            text="📦 **У вас нет предзаказов.**\n\nЧтобы оформить предзаказ, выберите товар в каталоге и нажмите 'Оформить предзаказ'.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🛍️ Каталог", callback_data="catalog_menu")],
                [InlineKeyboardButton("🔙 В личный кабинет", callback_data="personal_account")]
            ])
        )
        return

    user_preorders.sort(key=lambda x: x.get('created_at', ''), reverse=True)

    text = "📦 **Ваши предзаказы:**\n\n"
    keyboard = []

    for pre in user_preorders:
        product = next((p for p in catalog if p['id'] == pre['product_id']), None)
        product_name = escape_markdown(product['name']) if product else "Товар удален"
        status_emoji = {
            'pending': '⏳',
            'paid': '💳',
            'completed': '✅',
            'cancelled': '❌'
        }.get(pre['status'], '❓')
        text += f"{status_emoji} **{product_name}** (x{pre['quantity']})\n"
        text += f"   💰 {pre['total_price']} USDT\n"
        text += f"   📅 {pre['created_at'][:16]}\n"
        text += f"   Статус: {pre['status']}\n\n"
        keyboard.append([InlineKeyboardButton(
            f"{status_emoji} Предзаказ #{pre['id'][:8]}",
            callback_data=f"view_preorder:{pre['id']}"
        )])

    keyboard.append([InlineKeyboardButton("🔙 В личный кабинет", callback_data="personal_account")])

    await safe_edit_message_text(
        query=query,
        text=text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def view_preorder_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, preorder_id: str):
    """Показывает детали предзаказа"""
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id

    preorder = next((p for p in preorders if p['id'] == preorder_id), None)
    if not preorder or preorder['user_id'] != user_id:
        await safe_answer_query(query, "❌ Предзаказ не найден", show_alert=True)
        return

    product = next((p for p in catalog if p['id'] == preorder['product_id']), None)
    product_name = escape_markdown(product['name']) if product else "Товар удален"

    status_text = {
        'pending': '⏳ Ожидает оплаты',
        'paid': '💳 Оплачен, ожидает обработки',
        'completed': '✅ Выполнен',
        'cancelled': '❌ Отменён'
    }.get(preorder['status'], '❓ Неизвестно')

    text = (
        f"📦 **Предзаказ #{preorder_id[:8]}**\n\n"
        f"🛍️ **Товар:** {product_name}\n"
        f"📊 **Количество:** {preorder['quantity']}\n"
        f"💰 **Общая стоимость:** {preorder['total_price']} USDT\n"
        f"📅 **Дата создания:** {preorder['created_at'][:19]}\n"
        f"📊 **Статус:** {status_text}\n\n"
        f"📋 **Ваши данные:**\n"
        f"👤 Имя: {escape_markdown(preorder['user_data'].get('name', 'Не указано'))}\n"
        f"📞 Контакт: {escape_markdown(preorder['user_data'].get('contact', 'Не указано'))}\n"
        f"📝 Комментарий: {escape_markdown(preorder['user_data'].get('comment', 'Нет'))}\n"
    )

    keyboard = [[InlineKeyboardButton("🔙 К моим предзаказам", callback_data="my_preorders")]]
    await safe_edit_message_text(
        query=query,
        text=text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def preorder_product_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: str):
    """Начинает процесс предзаказа: запрос количества"""
    query = update.callback_query
    await safe_answer_query(query)

    # Очищаем все состояния, связанные с вводом, чтобы избежать конфликтов
    for key in list(context.user_data.keys()):
        if key.startswith('awaiting_') or key in ['preorder_product', 'preorder_quantity', 'preorder_total', 'preorder_product_id']:
            del context.user_data[key]

    product = next((p for p in catalog if p['id'] == product_id), None)
    if not product:
        await safe_answer_query(query, "❌ Товар не найден", show_alert=True)
        return

    await safe_edit_message_text(
        query=query,
        text=f"📦 **Предзаказ товара**\n\n"
             f"Товар: {escape_markdown(product['name'])}\n"
             f"Цена за единицу: {product['price']} USDT\n\n"
             f"Введите количество для предзаказа (целое число, минимум 1):",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data=f"product:{product_id}")]
        ])
    )
    context.user_data['preorder_product_id'] = product_id
    context.user_data['awaiting_preorder_quantity'] = True

async def process_preorder_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Обрабатывает ввод количества для предзаказа"""
    try:
        quantity = int(text)
        if quantity <= 0:
            await update.message.reply_text("❌ Количество должно быть больше 0. Введите целое число.")
            return
    except ValueError:
        await update.message.reply_text("❌ Пожалуйста, введите целое число.")
        return

    product_id = context.user_data.get('preorder_product_id')
    if not product_id:
        await update.message.reply_text("❌ Ошибка: товар не найден. Начните заново.")
        return

    product = next((p for p in catalog if p['id'] == product_id), None)
    if not product:
        await update.message.reply_text("❌ Товар не найден.")
        return

    total = product['price'] * quantity

    # Проверяем баланс
    user_data = ensure_user_registered(update.effective_user.id)
    if user_data['balance_usdt'] < total:
        await update.message.reply_text(
            f"❌ Недостаточно средств! Нужно: {total} USDT, у вас: {user_data['balance_usdt']:.2f} USDT.\nПополните баланс.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Пополнить баланс", callback_data="deposit_menu")],
                [InlineKeyboardButton("🔙 В каталог", callback_data="catalog_menu")]
            ])
        )
        del context.user_data['preorder_product_id']
        del context.user_data['awaiting_preorder_quantity']
        return

    # Удаляем состояние ожидания количества и устанавливаем ожидание данных
    del context.user_data['awaiting_preorder_quantity']
    context.user_data['preorder_product'] = product_id
    context.user_data['preorder_quantity'] = quantity
    context.user_data['preorder_total'] = total

    await update.message.reply_text(
        f"📦 **Оформление предзаказа**\n\n"
        f"Товар: {escape_markdown(product['name'])}\n"
        f"Количество: {quantity}\n"
        f"Сумма к оплате: {total} USDT\n\n"
        "Пожалуйста, введите ваши данные в формате:\n"
        "`Имя | Контакт | Комментарий (необязательно)`\n\n"
        "Например:\n"
        "`Иван Иванов | @ivanov | Хочу получить доставку`\n\n"
        "Или нажмите '🔙 Назад' для отмены.",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data=f"product:{product_id}")]
        ])
    )
    context.user_data['awaiting_preorder_data'] = True

async def process_preorder_data(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Обрабатывает ввод данных пользователя и создаёт предзаказ"""
    user_id = update.effective_user.id
    user_data = ensure_user_registered(user_id)

    # Проверяем наличие всех необходимых данных в контексте
    if not (context.user_data.get('preorder_product') and context.user_data.get('preorder_quantity') and context.user_data.get('preorder_total')):
        await update.message.reply_text("❌ Ошибка: данные предзаказа не найдены. Начните заново.")
        # Очищаем возможные остатки
        for key in ['preorder_product', 'preorder_quantity', 'preorder_total', 'preorder_product_id', 'awaiting_preorder_data']:
            if key in context.user_data:
                del context.user_data[key]
        return

    product_id = context.user_data['preorder_product']
    quantity = context.user_data['preorder_quantity']
    total = context.user_data['preorder_total']

    product = next((p for p in catalog if p['id'] == product_id), None)
    if not product:
        await update.message.reply_text("❌ Товар не найден.")
        return

    # Повторная проверка баланса (на случай изменения)
    if user_data['balance_usdt'] < total:
        await update.message.reply_text(
            f"❌ Недостаточно средств! Нужно: {total} USDT, у вас: {user_data['balance_usdt']:.2f} USDT.\nПополните баланс.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Пополнить баланс", callback_data="deposit_menu")],
                [InlineKeyboardButton("🔙 В каталог", callback_data="catalog_menu")]
            ])
        )
        return

    # Парсим введённые данные
    parts = [p.strip() for p in text.split('|')]
    name = escape_markdown(parts[0]) if len(parts) > 0 else "Не указано"
    contact = escape_markdown(parts[1]) if len(parts) > 1 else "Не указано"
    comment = escape_markdown(parts[2]) if len(parts) > 2 else ""

    # Списываем деньги
    user_data['balance_usdt'] -= total
    save_users()

    # Создаём предзаказ
    preorder_id = str(uuid.uuid4())[:8]
    preorder = {
        "id": preorder_id,
        "user_id": user_id,
        "product_id": product_id,
        "product_name": product['name'],
        "quantity": quantity,
        "price_per_item": product['price'],
        "total_price": total,
        "user_data": {
            "name": name,
            "contact": contact,
            "comment": comment
        },
        "status": "paid",  # сразу оплачен
        "created_at": datetime.now().isoformat(),
        "processed_at": None,
        "processed_by": None
    }
    preorders.append(preorder)
    save_preorders()

    # Уведомление пользователю
    await update.message.reply_text(
        f"✅ **Предзаказ успешно оформлен!**\n\n"
        f"🆔 **Номер предзаказа:** `{preorder_id}`\n"
        f"🛍️ **Товар:** {escape_markdown(product['name'])}\n"
        f"📊 **Количество:** {quantity}\n"
        f"💰 **Сумма:** {total} USDT\n"
        f"💳 **Новый баланс:** {user_data['balance_usdt']:.2f} USDT\n\n"
        f"Ваши данные:\n"
        f"👤 Имя: {name}\n"
        f"📞 Контакт: {contact}\n"
        f"📝 Комментарий: {comment if comment else 'Нет'}\n\n"
        f"Статус предзаказа: **оплачен, ожидает обработки**.\n"
        f"Вы можете отслеживать его в разделе «Мои предзаказы».",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📦 Мои предзаказы", callback_data="my_preorders")],
            [InlineKeyboardButton("🔙 В каталог", callback_data="catalog_menu")]
        ])
    )

    # Уведомление администраторам
    for admin_id in admins:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"📦 **Новый предзаказ!**\n\n"
                    f"🆔 **Номер:** `{preorder_id}`\n"
                    f"👤 **Пользователь:** {escape_markdown(user_data.get('first_name'))} (@{escape_markdown(user_data.get('username'))})\n"
                    f"🆔 **ID:** `{user_id}`\n"
                    f"🛍️ **Товар:** {escape_markdown(product['name'])}\n"
                    f"📊 **Количество:** {quantity}\n"
                    f"💰 **Сумма:** {total} USDT\n"
                    f"📋 **Данные:**\n"
                    f"Имя: {name}\n"
                    f"Контакт: {contact}\n"
                    f"Комментарий: {comment if comment else 'Нет'}\n\n"
                    f"Для обработки перейдите в админ-панель → Предзаказы."
                ),
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 К предзаказам", callback_data="admin_preorders_list")]
                ])
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить админа {admin_id}: {e}")

    # Очищаем данные пользователя
    for key in ['preorder_product', 'preorder_quantity', 'preorder_total', 'preorder_product_id', 'awaiting_preorder_data']:
        if key in context.user_data:
            del context.user_data[key]

# ========== АДМИН ФУНКЦИИ ДЛЯ ПРЕДЗАКАЗОВ ==========
async def admin_preorders_list_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список всех предзаказов для администратора"""
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        return

    load_data()

    if not preorders:
        await safe_edit_message_text(
            query=query,
            text="📦 **Нет предзаказов.**",
            parse_mode='Markdown',
            reply_markup=get_admin_preorders_keyboard()
        )
        return

    # Сортируем: сначала необработанные (paid, pending), потом остальные
    sorted_preorders = sorted(
        preorders,
        key=lambda x: (
            x.get('status') not in ['paid', 'pending'],
            x.get('created_at', '')
        )
    )

    text = "📦 **Все предзаказы:**\n\n"
    keyboard = []

    for pre in sorted_preorders:
        user = users.get(str(pre['user_id']), {})
        user_name = escape_markdown(user.get('first_name', 'Unknown'))
        status_emoji = {
            'pending': '⏳',
            'paid': '💳',
            'completed': '✅',
            'cancelled': '❌'
        }.get(pre['status'], '❓')
        text += f"{status_emoji} **{pre['id'][:8]}** – {user_name}\n"
        text += f"   🛍️ {escape_markdown(pre['product_name'])} x{pre['quantity']} = {pre['total_price']} USDT\n"
        text += f"   📅 {pre['created_at'][:16]}\n\n"
        keyboard.append([InlineKeyboardButton(
            f"{status_emoji} Предзаказ {pre['id'][:8]} – {user_name}",
            callback_data=f"view_admin_preorder:{pre['id']}"
        )])

    keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="admin_preorders_list")])
    keyboard.append([InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")])

    await safe_edit_message_text(
        query=query,
        text=text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def view_admin_preorder_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, preorder_id: str):
    """Просмотр деталей предзаказа администратором"""
    query = update.callback_query
    await safe_answer_query(query)
    user_id = query.from_user.id
    if not is_admin(user_id):
        return

    preorder = next((p for p in preorders if p['id'] == preorder_id), None)
    if not preorder:
        await safe_answer_query(query, "❌ Предзаказ не найден", show_alert=True)
        return

    user = users.get(str(preorder['user_id']), {})
    user_name = escape_markdown(user.get('first_name', 'Unknown'))
    username = escape_markdown(user.get('username', 'unknown'))

    status_text = {
        'pending': '⏳ Ожидает оплаты',
        'paid': '💳 Оплачен, ожидает обработки',
        'completed': '✅ Выполнен',
        'cancelled': '❌ Отменён'
    }.get(preorder['status'], '❓ Неизвестно')

    text = (
        f"📦 **Предзаказ #{preorder_id[:8]}**\n\n"
        f"👤 **Пользователь:** {user_name} (@{username})\n"
        f"🆔 **ID:** `{preorder['user_id']}`\n"
        f"🛍️ **Товар:** {escape_markdown(preorder['product_name'])}\n"
        f"📊 **Количество:** {preorder['quantity']}\n"
        f"💰 **Цена за ед.:** {preorder['price_per_item']} USDT\n"
        f"💵 **Общая стоимость:** {preorder['total_price']} USDT\n"
        f"📅 **Создан:** {preorder['created_at'][:19]}\n"
        f"📊 **Статус:** {status_text}\n\n"
        f"📋 **Данные пользователя:**\n"
        f"👤 Имя: {escape_markdown(preorder['user_data'].get('name', 'Не указано'))}\n"
        f"📞 Контакт: {escape_markdown(preorder['user_data'].get('contact', 'Не указано'))}\n"
        f"📝 Комментарий: {escape_markdown(preorder['user_data'].get('comment', 'Нет'))}\n"
    )

    if preorder.get('processed_at'):
        text += f"\n⏱ **Обработан:** {preorder['processed_at'][:19]}\n"
    if preorder.get('processed_by'):
        text += f"👤 **Обработал:** `{preorder['processed_by']}`\n"

    if preorder['status'] in ['paid']:
        reply_markup = get_confirm_preorder_keyboard(preorder_id)
    else:
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К предзаказам", callback_data="admin_preorders_list")]
        ])

    await safe_edit_message_text(
        query=query,
        text=text,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def confirm_preorder_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, preorder_id: str):
    """Подтверждение предзаказа (перевод в completed)"""
    query = update.callback_query
    await safe_answer_query(query)
    admin_id = query.from_user.id
    if not is_admin(admin_id):
        return

    preorder = next((p for p in preorders if p['id'] == preorder_id), None)
    if not preorder:
        await safe_answer_query(query, "❌ Предзаказ не найден", show_alert=True)
        return

    if preorder['status'] != 'paid':
        await safe_answer_query(query, "❌ Предзаказ не в статусе 'paid'", show_alert=True)
        return

    preorder['status'] = 'completed'
    preorder['processed_at'] = datetime.now().isoformat()
    preorder['processed_by'] = admin_id
    save_preorders()

    # Уведомляем пользователя
    try:
        await context.bot.send_message(
            chat_id=preorder['user_id'],
            text=(
                f"✅ **Ваш предзаказ #{preorder_id[:8]} подтверждён!**\n\n"
                f"🛍️ **Товар:** {escape_markdown(preorder['product_name'])}\n"
                f"📊 **Количество:** {preorder['quantity']}\n"
                f"💰 **Сумма:** {preorder['total_price']} USDT\n\n"
                f"Спасибо за ожидание! Если товар требует передачи, администратор свяжется с вами."
            ),
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📦 Мои предзаказы", callback_data="my_preorders")]
            ])
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить пользователя {preorder['user_id']}: {e}")

    await safe_answer_query(query, "✅ Предзаказ подтверждён", show_alert=True)
    await admin_preorders_list_handler(update, context)

async def reject_preorder_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, preorder_id: str):
    """Отмена предзаказа с возвратом денег"""
    query = update.callback_query
    await safe_answer_query(query)
    admin_id = query.from_user.id
    if not is_admin(admin_id):
        return

    preorder = next((p for p in preorders if p['id'] == preorder_id), None)
    if not preorder:
        await safe_answer_query(query, "❌ Предзаказ не найден", show_alert=True)
        return

    if preorder['status'] != 'paid':
        await safe_answer_query(query, "❌ Предзаказ не в статусе 'paid'", show_alert=True)
        return

    # Возвращаем деньги
    user_data = ensure_user_registered(preorder['user_id'])
    user_data['balance_usdt'] += preorder['total_price']
    save_users()

    preorder['status'] = 'cancelled'
    preorder['processed_at'] = datetime.now().isoformat()
    preorder['processed_by'] = admin_id
    save_preorders()

    # Уведомляем пользователя
    try:
        await context.bot.send_message(
            chat_id=preorder['user_id'],
            text=(
                f"❌ **Ваш предзаказ #{preorder_id[:8]} отменён.**\n\n"
                f"🛍️ **Товар:** {escape_markdown(preorder['product_name'])}\n"
                f"📊 **Количество:** {preorder['quantity']}\n"
                f"💰 **Сумма возврата:** {preorder['total_price']} USDT\n"
                f"💳 **Новый баланс:** {user_data['balance_usdt']:.2f} USDT\n\n"
                f"По вопросам обратитесь к администратору @{ADMIN_USERNAME}."
            ),
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📦 Мои предзаказы", callback_data="my_preorders")]
            ])
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить пользователя {preorder['user_id']}: {e}")

    await safe_answer_query(query, "❌ Предзаказ отменён, деньги возвращены", show_alert=True)
    await admin_preorders_list_handler(update, context)

# ========== БЕЗОПАСНЫЕ ФУНКЦИИ ==========
async def safe_answer_query(query, text=None, show_alert=False):
    try:
        await query.answer(text=text, show_alert=show_alert)
    except BadRequest as e:
        if "Query is too old" in str(e) or "query id is invalid" in str(e):
            logger.warning(f"Старый callback_query: {query.data} - игнорируем")
            return
        raise

async def safe_edit_message_text(query, text, parse_mode=None, reply_markup=None, disable_web_page_preview=None):
    try:
        await query.edit_message_text(
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview
        )
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.debug("Сообщение не изменилось - игнорируем")
            return
        elif "Message to edit not found" in str(e):
            logger.warning("Сообщение для редактирования не найдено")
            return
        raise

# ========== ОБРАБОТЧИК INLINE КНОПОК ==========
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await safe_answer_query(query)
    except Exception as e:
        logger.error(f"Ошибка при ответе на callback_query: {e}")
        return

    data = query.data
    user_id = query.from_user.id
    logger.info(f"Обработка callback_data: {data} от пользователя {user_id}")

    try:
        # Основные пользовательские кнопки
        if data == "back_to_menu" or data == "back_to_main":
            await start(update, context)
            return
        elif data == "catalog_menu":
            await catalog_handler(update, context)
            return
        elif data == "personal_account":
            await personal_account_handler(update, context)
            return
        elif data == "available_products":
            await available_products_handler(update, context)
            return
        elif data == "support":
            await support_handler(update, context)
            return
        elif data == "admin_panel":
            await admin_panel_handler(update, context)
            return
        elif data == "back_to_categories":
            await catalog_handler(update, context)
            return
        elif data == "back_to_catalog":
            await catalog_handler(update, context)
            return
        elif data == "deposit_menu":
            await deposit_handler(update, context)
            return
        elif data == "deposit_crypto":
            await deposit_crypto_handler(update, context)
            return
        elif data == "deposit_admin":
            await deposit_admin_handler(update, context)
            return
        elif data == "deposit_admin_start":
            await deposit_admin_start_handler(update, context)
            return
        elif data == "my_invoices":
            await invoices_handler(update, context)
            return
        elif data == "my_purchases":
            await my_purchases_handler(update, context)
            return
        elif data.startswith("purchases_page:"):
            page = int(data.split(":", 1)[1])
            await my_purchases_handler(update, context, page)
            return
        elif data.startswith("show_purchase:"):
            purchase_id = data.split(":", 1)[1]
            await show_purchase_detail(update, context, purchase_id)
            return
        elif data.startswith("download_purchase:"):
            purchase_id = data.split(":", 1)[1]
            await download_purchase_file(update, context, purchase_id)
            return
        elif data == "cancel_invoices":
            await cancel_invoices_list_handler(update, context)
            return
        elif data.startswith("cancel_invoice:"):
            invoice_id = data.split(":", 1)[1]
            await cancel_invoice_handler(update, context, invoice_id)
            return
        elif data == "promo_code":
            await promo_code_handler(update, context)
            return

        # Категории и товары
        elif data.startswith("category_"):
            category_name = data.split("_", 1)[1]
            await catalog_category_handler(update, context, category_name)
            return
        elif data.startswith("category_types_"):
            category_name = data.split("_", 2)[2]
            await category_types_handler(update, context, category_name)
            return
        elif data.startswith("cat_"):
            category_name = data.split("_", 1)[1]
            await subcategory_handler(update, context, category_name)
            return
        elif data.startswith("subcategory_"):
            parts = data.split("_", 2)
            if len(parts) == 3:
                category_name = parts[1]
                subcategory_name = parts[2]
                await catalog_subcategory_handler(update, context, category_name, subcategory_name)
            return
        elif data.startswith("subcat_"):
            parts = data.split("_", 2)
            if len(parts) == 3:
                category_name = parts[1]
                subcategory_name = parts[2]
                await catalog_subcategory_handler(update, context, category_name, subcategory_name)
            return
        elif data.startswith("type_"):
            parts = data.split("_", 3)
            if len(parts) == 4:
                category_name = parts[1]
                subcategory_name = parts[2]
                type_name = parts[3]
                await catalog_type_handler(update, context, category_name, subcategory_name, type_name)
            return
        elif data.startswith("type_no_subcat_"):
            parts = data.split("_", 3)
            if len(parts) == 5:
                category_name = parts[3]
                type_name = parts[4]
                await catalog_type_no_subcategory_handler(update, context, category_name, type_name)
            return
        elif data.startswith("product:"):
            product_id = data.split(":", 1)[1]
            await show_product_details(update, context, product_id)
            return
        elif data.startswith("product_back:"):
            product_id = data.split(":", 1)[1]
            await show_product_details(update, context, product_id)
            return
        elif data.startswith("buy_product:"):
            product_id = data.split(":", 1)[1]
            # Если нажата кнопка "Купить", запрашиваем количество
            load_data()
            product = next((p for p in catalog if p['id'] == product_id), None)
            if product:
                if product.get('is_bundle', False):
                    max_qty = get_available_files_count(product.get('bundle_files', []))
                else:
                    max_qty = product.get('quantity', 1)
                if max_qty <= 0:
                    await safe_answer_query(query, "❌ Товар временно отсутствует!", show_alert=True)
                    return
                await safe_edit_message_text(
                    query=query,
                    text=f"🛒 **Выберите количество для покупки**\n\n"
                         f"Товар: {escape_markdown(product['name'])}\n"
                         f"Цена за единицу: {product['price']} USDT\n"
                         f"Доступно: {max_qty}",
                    parse_mode='Markdown',
                    reply_markup=get_quantity_selection_keyboard(product_id, max_qty)
                )
            else:
                await safe_answer_query(query, "❌ Товар не найден", show_alert=True)
            return
        elif data.startswith("buy_qty:"):
            parts = data.split(":")
            if len(parts) == 3:
                product_id = parts[1]
                quantity = int(parts[2])
                await handle_quantity_selection(update, context, product_id, quantity)
            return
        elif data.startswith("custom_qty:"):
            product_id = data.split(":", 1)[1]
            await safe_edit_message_text(
                query=query,
                text="🔢 **Введите количество для покупки:**\n\n"
                     "Введите целое число (например: 3, 5, 10)\n"
                     "Или 'отмена' для возврата назад",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data=f"product:{product_id}")]
                ])
            )
            context.user_data['awaiting_custom_quantity'] = product_id
            return
        elif data.startswith("confirm_purchase:"):
            parts = data.split(":")
            if len(parts) == 3:
                product_id = parts[1]
                quantity = int(parts[2])
                await process_purchase(update, context, product_id, quantity)
            return

        # Предзаказы пользователя
        elif data == "my_preorders":
            await my_preorders_handler(update, context)
            return
        elif data.startswith("view_preorder:"):
            preorder_id = data.split(":", 1)[1]
            await view_preorder_handler(update, context, preorder_id)
            return
        elif data.startswith("preorder_product:"):
            product_id = data.split(":", 1)[1]
            await preorder_product_handler(update, context, product_id)
            return

        # Админские кнопки
        if is_admin(user_id):
            # Управление категориями
            if data == "admin_create_product":
                await admin_create_product_handler(update, context)
                return
            elif data == "admin_attach_file":
                await admin_attach_file_handler(update, context)
                return
            elif data == "admin_manage_categories":
                await admin_manage_categories_handler(update, context)
                return
            elif data == "admin_add_category":
                await admin_add_category_handler(update, context)
                return
            elif data == "admin_add_subcategory":
                await admin_add_subcategory_handler(update, context)
                return
            elif data == "admin_add_type_to_category":
                await admin_add_type_to_category_handler(update, context)
                return
            elif data == "admin_add_type_to_subcategory":
                await admin_add_type_to_subcategory_handler(update, context)
                return
            elif data == "admin_delete_subcategory":
                await admin_delete_subcategory_handler(update, context)
                return
            elif data.startswith("select_category_for_subcat:"):
                category = data.split(":", 1)[1]
                await select_category_for_subcategory_handler(update, context, category)
                return
            elif data.startswith("select_category_for_type:"):
                category = data.split(":", 1)[1]
                await select_category_for_type_handler(update, context, category)
                return
            elif data.startswith("select_category_for_type_to_category:"):
                category = data.split(":", 1)[1]
                await select_category_for_type_to_category_handler(update, context, category)
                return
            elif data.startswith("select_category_for_delete_subcat:"):
                category = data.split(":", 1)[1]
                await select_category_for_delete_subcat_handler(update, context, category)
                return
            elif data.startswith("select_subcategory_for_type:"):
                parts = data.split(":", 2)
                if len(parts) == 3:
                    category = parts[1]
                    subcategory = parts[2]
                    await select_subcategory_for_type_handler(update, context, category, subcategory)
                return
            elif data.startswith("select_subcategory_for_delete:"):
                parts = data.split(":", 2)
                if len(parts) == 3:
                    category = parts[1]
                    subcategory = parts[2]
                    await select_subcategory_for_delete_handler(update, context, category, subcategory)
                return
            elif data == "admin_view_structure":
                await admin_view_structure_handler(update, context)
                return
            elif data == "admin_delete_category":
                await admin_delete_category_handler(update, context)
                return
            elif data.startswith("confirm_delete_category:"):
                category = data.split(":", 1)[1]
                await confirm_delete_category_handler(update, context, category)
                return
            elif data.startswith("final_delete_category:"):
                category = data.split(":", 1)[1]
                await final_delete_category_handler(update, context, category)
                return
            elif data.startswith("final_delete_subcategory:"):
                parts = data.split(":", 2)
                if len(parts) == 3:
                    category = parts[1]
                    subcategory = parts[2]
                    await final_delete_subcategory_handler(update, context, category, subcategory)
                return
            elif data == "admin_bulk_upload_to_type":
                await admin_bulk_upload_to_type_handler(update, context)
                return
            elif data.startswith("bulk_select_category:"):
                category = data.split(":", 1)[1]
                await bulk_select_category_handler(update, context, category)
                return
            elif data.startswith("bulk_category_types:"):
                category = data.split(":", 1)[1]
                await bulk_category_types_handler(update, context, category)
                return
            elif data.startswith("bulk_select_subcategory:"):
                parts = data.split(":", 2)
                if len(parts) == 3:
                    category = parts[1]
                    subcategory = parts[2]
                    await bulk_select_subcategory_handler(update, context, category, subcategory)
                return
            elif data.startswith("bulk_select_category_type:"):
                parts = data.split(":", 2)
                if len(parts) == 3:
                    category = parts[1]
                    type_ = parts[2]
                    await bulk_select_category_type_handler(update, context, category, type_)
                return
            elif data.startswith("bulk_select_type:"):
                parts = data.split(":", 3)
                if len(parts) == 4:
                    category = parts[1]
                    subcategory = parts[2]
                    type_ = parts[3]
                    await bulk_select_type_handler(update, context, category, subcategory, type_)
                return
            elif data == "admin_restock":
                await admin_restock_handler(update, context)
                return
            elif data == "admin_delete_product":
                await admin_delete_product_handler(update, context)
                return
            elif data == "admin_add_balance":
                await admin_add_balance_handler(update, context)
                return
            elif data == "admin_remove_balance":
                await admin_remove_balance_handler(update, context)
                return
            elif data == "admin_manage_admins":
                await admin_manage_admins_handler(update, context)
                return
            elif data == "admin_add_admin":
                await admin_add_admin_handler(update, context)
                return
            elif data == "admin_remove_admin":
                await admin_remove_admin_handler(update, context)
                return
            elif data == "admin_list_admins":
                await admin_list_admins_handler(update, context)
                return
            # Промокоды
            elif data == "admin_manage_promocodes":
                await admin_manage_promocodes_handler(update, context)
                return
            elif data == "admin_create_balance_promo":
                await admin_create_balance_promo_handler(update, context)
                return
            elif data == "admin_create_item_promo":
                await admin_create_item_promo_handler(update, context)
                return
            elif data == "admin_list_promocodes":
                await admin_list_promocodes_handler(update, context)
                return
            elif data.startswith("promo_select_item:"):
                product_id = data.split(":", 1)[1]
                await promo_select_item_handler(update, context, product_id)
                return
            # Заявки
            elif data == "admin_requests_list":
                await admin_requests_list_handler(update, context)
                return
            elif data.startswith("view_request:"):
                request_id = data.split(":", 1)[1]
                await view_request_handler(update, context, request_id)
                return
            elif data.startswith("confirm_request:"):
                request_id = data.split(":", 1)[1]
                await confirm_request_handler(update, context, request_id)
                return
            elif data.startswith("reject_request:"):
                request_id = data.split(":", 1)[1]
                await reject_request_handler(update, context, request_id)
                return
            # Предзаказы админа
            elif data == "admin_preorders_list":
                await admin_preorders_list_handler(update, context)
                return
            elif data.startswith("view_admin_preorder:"):
                preorder_id = data.split(":", 1)[1]
                await view_admin_preorder_handler(update, context, preorder_id)
                return
            elif data.startswith("confirm_preorder:"):
                preorder_id = data.split(":", 1)[1]
                await confirm_preorder_handler(update, context, preorder_id)
                return
            elif data.startswith("reject_preorder:"):
                preorder_id = data.split(":", 1)[1]
                await reject_preorder_handler(update, context, preorder_id)
                return
            elif data == "back_to_admin":
                await admin_panel_handler(update, context)
                return
            elif data.startswith("attach_to:"):
                product_id = data.split(":", 1)[1]
                load_data()
                product = next((p for p in catalog if p['id'] == product_id), None)
                if product:
                    await safe_edit_message_text(
                        query=query,
                        text=f"📦 **Товар:** {escape_markdown(product['name'])}\n"
                             f"🆔 **ID:** `{product_id}`\n"
                             f"💰 **Цена:** {product['price']} USDT\n\n"
                             f"📤 **Теперь отправьте .txt файл для этого товара:**",
                        parse_mode='Markdown',
                        reply_markup=get_back_to_admin_keyboard()
                    )
                    context.user_data['awaiting_file_for_product'] = product_id
                else:
                    await safe_answer_query(query, "❌ Товар не найден", show_alert=True)
                return
            elif data.startswith("delete_product:"):
                product_id = data.split(":", 1)[1]
                await delete_product_handler(update, context, product_id)
                return
            elif data.startswith("restock:"):
                product_id = data.split(":", 1)[1]
                await restock_product_handler(update, context, product_id)
                return
            elif data == "no_products":
                await safe_edit_message_text(
                    query=query,
                    text="ℹ️ **Нет товаров для прикрепления файла.**",
                    parse_mode='Markdown',
                    reply_markup=get_admin_keyboard()
                )
                return
            elif data == "no_products_delete":
                await safe_edit_message_text(
                    query=query,
                    text="ℹ️ **Нет товаров для удаления.**",
                    parse_mode='Markdown',
                    reply_markup=get_admin_keyboard()
                )
                return
            elif data == "admin_crypto_stats":
                try:
                    load_data()
                    stats_text = "👑 **CryptoBot Статистика**\n\n"
                    active_invoices = sum(1 for inv in invoices.values() if inv.get('status') == 'active')
                    paid_invoices = sum(1 for inv in invoices.values() if inv.get('status') == 'paid')
                    expired_invoices = sum(1 for inv in invoices.values() if inv.get('status') == 'expired')
                    total_usdt_amount = sum(inv.get('amount', 0) for inv in invoices.values() if inv.get('status') == 'paid')
                    stats_text += f"📊 **Счета:**\n"
                    stats_text += f"• Активные: {active_invoices}\n"
                    stats_text += f"• Оплаченные: {paid_invoices}\n"
                    stats_text += f"• Истекшие: {expired_invoices}\n\n"
                    stats_text += f"💰 **Общая сумма:**\n"
                    stats_text += f"• В USDT: {total_usdt_amount:.2f}"
                    await safe_edit_message_text(
                        query=query,
                        text=stats_text,
                        parse_mode='Markdown',
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔄 Обновить", callback_data="admin_crypto_stats")],
                            [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")]
                        ])
                    )
                except Exception as e:
                    logger.error(f"Ошибка получения статистики CryptoBot: {e}")
                    await safe_edit_message_text(
                        query=query,
                        text=f"❌ **Ошибка получения статистики:**\n\n`{str(e)}`",
                        parse_mode='Markdown',
                        reply_markup=get_admin_keyboard()
                    )
                return

    except Exception as e:
        logger.error(f"Ошибка в обработчике кнопок: {e}")
        await safe_answer_query(query, "❌ Произошла ошибка", show_alert=True)

# ========== ОБРАБОТЧИК СООБЩЕНИЙ ==========
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    logger.info(f"Получено сообщение от {user_id}: {text}")

    update_user_info(
        user_id=user_id,
        username=update.effective_user.username,
        first_name=update.effective_user.first_name
    )

    if text.lower() in ['отмена', 'cancel', 'назад', 'back']:
        for key in list(context.user_data.keys()):
            if key.startswith('awaiting_'):
                del context.user_data[key]
        await update.message.reply_text(
            "❌ Действие отменено.",
            reply_markup=get_main_inline_keyboard(user_id)
        )
        return

    # Предзаказ: ввод количества
    if context.user_data.get('awaiting_preorder_quantity'):
        await process_preorder_quantity(update, context, text)
        return

    # Предзаказ: ввод данных
    if context.user_data.get('awaiting_preorder_data'):
        await process_preorder_data(update, context, text)
        return

    # Промокод: ввод кода
    if context.user_data.get('awaiting_promo_code'):
        success, msg = await activate_promocode(user_id, text, context)
        await update.message.reply_text(
            msg,
            parse_mode='Markdown',
            reply_markup=get_main_inline_keyboard(user_id)
        )
        del context.user_data['awaiting_promo_code']
        return

    # Админ: создание денежного промокода
    if context.user_data.get('awaiting_promo_balance_amount') and is_admin(user_id):
        try:
            amount = float(text.replace(',', '.'))
            if amount <= 0:
                await update.message.reply_text("❌ Сумма должна быть больше 0.")
                return
            context.user_data['promo_balance_amount'] = amount
            context.user_data['awaiting_promo_balance_limit'] = True
            del context.user_data['awaiting_promo_balance_amount']
            await update.message.reply_text(
                f"💰 Сумма: {amount} USDT\n\nВведите максимальное количество активаций (целое число, 0 = безлимит):",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data="admin_manage_promocodes")]
                ])
            )
        except ValueError:
            await update.message.reply_text("❌ Неверный формат суммы. Введите число.")
        return

    if context.user_data.get('awaiting_promo_balance_limit') and is_admin(user_id):
        try:
            limit = int(text)
            if limit < 0:
                await update.message.reply_text("❌ Лимит не может быть отрицательным.")
                return
            context.user_data['promo_balance_limit'] = limit
            context.user_data['awaiting_promo_balance_code'] = True
            del context.user_data['awaiting_promo_balance_limit']
            await update.message.reply_text(
                f"💰 Сумма: {context.user_data['promo_balance_amount']} USDT\n"
                f"📊 Лимит: {limit if limit > 0 else 'безлимит'}\n\n"
                f"Введите код промокода (или оставьте пустым для автогенерации):",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data="admin_manage_promocodes")]
                ])
            )
        except ValueError:
            await update.message.reply_text("❌ Неверный формат лимита. Введите целое число.")
        return

    if context.user_data.get('awaiting_promo_balance_code') and is_admin(user_id):
        code = text.strip().upper()
        if not code:
            code = generate_promo_code()
        amount = context.user_data.get('promo_balance_amount')
        limit = context.user_data.get('promo_balance_limit', 0)
        promocodes[code] = {
            'code': code,
            'type': 'balance',
            'value': amount,
            'max_uses': limit,
            'used_count': 0,
            'created_by': user_id,
            'created_at': datetime.now().isoformat(),
            'active': True
        }
        save_promocodes()
        del context.user_data['awaiting_promo_balance_code']
        del context.user_data['promo_balance_amount']
        del context.user_data['promo_balance_limit']
        await update.message.reply_text(
            f"✅ **Промокод создан!**\n\nКод: `{code}`\nТип: денежный\nСумма: {amount} USDT\nЛимит: {limit if limit > 0 else 'безлимит'}",
            parse_mode='Markdown',
            reply_markup=get_admin_promocodes_keyboard()
        )
        return

    # Админ: создание товарного промокода (после выбора товара запрашиваем лимит)
    if context.user_data.get('promo_item_product') and context.user_data.get('awaiting_promo_item_limit') is None and is_admin(user_id):
        # Этот блок не нужен, лимит запрашивается после выбора товара
        pass

    if context.user_data.get('awaiting_promo_item_limit') and is_admin(user_id):
        try:
            limit = int(text)
            if limit < 0:
                await update.message.reply_text("❌ Лимит не может быть отрицательным.")
                return
            context.user_data['promo_item_limit'] = limit
            context.user_data['awaiting_promo_item_code'] = True
            del context.user_data['awaiting_promo_item_limit']
            await update.message.reply_text(
                f"📊 Лимит: {limit if limit > 0 else 'безлимит'}\n\n"
                f"Введите код промокода (или оставьте пустым для автогенерации):",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Отмена", callback_data="admin_manage_promocodes")]
                ])
            )
        except ValueError:
            await update.message.reply_text("❌ Неверный формат лимита. Введите целое число.")
        return

    if context.user_data.get('awaiting_promo_item_code') and is_admin(user_id):
        code = text.strip().upper()
        if not code:
            code = generate_promo_code()
        product_id = context.user_data.get('promo_item_product')
        limit = context.user_data.get('promo_item_limit', 0)
        promocodes[code] = {
            'code': code,
            'type': 'item',
            'value': product_id,
            'max_uses': limit,
            'used_count': 0,
            'created_by': user_id,
            'created_at': datetime.now().isoformat(),
            'active': True
        }
        save_promocodes()
        del context.user_data['awaiting_promo_item_code']
        del context.user_data['promo_item_product']
        del context.user_data['promo_item_limit']
        product = next((p for p in catalog if p['id'] == product_id), None)
        product_name = escape_markdown(product['name']) if product else "Товар"
        await update.message.reply_text(
            f"✅ **Промокод создан!**\n\nКод: `{code}`\nТип: товарный\nТовар: {product_name}\nЛимит: {limit if limit > 0 else 'безлимит'}",
            parse_mode='Markdown',
            reply_markup=get_admin_promocodes_keyboard()
        )
        return

    # Заявка: ввод суммы в рублях
    if context.user_data.get('awaiting_rub_amount'):
        try:
            rub_amount = float(text.replace(',', '.'))
            if rub_amount <= 0:
                await update.message.reply_text("❌ Сумма должна быть больше 0. Введите сумму в рублях.")
                return
            if rub_amount > 1000000:
                await update.message.reply_text("❌ Слишком большая сумма. Максимум 1 000 000 руб.")
                return
            await process_rub_amount(update, context, rub_amount)
        except ValueError:
            await update.message.reply_text("❌ Неверный формат суммы. Введите число (например: 500).")
        return

    # Пополнение USDT
    if context.user_data.get('awaiting_usdt_amount'):
        if text.lower() in ['назад', 'back', 'отмена', 'cancel', '🔙 назад']:
            if 'awaiting_usdt_amount' in context.user_data:
                del context.user_data['awaiting_usdt_amount']
            await update.message.reply_text(
                "❌ **Ввод суммы отменен.**\n\nВозвращаюсь к выбору способа оплаты...",
                parse_mode='Markdown',
                reply_markup=get_payment_methods_keyboard()
            )
            return

        try:
            amount = float(text.replace(',', '.'))
            if amount < 1:
                await update.message.reply_text(
                    "❌ **Минимальная сумма: 1 USDT**\n\nВведите сумму больше или равную 1 USDT\nИли нажмите '🔙 Назад' для отмены",
                    parse_mode='Markdown',
                    reply_markup=get_deposit_crypto_keyboard()
                )
                return
            if amount > 1000:
                await update.message.reply_text(
                    "❌ **Максимальная сумма: 1000 USDT**\n\nВведите сумму меньше или равную 1000 USDT\nИли нажмите '🔙 Назад' для отмены",
                    parse_mode='Markdown',
                    reply_markup=get_deposit_crypto_keyboard()
                )
                return
            await create_usdt_invoice(update, context, amount)
            if 'awaiting_usdt_amount' in context.user_data:
                del context.user_data['awaiting_usdt_amount']
        except ValueError:
            await update.message.reply_text(
                "❌ **Неверный формат суммы!**\n\nПожалуйста, введите число (например: 10.5)\nМожно использовать точку или запятую как разделитель\n\nИли нажмите '🔙 Назад' для отмены",
                parse_mode='Markdown',
                reply_markup=get_deposit_crypto_keyboard()
            )
        return

    elif context.user_data.get('awaiting_product_data') and is_admin(user_id):
        await process_product_creation(update, context, text)
        return
    elif context.user_data.get('awaiting_admin_add_balance') and is_admin(user_id):
        await process_admin_add_balance(update, context, text)
        return
    elif context.user_data.get('awaiting_admin_remove_balance') and is_admin(user_id):
        await process_admin_remove_balance(update, context, text)
        return
    elif context.user_data.get('awaiting_admin_id') and is_bot_owner(user_id):
        action = context.user_data['awaiting_admin_id']
        if text.lower() in ['отмена', 'cancel', 'назад', 'back']:
            del context.user_data['awaiting_admin_id']
            await admin_manage_admins_handler(update, context)
            return
        try:
            new_admin_id = int(text)
            if action == 'add':
                if new_admin_id in admins:
                    await update.message.reply_text("❌ Этот пользователь уже является администратором")
                    return
                if add_admin(new_admin_id):
                    await update.message.reply_text(
                        f"✅ **Пользователь {new_admin_id} добавлен в администраторы!**\n\nТеперь он имеет доступ к админ-панели.",
                        parse_mode='Markdown',
                        reply_markup=get_admin_management_keyboard()
                    )
                    try:
                        await context.bot.send_message(
                            chat_id=new_admin_id,
                            text=f"👑 **Вы стали администратором бота!**\n\nВладелец бота назначил вас администратором.\nИспользуйте команду /admin для доступа к панели управления.",
                            parse_mode='Markdown',
                            reply_markup=get_main_inline_keyboard(new_admin_id)
                        )
                    except Exception as e:
                        logger.error(f"Не удалось уведомить пользователя {new_admin_id}: {e}")
                else:
                    await update.message.reply_text("❌ Ошибка добавления администратора")
            elif action == 'remove':
                if new_admin_id == BOT_OWNER_ID:
                    await update.message.reply_text("❌ Нельзя удалить владельца бота")
                    return
                if new_admin_id not in admins:
                    await update.message.reply_text("❌ Этот пользователь не является администратором")
                    return
                if remove_admin(new_admin_id):
                    admin_info = users.get(str(new_admin_id), {})
                    username = escape_markdown(admin_info.get('username', 'unknown'))
                    first_name = escape_markdown(admin_info.get('first_name', 'User'))
                    await update.message.reply_text(
                        f"✅ **Администратор {first_name} удален!**\n\n"
                        f"👤 **Имя:** {first_name}\n"
                        f"📧 **Username:** @{username}\n"
                        f"🆔 **ID:** `{new_admin_id}`\n\n"
                        f"Пользователь больше не имеет прав администратора.",
                        parse_mode='Markdown',
                        reply_markup=get_admin_management_keyboard()
                    )
                    try:
                        await context.bot.send_message(
                            chat_id=new_admin_id,
                            text=f"👑 **Вы больше не администратор бота.**\n\nВладелец бота снял с вас права администратора.",
                            parse_mode='Markdown',
                            reply_markup=get_main_inline_keyboard(new_admin_id)
                        )
                    except Exception as e:
                        logger.error(f"Не удалось уведомить пользователя {new_admin_id}: {e}")
                else:
                    await update.message.reply_text("❌ Ошибка удаления администратора")
            del context.user_data['awaiting_admin_id']
        except ValueError:
            await update.message.reply_text("❌ Неверный формат ID. ID должен быть числом")
        return
    elif context.user_data.get('awaiting_category_name') and is_admin(user_id):
        if text.lower() in ['отмена', 'cancel', 'назад', 'back']:
            del context.user_data['awaiting_category_name']
            await admin_manage_categories_handler(update, context)
            return
        category_name = text.strip()
        if len(category_name) < 2:
            await update.message.reply_text("❌ Название категории слишком короткое! Минимум 2 символа.")
            return
        categories = get_categories()
        if category_name in categories:
            await update.message.reply_text(f"❌ Категория '{category_name}' уже существует!")
            return
        placeholder = create_placeholder_product(category=category_name)
        catalog.append(placeholder)
        save_catalog()
        await update.message.reply_text(
            f"✅ **Категория '{escape_markdown(category_name)}' успешно создана!**\n\n"
            f"📁 **Название:** {escape_markdown(category_name)}\n\n"
            f"Теперь вы можете добавлять подкатегории и товары в эту категорию.",
            parse_mode='Markdown',
            reply_markup=get_category_management_keyboard()
        )
        del context.user_data['awaiting_category_name']
        return
    elif context.user_data.get('awaiting_subcategory_name') and is_admin(user_id):
        if text.lower() in ['отмена', 'cancel', 'назад', 'back']:
            del context.user_data['awaiting_subcategory_name']
            await admin_manage_categories_handler(update, context)
            return
        subcategory_name = text.strip()
        category = context.user_data['awaiting_subcategory_name']
        if len(subcategory_name) < 2:
            await update.message.reply_text("❌ Название подкатегории слишком короткое! Минимум 2 символа.")
            return
        subcategories = get_subcategories(category)
        if subcategory_name in subcategories:
            await update.message.reply_text(f"❌ Подкатегория '{subcategory_name}' уже существует в категории '{category}'!")
            return
        placeholder = create_placeholder_product(category=category, subcategory=subcategory_name)
        catalog.append(placeholder)
        save_catalog()
        await update.message.reply_text(
            f"✅ **Подкатегория '{escape_markdown(subcategory_name)}' успешно создана!**\n\n"
            f"📁 **Категория:** {escape_markdown(category)}\n"
            f"📂 **Подкатегория:** {escape_markdown(subcategory_name)}\n\n"
            f"Теперь вы можете добавлять типы и товары в эту подкатегорию.",
            parse_mode='Markdown',
            reply_markup=get_category_management_keyboard()
        )
        del context.user_data['awaiting_subcategory_name']
        return
    elif context.user_data.get('awaiting_type_to_category') and is_admin(user_id):
        if text.lower() in ['отмена', 'cancel', 'назад', 'back']:
            del context.user_data['awaiting_type_to_category']
            await admin_manage_categories_handler(update, context)
            return
        type_name = text.strip()
        type_data = context.user_data['awaiting_type_to_category']
        category = type_data['category']
        if len(type_name) < 2:
            await update.message.reply_text("❌ Название типа слишком короткое! Минимум 2 символа.")
            return
        category_types = get_types(category, None)
        if type_name in category_types:
            await update.message.reply_text(f"❌ Тип '{type_name}' уже существует в категории '{category}' (без подкатегории)!")
            return
        placeholder = create_placeholder_product(category=category, type_=type_name)
        catalog.append(placeholder)
        save_catalog()
        await update.message.reply_text(
            f"✅ **Тип '{escape_markdown(type_name)}' успешно создан в категории '{escape_markdown(category)}' (без подкатегории)!**\n\n"
            f"📁 **Категория:** {escape_markdown(category)}\n"
            f"📝 **Тип:** {escape_markdown(type_name)} (без подкатегории)\n\n"
            f"Теперь вы можете добавлять товары в этот тип через массовую загрузку.",
            parse_mode='Markdown',
            reply_markup=get_category_management_keyboard()
        )
        del context.user_data['awaiting_type_to_category']
        return
    elif context.user_data.get('awaiting_type_name') and is_admin(user_id):
        if text.lower() in ['отмена', 'cancel', 'назад', 'back']:
            del context.user_data['awaiting_type_name']
            await admin_manage_categories_handler(update, context)
            return
        type_name = text.strip()
        type_data = context.user_data['awaiting_type_name']
        category = type_data['category']
        subcategory = type_data['subcategory']
        if len(type_name) < 2:
            await update.message.reply_text("❌ Название типа слишком короткое! Минимум 2 символа.")
            return
        types = get_types(category, subcategory)
        if type_name in types:
            await update.message.reply_text(f"❌ Тип '{type_name}' уже существует в подкатегории '{subcategory}'!")
            return
        placeholder = create_placeholder_product(category=category, subcategory=subcategory, type_=type_name)
        catalog.append(placeholder)
        save_catalog()
        await update.message.reply_text(
            f"✅ **Тип '{escape_markdown(type_name)}' успешно создан!**\n\n"
            f"📁 **Категория:** {escape_markdown(category)}\n"
            f"📂 **Подкатегория:** {escape_markdown(subcategory)}\n"
            f"📝 **Тип:** {escape_markdown(type_name)}\n\n"
            f"Теперь вы можете добавлять товары в этот тип через массовую загрузку.",
            parse_mode='Markdown',
            reply_markup=get_category_management_keyboard()
        )
        del context.user_data['awaiting_type_name']
        return
    elif context.user_data.get('awaiting_delete_category') and is_admin(user_id):
        category = context.user_data['awaiting_delete_category']
        if text.lower() in ['отмена', 'cancel', 'назад', 'back']:
            del context.user_data['awaiting_delete_category']
            await admin_manage_categories_handler(update, context)
            return
        confirmation_text = f"ДА, УДАЛИТЬ {category}"
        if text.upper() != confirmation_text:
            await update.message.reply_text(
                f"❌ **Текст подтверждения не совпадает!**\n\nВведите точно: `{confirmation_text}`\nИли 'отмена' для отмены",
                parse_mode='Markdown'
            )
            return
        products_to_delete = get_products_by_path(category)
        deleted_count = 0
        for product in products_to_delete:
            if product.get('has_file') and product.get('file_path'):
                try:
                    file_path = product.get('file_path')
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception as e:
                    logger.error(f"Ошибка удаления файла {product['file_path']}: {e}")
            catalog[:] = [p for p in catalog if p['id'] != product['id']]
            deleted_count += 1
        save_catalog()
        await update.message.reply_text(
            f"✅ **Категория '{category}' успешно удалена!**\n\n"
            f"📊 **Удалено:**\n"
            f"• Категория: {category}\n"
            f"• Товаров: {deleted_count}\n\n"
            f"Все данные категории были безвозвратно удалены.",
            parse_mode='Markdown',
            reply_markup=get_category_management_keyboard()
        )
        del context.user_data['awaiting_delete_category']
        return
    elif context.user_data.get('awaiting_bulk_upload_params') and is_admin(user_id):
        if text.lower() in ['отмена', 'cancel', 'назад', 'back']:
            del context.user_data['awaiting_bulk_upload_params']
            await admin_manage_categories_handler(update, context)
            return
        try:
            parts = text.split('|')
            if len(parts) < 2:
                await update.message.reply_text(
                    "❌ **Неверный формат!**\n\nНужно 2 параметра через символ |\nФормат: Цена_USDT|Описание\n\n**Пример:**\n`1.11|Аккаунт Wildberries`",
                    parse_mode='Markdown'
                )
                return
            price_str = parts[0].strip()
            description = '|'.join(parts[1:]).strip()
            try:
                price = float(price_str.replace(',', '.'))
                if price <= 0:
                    await update.message.reply_text("❌ Цена должна быть больше 0")
                    return
            except ValueError:
                await update.message.reply_text("❌ Неверный формат цены! Используйте число (например: 1.11)")
                return
            params = context.user_data['awaiting_bulk_upload_params']
            params['price'] = price
            params['description'] = description
            text_msg = f"✅ **Параметры установлены!**\n\n"
            text_msg += f"📁 **Категория:** {escape_markdown(params['category'])}\n"
            if params.get('subcategory'):
                text_msg += f"📂 Подкатегория: {escape_markdown(params['subcategory'])}\n"
            text_msg += f"📝 **Тип:** {escape_markdown(params['type'])}\n"
            text_msg += f"💰 **Цена:** {price} USDT\n"
            text_msg += f"📝 **Описание:** {escape_markdown(description)}\n\n"
            text_msg += "📤 **Теперь отправляйте .txt файлы (можно несколько сообщений):**\n"
            text_msg += "Каждый файл будет добавлен в набор для этого типа.\n\n"
            text_msg += "Для завершения нажмите '🔙 В админку'"
            await update.message.reply_text(
                text_msg,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 В админку", callback_data="back_to_admin")]
                ])
            )
        except Exception as e:
            logger.error(f"Ошибка установки параметров массовой загрузки: {e}")
            await update.message.reply_text(
                f"❌ **Ошибка:** {str(e)}",
                parse_mode='Markdown'
            )
        return
    elif context.user_data.get('awaiting_restock') and is_admin(user_id):
        try:
            new_quantity = int(text)
            if new_quantity < 0:
                await update.message.reply_text("❌ Количество не может быть отрицательным")
                return
            restock_data = context.user_data['awaiting_restock']
            product_id = restock_data['product_id']
            product_name = restock_data['product_name']
            product = next((p for p in catalog if p['id'] == product_id), None)
            if product:
                old_quantity = product.get('quantity', 0)
                product['quantity'] = new_quantity
                if new_quantity > 0 and product.get('sold'):
                    product['sold'] = False
                    if 'sold_to' in product:
                        del product['sold_to']
                    if 'sold_at' in product:
                        del product['sold_at']
                save_catalog()
                await update.message.reply_text(
                    f"✅ **Количество товара обновлено!**\n\n"
                    f"🛍️ **Товар:** {escape_markdown(product_name)}\n"
                    f"📦 **Было:** {old_quantity} шт.\n"
                    f"📊 **Стало:** {new_quantity} шт.\n"
                    f"🆔 **ID товара:** `{product_id}`",
                    parse_mode='Markdown',
                    reply_markup=get_admin_keyboard()
                )
            else:
                await update.message.reply_text("❌ Товар не найден")
            if 'awaiting_restock' in context.user_data:
                del context.user_data['awaiting_restock']
        except ValueError:
            await update.message.reply_text("❌ Введите целое число (например: 10)")
        return
    elif context.user_data.get('awaiting_custom_quantity'):
        product_id = context.user_data['awaiting_custom_quantity']
        if text.lower() in ['отмена', 'cancel', 'назад', 'back']:
            del context.user_data['awaiting_custom_quantity']
            await show_product_details(update, context, product_id)
            return
        try:
            quantity = int(text)
            if quantity <= 0:
                await update.message.reply_text("❌ Количество должно быть больше 0")
                return
            await handle_quantity_selection(update, context, product_id, quantity)
            del context.user_data['awaiting_custom_quantity']
        except ValueError:
            await update.message.reply_text("❌ Пожалуйста, введите целое число (например: 3, 5, 10)")
        return

    await update.message.reply_text(
        "🤖 **Используйте кнопки меню для навигации**",
        parse_mode='Markdown',
        reply_markup=get_main_inline_keyboard(user_id)
    )

# ========== ОБРАБОТЧИК ДОКУМЕНТОВ ==========
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    await handle_document_for_product(update, context)

# ========== ГЛАВНАЯ ФУНКЦИЯ ==========
def main():
    print("=" * 50)
    print("🤖 ЗАПУСК БОТА С CRYPTOBOT, ИЕРАРХИЧЕСКИМИ КАТЕГОРИЯМИ, ПРОМОКОДАМИ, ЗАЯВКАМИ И ПРЕДЗАКАЗАМИ")
    print("=" * 50)
    print("📁 **ИЕРАРХИЯ КАТЕГОРИЙ:** Категория → Подкатегория → Тип")
    print("📁 **ТИПЫ В КАТЕГОРИИ:** Теперь можно создавать типы прямо в категориях (без подкатегорий)")
    print("📦 **МАССОВАЯ ЗАГРУЗКА:** Загрузка файлов в конкретный тип")
    print("🛡️ **УПРАВЛЕНИЕ КАТЕГОРИЯМИ:** Добавление/удаление категорий, подкатегорий, типов")
    print("🛍️ **МОИ ПОКУПКИ:** Раздел для просмотра купленных товаров")
    print("🎟️ **ПРОМОКОДЫ:** Денежные и товарные промокоды с лимитами и защитой от повторного использования")
    print("📋 **ЗАЯВКИ НА ПОПОЛНЕНИЕ:** Пополнение через администратора с подтверждением")
    print("📦 **ПРЕДЗАКАЗЫ:** Возможность оформить предзаказ с оплатой и вводом данных")
    print("💰 **НОВОЕ:** Команда списания баланса в админ-панели")
    print("⚡ **ОПТИМИЗАЦИЯ:** Кэширование данных, атомарная запись файлов")
    print("=" * 50)

    load_data()

    if CRYPTO_BOT_TOKEN == "528185:AAxnCLhKJKxLQgsPxsK0xPkm3pQ61kdwRL3":
        print("⚠️ ВНИМАНИЕ: Используется тестовый токен CryptoBot!")

    available_products = get_available_products()
    file_products = [p for p in available_products if p.get('has_file')]
    regular_products = [p for p in available_products if not p.get('has_file')]
    bundle_products = [p for p in available_products if p.get('is_bundle', False)]

    categories = get_categories()
    total_quantity = sum(p.get('quantity', 0) for p in catalog if not p.get('sold', False))

    total_bundle_files = 0
    available_bundle_files = 0
    for product in catalog:
        if product.get('is_bundle', False):
            bundle_files = product.get('bundle_files', [])
            total_bundle_files += len(bundle_files)
            available_bundle_files += get_available_files_count(bundle_files)

    print(f"📊 **СИСТЕМНАЯ СТАТИСТИКА:**")
    print(f"• Пользователей: {len(users)}")
    print(f"• Товаров в каталоге: {len(catalog)}")
    print(f"• Доступно товаров: {len(available_products)}")
    print(f"• Файловых товаров: {len(file_products)}")
    print(f"• Наборов файлов: {len(bundle_products)}")
    print(f"• Файлов в наборах: {total_bundle_files} (доступно: {available_bundle_files})")
    print(f"• Категорий: {len(categories)}")
    print(f"• Администраторов: {len(admins)}")
    print(f"• Промокодов: {len(promocodes)}")
    print(f"• Заявок: {len(requests_dict)}")
    print(f"• Предзаказов: {len(preorders)}")
    print("=" * 50)

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("myid", myid_command))
    application.add_handler(CommandHandler("addadmin", addadmin_command))
    application.add_handler(CommandHandler("removeadmin", removeadmin_command))

    application.add_handler(CallbackQueryHandler(button_callback))

    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("✅ Бот успешно инициализирован!")
    print("⏳ Запускаю бота...")
    print("=" * 50)

    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
