import logging
import asyncio
import sqlite3
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, PhotoSize
from aiogram.filters import CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Токен бота и ID группы (безопасное получение)
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_CHAT_ID_STR = os.getenv("GROUP_CHAT_ID")

if not BOT_TOKEN:
    logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: BOT_TOKEN не установлен в Railway Variables!")
    logger.error("👉 Решение: Зайди в Railway → Variables и добавь BOT_TOKEN")
    exit(1)

if not GROUP_CHAT_ID_STR:
    logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: GROUP_CHAT_ID не установлен в Railway Variables!")
    logger.error("👉 Решение: Зайди в Railway → Variables и добавь GROUP_CHAT_ID")
    exit(1)

try:
    GROUP_CHAT_ID = int(GROUP_CHAT_ID_STR)
except ValueError:
    logger.error(f"❌ ОШИБКА: GROUP_CHAT_ID должен быть числом! Сейчас: '{GROUP_CHAT_ID_STR}'")
    exit(1)

logger.info(f"✅ Переменные загружены! GROUP_CHAT_ID: {GROUP_CHAT_ID}")

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# СОСТОЯНИЯ FSM
class UserStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_office = State()
    waiting_for_book_title = State()
    waiting_for_confirmation = State()
    waiting_for_duration = State()
    waiting_for_photo = State()

# ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ
def get_db_connection():
    """Простое подключение к базе данных"""
    return sqlite3.connect('library.db', check_same_thread=False)

# ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ
def init_db():
    """Создание таблиц и первоначальное заполнение книгами"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Таблица пользователей
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        office TEXT,
        current_book TEXT,
        booking_start TEXT,
        booking_duration TEXT,
        booking_end TEXT,
        status TEXT DEFAULT 'available',
        telegram_id INTEGER UNIQUE NOT NULL
    )
    ''')
    
    # Таблица книг
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        author TEXT,
        office TEXT,
        status TEXT DEFAULT 'available'
    )
    ''')
    
    # Таблица бронирований
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS bookings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        book_title TEXT,
        office TEXT,
        start_time TEXT,
        duration TEXT,
        end_time TEXT,
        status TEXT DEFAULT 'active',
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')
    
    # Добавление книг только если их нет (ИСПРАВЛЕНО)
    cursor.execute('SELECT COUNT(*) FROM books')
    if cursor.fetchone()[0] == 0:
        books_data = [
            ("книга а", "автор А", "Stone Towers"),
            ("книга в", "автор В", "Stone Towers"),
            ("книга с", "автор С", "Stone Towers"),
            ("книга d", "автор D", "Manhatten"),
            ("книга е", "автор E", "Manhatten"),
            ("книга x", "автор Х", "Известия"),
            ("книга z", "автор Z", "Известия"),
            ("книга y", "автор У", "Известия")
        ]
        
        cursor.executemany('''
        INSERT INTO books (title, author, office) VALUES (?, ?, ?)
        ''', books_data)
    
    conn.commit()
    conn.close()
    logger.info("✅ База данных успешно инициализирована")

# ПОЛУЧЕНИЕ КНИГ ПО ОФИСУ
def get_books_by_office(office):
    """Получение списка доступных книг в указанном офисе"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT title, author FROM books WHERE office = ? AND status = "available"', (office,))
    books = cursor.fetchall()
    conn.close()
    return books

# ПРОВЕРКА СУЩЕСТВОВАНИЯ КНИГИ В ОФИСЕ
def book_exists_in_office(title, office):
    """Проверка, существует ли книга в указанном офисе"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT title, author FROM books WHERE LOWER(title) = ? AND office = ? AND status = "available"', 
                  (title.lower(), office))
    result = cursor.fetchone()
    conn.close()
    return result

# ОБНОВЛЕНИЕ СТАТУСА КНИГИ
def update_book_status(title, office, status):
    """Обновление статуса книги в базе данных"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE books SET status = ? WHERE LOWER(title) = ? AND office = ?', 
                  (status, title.lower(), office))
    conn.commit()
    conn.close()

# СОЗДАНИЕ БРОНИРОВАНИЯ
def create_booking(user_id, book_title, office, duration):
    """Создание нового бронирования книги"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Получаем текущее время
    start_time = datetime.now()
    
    # Рассчитываем время окончания
    if duration == "1 час":
        end_time = start_time + timedelta(hours=1)
    elif duration == "1 день":
        end_time = start_time + timedelta(days=1)
    elif duration == "1 неделя":
        end_time = start_time + timedelta(weeks=1)
    elif duration == "1 месяц":
        end_time = start_time + timedelta(days=30)
    
    # Создаем запись о бронировании
    cursor.execute('''
    INSERT INTO bookings (user_id, book_title, office, start_time, duration, end_time)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (user_id, book_title, office, start_time.isoformat(), duration, end_time.isoformat()))
    
    # Обновляем статус книги
    update_book_status(book_title, office, "booked")
    
    # Обновляем информацию о пользователе
    cursor.execute('''
    UPDATE users SET current_book = ?, booking_start = ?, booking_duration = ?, booking_end = ?, status = 'booked'
    WHERE user_id = ?
    ''', (book_title, start_time.isoformat(), duration, end_time.isoformat(), user_id))
    
    booking_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return booking_id, end_time

# ПОЛУЧЕНИЕ ИНФОРМАЦИИ О БРОНИРОВАНИИ ПОЛЬЗОВАТЕЛЯ
def get_user_booking(user_id):
    """Получение информации о текущем бронировании пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    SELECT current_book, booking_start, booking_duration, booking_end 
    FROM users WHERE user_id = ? AND status = 'booked'
    ''', (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result

# ЗАВЕРШЕНИЕ БРОНИРОВАНИЯ
def complete_booking(user_id, book_title, office):
    """Завершение бронирования и возврат книги"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Обновляем статус книги
    update_book_status(book_title, office, "available")
    
    # Обновляем статус пользователя
    cursor.execute('''
    UPDATE users SET current_book = NULL, booking_start = NULL, 
    booking_duration = NULL, booking_end = NULL, status = 'available'
    WHERE user_id = ?
    ''', (user_id,))
    
    # Обновляем статус бронирования
    cursor.execute('''
    UPDATE bookings SET status = 'completed' 
    WHERE user_id = ? AND book_title = ? AND status = 'active'
    ''', (user_id, book_title))
    
    conn.commit()
    conn.close()

# РЕГИСТРАЦИЯ ПОЛЬЗОВАТЕЛЯ С TELEGRAM ID
def register_user(user_id, first_name, last_name, telegram_id):
    """Регистрация или обновление пользователя с сохранением Telegram ID"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    INSERT OR REPLACE INTO users 
    (user_id, first_name, last_name, status, telegram_id) 
    VALUES (?, ?, ?, 'available', ?)
    ''', (user_id, first_name, last_name, telegram_id))
    conn.commit()
    conn.close()

# ОБНОВЛЕНИЕ ОФИСА ПОЛЬЗОВАТЕЛЯ
def update_user_office(telegram_id, office):
    """Обновление офиса пользователя по его Telegram ID"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    UPDATE users SET office = ? 
    WHERE telegram_id = ?
    ''', (office, telegram_id))
    conn.commit()
    conn.close()

# ПОЛУЧЕНИЕ ИНФОРМАЦИИ О ПОЛЬЗОВАТЕЛЕ
def get_user_info(telegram_id):
    """Получение информации о пользователе по Telegram ID"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    SELECT first_name, last_name, office 
    FROM users 
    WHERE telegram_id = ?
    ''', (telegram_id,))
    result = cursor.fetchone()
    conn.close()
    return result

# ФОРМАТИРОВАНИЕ СПИСКА КНИГ
def format_books_list(books):
    """Форматирование списка книг для сообщения"""
    if not books:
        return "В этом офисе сейчас нет доступных книг."
    
    result = "📚 Доступные книги в этом офисе:\n\n"
    for i, (title, author) in enumerate(books, 1):
        result += f"{i}. {title} - {author}\n"
    return result

# КЛАВИАТУРЫ
def get_start_keyboard():
    """Клавиатура для начального сообщения"""
    builder = InlineKeyboardBuilder()
    builder.button(text="Начать", callback_data="start")
    return builder.as_markup()

def get_office_keyboard():
    """Клавиатура для выбора офиса"""
    builder = InlineKeyboardBuilder()
    builder.button(text="Stone Towers", callback_data="office_stone")
    builder.button(text="Manhatten", callback_data="office_manhatten")
    builder.button(text="Известия", callback_data="office_izvestia")
    builder.adjust(1)
    return builder.as_markup()

def get_action_keyboard():
    """Клавиатура для выбора действия"""
    builder = InlineKeyboardBuilder()
    builder.button(text="Забронировать", callback_data="action_book")
    builder.button(text="Ознакомиться со списком", callback_data="action_list")
    builder.adjust(1)
    return builder.as_markup()

def get_confirmation_keyboard():
    """Клавиатура для подтверждения бронирования"""
    builder = InlineKeyboardBuilder()
    builder.button(text="Да", callback_data="confirm_yes")
    builder.button(text="Нет", callback_data="confirm_no")
    builder.adjust(2)
    return builder.as_markup()

def get_duration_keyboard():
    """Клавиатура для выбора срока бронирования"""
    builder = InlineKeyboardBuilder()
    builder.button(text="1 час", callback_data="duration_1h")
    builder.button(text="1 день", callback_data="duration_1d")
    builder.button(text="1 неделя", callback_data="duration_1w")
    builder.button(text="1 месяц", callback_data="duration_1m")
    builder.adjust(2)
    return builder.as_markup()

def get_return_options_keyboard():
    """Клавиатура для опций после отмены бронирования"""
    builder = InlineKeyboardBuilder()
    builder.button(text="Не бронирую", callback_data="return_cancel")
    builder.button(text="Забронировать другую", callback_data="return_another")
    builder.adjust(1)
    return builder.as_markup()

def get_booking_keyboard(book_title):
    """Клавиатура для возврата книги"""
    builder = InlineKeyboardBuilder()
    builder.button(text=f"Книга {book_title} возвращена", callback_data=f"return_{book_title}")
    builder.adjust(1)
    return builder.as_markup()

# БЕЗОПАСНОЕ РЕДАКТИРОВАНИЕ СООБЩЕНИЙ
async def safe_edit_message(message, text, reply_markup=None):
    """Безопасное редактирование сообщения с обработкой ошибок"""
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            logger.warning("Message not modified - sending as new message")
            await message.answer(text, reply_markup=reply_markup)
        else:
            raise

# ОБРАБОТЧИКИ
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start"""
    await state.clear()
    await message.answer(
        "Привет! Я бот библиотеки Stone. Нажми кнопку 'Начать', чтобы начать работу с библиотекой.",
        reply_markup=get_start_keyboard()
    )

@router.callback_query(F.data == "start")
async def process_start(callback: CallbackQuery, state: FSMContext):
    """Обработчик начала работы"""
    # СНАЧАЛА ОТПРАВЛЯЕМ СООБЩЕНИЕ!
    await callback.message.edit_text(
        "Привет! Вы зашли в библиотеку Stone. Здесь вы сможете ознакомиться со списком книг в наличии, а также забронировать ту книгу, которая вам интересна. Для начала давайте познакомимся! Напишите, пожалуйста свои Имя и Фамилию"
    )
    # ПОТОМ УСТАНАВЛИВАЕМ СОСТОЯНИЕ!
    await state.set_state(UserStates.waiting_for_name)

@router.message(StateFilter(UserStates.waiting_for_name))
async def process_name(message: Message, state: FSMContext):
    """Обработчик ввода имени и фамилии"""
    name_parts = message.text.split()
    if len(name_parts) < 2:
        await message.answer("Пожалуйста, введите ваше Имя и Фамилию через пробел.")
        return
    
    first_name = name_parts[0]
    last_name = " ".join(name_parts[1:])
    
    # Регистрируем пользователя с Telegram ID
    register_user(message.from_user.id, first_name, last_name, message.from_user.id)
    
    await state.update_data(first_name=first_name, last_name=last_name)
    await message.answer(
        f"{first_name}, выбери, пожалуйста, офис, в котором ты работаешь, чтобы я мог подсказать книги в наличии",
        reply_markup=get_office_keyboard()
    )
    await state.set_state(UserStates.waiting_for_office)

# ОБРАБОТЧИКИ ДЛЯ ТЕКСТОВЫХ СООБЩЕНИЙ В СОСТОЯНИЯХ ОЖИДАНИЯ КНОПОК
@router.message(StateFilter(UserStates.waiting_for_office, 
                           UserStates.waiting_for_confirmation,
                           UserStates.waiting_for_duration))
async def ignore_text_in_button_states(message: Message):
    """Обработка текстовых сообщений в состояниях ожидания кнопок"""
    await message.answer("Пожалуйста, используй кнопки для выбора. Текстовые сообщения в этом состоянии не обрабатываются.")

@router.callback_query(StateFilter(UserStates.waiting_for_office), F.data.startswith("office_"))
async def process_office(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора офиса"""
    office_map = {
        "office_stone": "Stone Towers",
        "office_manhatten": "Manhatten", 
        "office_izvestia": "Известия"
    }
    
    office = office_map.get(callback.data)
    if not office:
        await callback.answer("Неверный выбор офиса")
        return
    
    # Обновляем офис пользователя
    update_user_office(callback.from_user.id, office)
    await state.update_data(office=office)
    
    await callback.message.edit_text(
        "Ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
        reply_markup=get_action_keyboard()
    )
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(StateFilter(UserStates.waiting_for_book_title), F.data == "action_book")
async def process_action_book(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Забронировать'"""
    await callback.message.edit_text("Напиши, пожалуйста, название книги")
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(StateFilter(UserStates.waiting_for_book_title), F.data == "action_list")
async def process_action_list(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Ознакомиться со списком'"""
    data = await state.get_data()
    office = data.get('office')
    
    if not office:
        await callback.answer("Ошибка: офис не выбран")
        return
    
    books = get_books_by_office(office)
    books_list = format_books_list(books)
    
    await callback.message.edit_text(
        f"{books_list}\n\nКак только выберешь нужную книгу, просто напиши мне её название. Если не нашёл для себя подходящей книги, напиши Нет"
    )
    await state.set_state(UserStates.waiting_for_book_title)

@router.message(StateFilter(UserStates.waiting_for_book_title))
async def process_book_title(message: Message, state: FSMContext):
    """Обработчик ввода названия книги"""
    data = await state.get_data()
    office = data.get('office')
    first_name = data.get('first_name')
    
    if not office or not first_name:
        await message.answer("Ошибка: данные о пользователе не найдены. Начните сначала.")
        await state.clear()
        return
    
    book_title = message.text.strip()
    
    if book_title.lower() == "нет":
        await message.answer("Жаль что тут нет подходящей книги, заходи в другой раз!")
        await message.answer(
            "Если захочешь забронировать книгу, просто нажми кнопку забронировать",
            reply_markup=InlineKeyboardBuilder().button(text="Забронировать", callback_data="action_book").as_markup()
        )
        return
    
    # Проверяем наличие книги
    book_info = book_exists_in_office(book_title, office)
    
    if not book_info:
        await message.answer(
            "Такой книги нет в нашей библиотеке. Хочешь забронировать другую книгу или не будешь ничего бронировать?",
            reply_markup=get_return_options_keyboard()
        )
        await state.set_state(UserStates.waiting_for_confirmation)
        return
    
    title, author = book_info
    await state.update_data(book_title=title, author=author)
    
    await message.answer(
        f"{first_name}, ты хочешь забронировать книгу '{title}' от автора {author} ?",
        reply_markup=get_confirmation_keyboard()
    )
    await state.set_state(UserStates.waiting_for_confirmation)

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "confirm_yes")
async def process_confirmation_yes(callback: CallbackQuery, state: FSMContext):
    """Обработчик подтверждения бронирования"""
    data = await state.get_data()
    first_name = data.get('first_name')
    
    await callback.message.edit_text(
        f"{first_name}, выбери, пожалуйста, промежуток времени, на который ты хочешь забронировать книгу",
        reply_markup=get_duration_keyboard()
    )
    await state.set_state(UserStates.waiting_for_duration)

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "confirm_no")
async def process_confirmation_no(callback: CallbackQuery, state: FSMContext):
    """Обработчик отмены бронирования"""
    await callback.message.edit_text(
        "Ты не будешь бронировать книгу или ты решил забронировать другую?",
        reply_markup=InlineKeyboardBuilder()
        .button(text="Не бронирую", callback_data="return_cancel")
        .button(text="Забронировать другую", callback_data="return_another")
        .adjust(1)
        .as_markup()
    )
    await state.set_state(UserStates.waiting_for_confirmation)

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "return_cancel")
async def process_return_cancel(callback: CallbackQuery, state: FSMContext):
    """Обработчик отмены бронирования и возврата в главное меню"""
    await callback.message.edit_text(
        "Если захочешь забронировать книгу, просто нажми кнопку забронировать",
        reply_markup=InlineKeyboardBuilder().button(text="Забронировать", callback_data="action_book").as_markup()
    )
    await state.clear()

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "return_another")
async def process_return_another(callback: CallbackQuery, state: FSMContext):
    """Обработчик перехода к бронированию другой книги"""
    data = await state.get_data()
    office = data.get('office')
    
    await callback.message.edit_text(
        "Ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
        reply_markup=get_action_keyboard()
    )
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(StateFilter(UserStates.waiting_for_duration), F.data.startswith("duration_"))
async def process_duration(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора срока бронирования"""
    duration_map = {
        "duration_1h": "1 час",
        "duration_1d": "1 день", 
        "duration_1w": "1 неделя",
        "duration_1m": "1 месяц"
    }
    
    duration = duration_map.get(callback.data)
    if not duration:
        await callback.answer("Неверный выбор длительности")
        return
    
    data = await state.get_data()
    book_title = data.get('book_title')
    author = data.get('author')
    office = data.get('office')
    first_name = data.get('first_name')
    
    try:
        booking_id, end_time = create_booking(
            callback.from_user.id, book_title, office, duration
        )
        
        # Отправляем уведомление в группу с Telegram ID
        user_info = get_user_info(callback.from_user.id)
        if user_info:
            _, last_name, _ = user_info
            await bot.send_message(
                GROUP_CHAT_ID,
                f"Пользователь {first_name} {last_name} (ID: {callback.from_user.id}) "
                f"забронировал книгу '{book_title}' на срок {duration}"
            )
        
        await safe_edit_message(
            callback.message,
            f"{first_name}, ты забронировал книгу '{book_title}' на {duration}. Я напомню тебе, когда ты должен вернуть книгу!",
            reply_markup=InlineKeyboardBuilder().button(text="Забронировать", callback_data="action_book").as_markup()
        )
    except Exception as e:
        logger.error(f"❌ Ошибка при бронировании: {e}")
        await safe_edit_message(
            callback.message,
            "Произошла временная ошибка с базой данных. Пожалуйста, попробуйте позже.",
            reply_markup=InlineKeyboardBuilder().button(text="Попробовать снова", callback_data=callback.data).as_markup()
        )
    
    await state.clear()

# ФОНОВАЯ ЗАДАЧА ДЛЯ НАПОМИНАНИЙ
async def check_reminders():
    """Фоновая задача для отправки напоминаний о возврате книг"""
    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT user_id, current_book, booking_start, booking_duration, booking_end, first_name, office
            FROM users 
            WHERE status = 'booked' AND booking_end IS NOT NULL
            ''')
            
            users = cursor.fetchall()
            current_time = datetime.now()
            
            for user in users:
                user_id, book_title, booking_start_str, duration, booking_end_str, first_name, office = user
                
                if not booking_start_str or not booking_end_str:
                    continue
                
                booking_start = datetime.fromisoformat(booking_start_str)
                booking_end = datetime.fromisoformat(booking_end_str)
                
                if duration == "1 час":
                    reminder_time = booking_end - timedelta(minutes=15)
                    if current_time >= reminder_time and current_time < booking_end:
                        try:
                            await bot.send_message(
                                user_id,
                                f"*Не забудь вернуть книгу '{book_title}' через 15 минут*",
                                parse_mode="Markdown",
                                reply_markup=get_booking_keyboard(book_title)
                            )
                        except Exception as e:
                            logger.error(f"❌ Ошибка отправки напоминания: {e}")
                    
                    if current_time >= booking_end:
                        try:
                            await bot.send_message(
                                user_id,
                                f"Бронь книги '{book_title}' закончилась. Пожалуйста, верни книгу.",
                                reply_markup=get_booking_keyboard(book_title)
                            )
                        except Exception as e:
                            logger.error(f"❌ Ошибка отправки напоминания об окончании: {e}")
            
            conn.close()
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке напоминаний: {e}")
        
        await asyncio.sleep(300)

# ОБРАБОТЧИКИ ВОЗВРАТА КНИГИ
@router.callback_query(F.data.startswith("return_"))
async def process_return_book(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки возврата книги"""
    book_title = callback.data.replace("return_", "")
    
    user_info = get_user_info(callback.from_user.id)
    if not user_info:
        await callback.answer("Ошибка: пользователь не найден")
        return
    
    first_name, last_name, office = user_info
    
    booking_info = get_user_booking(callback.from_user.id)
    if not booking_info or booking_info[0] != book_title:
        await callback.answer("У вас нет активного бронирования этой книги")
        return
    
    await callback.message.edit_text("Отправь пожалуйста фото книги в библиотеке")
    await state.set_state(UserStates.waiting_for_photo)
    await state.update_data(book_title=book_title, office=office, first_name=first_name, last_name=last_name)

@router.message(StateFilter(UserStates.waiting_for_photo), F.photo)
async def process_return_photo(message: Message, state: FSMContext):
    """Обработчик фото при возврате книги"""
    data = await state.get_data()
    book_title = data.get('book_title')
    office = data.get('office')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    
    try:
        complete_booking(message.from_user.id, book_title, office)
        
        # Отправляем уведомление в группу с фото и Telegram ID
        photo = message.photo[-1]
        await bot.send_photo(
            GROUP_CHAT_ID,
            photo.file_id,
            caption=f"Пользователь {first_name} {last_name} (ID: {message.from_user.id}) "
                    f"вернул книгу '{book_title}'"
        )
        
        await message.answer(
            "Спасибо, что вернул книгу. Надеюсь она была интересной и понравилась тебе.",
            reply_markup=InlineKeyboardBuilder().button(text="Забронировать", callback_data="action_book").as_markup()
        )
        
        await state.clear()
    except Exception as e:
        logger.error(f"❌ Ошибка при завершении бронирования: {e}")
        await message.answer(
            "Произошла ошибка при обработке возврата. Пожалуйста, попробуйте ещё раз.",
            reply_markup=InlineKeyboardBuilder().button(text="Попробовать снова", callback_data=f"return_{book_title}").as_markup()
        )

# ИГНОРИРОВАНИЕ ТЕКСТОВЫХ СООБЩЕНИЙ В СОСТОЯНИИ ОЖИДАНИЯ ФОТО
@router.message(StateFilter(UserStates.waiting_for_photo))
async def ignore_text_during_photo(message: Message):
    """Игнорирование текстовых сообщений при ожидании фото"""
    await message.answer("Пожалуйста, отправьте фото книги, а не текстовое сообщение.")

# ОБРАБОТЧИК КНОПКИ "ЗАБРОНИРОВАТЬ" ПОСЛЕ УСПЕШНОГО БРОНИРОВАНИЯ
@router.callback_query(F.data == "action_book")
async def process_action_book_any_state(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Забронировать' в любом состоянии"""
    user_info = get_user_info(callback.from_user.id)
    
    if not user_info:
        await callback.message.edit_text(
            "Похоже, мы с тобой ещё не знакомились. Напиши, пожалуйста, свои Имя и Фамилию"
        )
        await state.set_state(UserStates.waiting_for_name)
        return
    
    first_name, last_name, office = user_info
    
    booking_info = get_user_booking(callback.from_user.id)
    if booking_info and booking_info[0]:
        current_book, booking_start_str, duration, booking_end_str = booking_info
        await callback.message.edit_text(
            f"{first_name}, у тебя уже есть активное бронирование книги '{current_book}' на срок {duration}. "
            f"Сначала верни эту книгу, прежде чем бронировать новую.",
            reply_markup=get_booking_keyboard(current_book)
        )
        return
    
    if office:
        await callback.message.edit_text(
            "Ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
            reply_markup=get_action_keyboard()
        )
        await state.set_state(UserStates.waiting_for_book_title)
    else:
        await callback.message.edit_text(
            f"{first_name}, выбери, пожалуйста, офис, в котором ты работаешь, чтобы я мог подсказать книги в наличии",
            reply_markup=get_office_keyboard()
        )
        await state.set_state(UserStates.waiting_for_office)

# ФУНКЦИЯ ЗАПУСКА БОТА
async def main():
    """Основная функция запуска бота"""
    try:
        # Инициализация базы данных
        init_db()
        
        # Запускаем фоновую задачу для напоминаний
        asyncio.create_task(check_reminders())
        
        # Запускаем бота
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске бота: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
