import logging
import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional, List

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, BotCommand, BotCommandScopeChat
)
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
import asyncpg
from asyncpg.pool import Pool
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Токен бота и ID группы из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-5126633040"))
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    logger.error("BOT_TOKEN не найден в переменных окружения!")
    exit(1)
if not DATABASE_URL:
    logger.error("DATABASE_URL не найден в переменных окружения!")
    exit(1)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# Ссылка на правила библиотеки
RULES_URL = "https://docs.google.com/document/d/1l9nUMiQPCYPPoV_deUjroP2BZb6MRRRBVtw_D57NAxs/edit?usp=sharing"

# --- Класс для работы с БД ---
class Database:
    def __init__(self):
        self.pool: Optional[Pool] = None

    async def create_pool(self):
        try:
            self.pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            logger.info("Пул соединений с базой данных создан")
        except Exception as e:
            logger.error(f"Ошибка создания пула соединений: {e}")
            raise

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("Пул соединений закрыт")

db = Database()

# --- Состояния FSM ---
class UserStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_accept_rules = State()
    waiting_for_office = State()
    waiting_for_book_title = State()
    waiting_for_confirmation = State()
    waiting_for_duration = State()
    waiting_for_booking_confirmation = State()  # после бронирования, перед завершением
    waiting_for_photo = State()
    waiting_for_return_completion = State()      # после фото, перед завершением возврата
    waiting_for_waitlist_choice = State()

# --- Инициализация БД ---
async def init_db():
    async with db.pool.acquire() as conn:
        # Таблица пользователей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT,
                office TEXT,
                current_book TEXT,
                booking_start TIMESTAMP,
                booking_duration TEXT,
                booking_end TIMESTAMP,
                status TEXT DEFAULT 'available',
                rules_accepted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица книг
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS books (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                author TEXT NOT NULL,
                office TEXT NOT NULL,
                status TEXT DEFAULT 'available',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Добавляем колонки shelf и floor, если их нет
        try:
            await conn.execute('ALTER TABLE books ADD COLUMN IF NOT EXISTS shelf INTEGER;')
            await conn.execute('ALTER TABLE books ADD COLUMN IF NOT EXISTS floor INTEGER;')
            logger.info("Колонки shelf и floor добавлены или уже существуют")
        except Exception as e:
            logger.error(f"Ошибка при добавлении колонок: {e}")

        # Таблица бронирований
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bookings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                book_title TEXT NOT NULL,
                office TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                duration TEXT NOT NULL,
                end_time TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица листа ожидания
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS waiting_list (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                book_title TEXT NOT NULL,
                office TEXT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notified BOOLEAN DEFAULT FALSE,
                CONSTRAINT unique_waiting_entry UNIQUE (user_id, book_title, office)
            )
        ''')

        # Проверяем, есть ли книги в базе
        count = await conn.fetchval('SELECT COUNT(*) FROM books')
        if count == 0:
            books_data = [
                ("книга а", "автор А", "Stone Towers", 1, 5),
                ("книга в", "автор В", "Stone Towers", 4, 5),
                ("книга с", "автор С", "Stone Towers", 3, 6),
                ("книга d", "автор D", "Manhatten", None, None),
                ("книга е", "автор E", "Manhatten", None, None),
                ("книга x", "автор Х", "Известия", None, None),
                ("книга z", "автор Z", "Известия", None, None),
                ("книга y", "автор У", "Известия", None, None)
            ]
            for title, author, office, shelf, floor in books_data:
                await conn.execute(
                    'INSERT INTO books (title, author, office, shelf, floor) VALUES ($1, $2, $3, $4, $5)',
                    title, author, office, shelf, floor
                )
            logger.info("Добавлены начальные данные в таблицу книг с полками и этажами")
        else:
            # Обновляем существующие записи для Stone Towers
            stone_books = [
                ("книга а", 1, 5),
                ("книга в", 4, 5),
                ("книга с", 3, 6)
            ]
            for title, shelf, floor in stone_books:
                await conn.execute(
                    '''
                    UPDATE books 
                    SET shelf = $1, floor = $2 
                    WHERE LOWER(title) = LOWER($3) AND office = 'Stone Towers'
                    ''',
                    shelf, floor, title
                )
            logger.info("Обновлены полки и этажи для книг в Stone Towers")

        logger.info("База данных инициализирована")

# --- Функции для работы с БД ---
async def register_user(user_id: int, first_name: str, last_name: str):
    async with db.pool.acquire() as conn:
        await conn.execute(
            '''
            INSERT INTO users (user_id, first_name, last_name, status)
            VALUES ($1, $2, $3, 'available')
            ON CONFLICT (user_id) 
            DO UPDATE SET first_name = $2, last_name = $3
            ''',
            user_id, first_name, last_name
        )

async def accept_rules(user_id: int):
    async with db.pool.acquire() as conn:
        await conn.execute(
            'UPDATE users SET rules_accepted = TRUE WHERE user_id = $1',
            user_id
        )

async def update_user_office(user_id: int, office: str):
    async with db.pool.acquire() as conn:
        await conn.execute(
            'UPDATE users SET office = $1 WHERE user_id = $2',
            office, user_id
        )

async def get_user_info(user_id: int):
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT first_name, last_name, office, status, rules_accepted FROM users WHERE user_id = $1',
            user_id
        )
        return row

async def get_books_by_office(office: str):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT title, author, shelf, floor FROM books WHERE office = $1 AND status = $2',
            office, 'available'
        )
        return rows

async def book_exists_in_office(title: str, office: str):
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT title, author, status, shelf, floor FROM books WHERE LOWER(title) = LOWER($1) AND office = $2',
            title, office
        )
        return row

async def update_book_status(title: str, office: str, status: str):
    async with db.pool.acquire() as conn:
        await conn.execute(
            'UPDATE books SET status = $1 WHERE LOWER(title) = LOWER($2) AND office = $3',
            status, title, office
        )

async def get_user_booking(user_id: int):
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            '''
            SELECT current_book, booking_start, booking_duration, booking_end 
            FROM users WHERE user_id = $1 AND status = 'booked'
            ''',
            user_id
        )
        return row

async def create_booking(user_id: int, book_title: str, office: str, duration: str):
    async with db.pool.acquire() as conn:
        start_time = datetime.now()
        if duration == "1 час":
            end_time = start_time + timedelta(hours=1)
        elif duration == "1 день":
            end_time = start_time + timedelta(days=1)
        elif duration == "1 неделя":
            end_time = start_time + timedelta(weeks=1)
        elif duration == "1 месяц":
            end_time = start_time + timedelta(days=30)

        async with conn.transaction():
            await update_book_status(book_title, office, "booked")
            await remove_from_waiting_list(user_id, book_title, office)

            booking_id = await conn.fetchval(
                '''
                INSERT INTO bookings (user_id, book_title, office, start_time, duration, end_time)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
                ''',
                user_id, book_title, office, start_time, duration, end_time
            )

            await conn.execute(
                '''
                UPDATE users 
                SET current_book = $1, booking_start = $2, 
                    booking_duration = $3, booking_end = $4, status = 'booked'
                WHERE user_id = $5
                ''',
                book_title, start_time, duration, end_time, user_id
            )
        return booking_id, end_time

async def complete_booking(user_id: int, book_title: str, office: str):
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            await update_book_status(book_title, office, "available")
            await conn.execute(
                '''
                UPDATE users 
                SET current_book = NULL, booking_start = NULL, 
                    booking_duration = NULL, booking_end = NULL, status = 'available'
                WHERE user_id = $1
                ''',
                user_id
            )
            await conn.execute(
                '''
                UPDATE bookings 
                SET status = 'completed' 
                WHERE user_id = $1 AND book_title = $2 AND status = 'active'
                ''',
                user_id, book_title
            )
            await notify_next_in_waiting_list(book_title, office)

async def add_to_waiting_list(user_id: int, book_title: str, office: str):
    async with db.pool.acquire() as conn:
        try:
            await conn.execute(
                '''
                INSERT INTO waiting_list (user_id, book_title, office)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, book_title, office) DO NOTHING
                ''',
                user_id, book_title, office
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка при добавлении в лист ожидания: {e}")
            return False

async def get_first_in_waiting_list(book_title: str, office: str):
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            '''
            SELECT user_id FROM waiting_list 
            WHERE book_title = $1 AND office = $2 AND NOT notified
            ORDER BY added_at ASC
            LIMIT 1
            ''',
            book_title, office
        )
        return row

async def remove_from_waiting_list(user_id: int, book_title: str, office: str):
    async with db.pool.acquire() as conn:
        await conn.execute(
            '''
            DELETE FROM waiting_list 
            WHERE user_id = $1 AND book_title = $2 AND office = $3
            ''',
            user_id, book_title, office
        )

async def notify_next_in_waiting_list(book_title: str, office: str):
    async with db.pool.acquire() as conn:
        waiting_user = await get_first_in_waiting_list(book_title, office)
        if waiting_user:
            user_id = waiting_user['user_id']
            user_info = await get_user_info(user_id)
            if user_info:
                first_name = user_info['first_name']
                try:
                    await bot.send_message(
                        user_id,
                        f"🎉 {first_name}, книга '{book_title}' освободилась! "
                        f"Хотите её забронировать?",
                        reply_markup=get_waitlist_notification_keyboard(book_title, office)
                    )
                    await conn.execute(
                        '''
                        UPDATE waiting_list 
                        SET notified = TRUE 
                        WHERE user_id = $1 AND book_title = $2 AND office = $3
                        ''',
                        user_id, book_title, office
                    )
                    return True
                except Exception as e:
                    logger.error(f"Ошибка при отправке уведомления: {e}")
        return False

# --- Функции для управления командами меню ---
async def set_user_commands(user_id: int, commands: List[BotCommand]):
    try:
        await bot.set_my_commands(
            commands=commands,
            scope=BotCommandScopeChat(chat_id=user_id)
        )
        logger.info(f"Команды для пользователя {user_id} обновлены: {[c.command for c in commands]}")
    except Exception as e:
        logger.error(f"Ошибка установки команд для пользователя {user_id}: {e}")

async def set_initial_commands_after_accept(user_id: int):
    await set_user_commands(user_id, [
        BotCommand(command="rules", description="📚 Правила библиотеки")
    ])

async def add_return_command(user_id: int, book_title: str):
    await set_user_commands(user_id, [
        BotCommand(command="rules", description="📚 Правила библиотеки"),
        BotCommand(command="return", description=f"↩️ Вернуть книгу {book_title}")
    ])

async def add_book_command(user_id: int):
    await set_user_commands(user_id, [
        BotCommand(command="rules", description="📚 Правила библиотеки"),
        BotCommand(command="book", description="📖 Забронировать книгу")
    ])

async def remove_return_command(user_id: int):
    await set_initial_commands_after_accept(user_id)

async def remove_book_command(user_id: int):
    await set_initial_commands_after_accept(user_id)

async def update_commands_on_start(user_id: int, has_active_booking: bool = False, current_book: str = None):
    if has_active_booking and current_book:
        await add_return_command(user_id, current_book)
    else:
        await set_initial_commands_after_accept(user_id)

# --- Клавиатуры ---
def get_accept_rules_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Принимаю правила библиотеки", callback_data="accept_rules")
    return builder.as_markup()

def get_office_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Stone Towers", callback_data="office_stone")
    builder.button(text="Manhatten", callback_data="office_manhatten")
    builder.button(text="Известия", callback_data="office_izvestia")
    builder.adjust(1)
    return builder.as_markup()

def get_action_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Забронировать", callback_data="action_book")
    builder.button(text="Ознакомиться со списком", callback_data="action_list")
    builder.adjust(1)
    return builder.as_markup()

def get_confirmation_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Да", callback_data="confirm_yes")
    builder.button(text="Нет", callback_data="confirm_no")
    builder.adjust(2)
    return builder.as_markup()

def get_duration_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="1 час", callback_data="duration_1h")
    builder.button(text="1 день", callback_data="duration_1d")
    builder.button(text="1 неделя", callback_data="duration_1w")
    builder.button(text="1 месяц", callback_data="duration_1m")
    builder.adjust(2)
    return builder.as_markup()

def get_return_options_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Не бронирую", callback_data="return_cancel")
    builder.button(text="Забронировать другую", callback_data="return_another")
    builder.adjust(1)
    return builder.as_markup()

def get_return_book_keyboard(book_title: str):
    builder = InlineKeyboardBuilder()
    builder.button(text=f"Вернуть книгу {book_title}", callback_data=f"return_{book_title}")
    builder.adjust(1)
    return builder.as_markup()

def get_finish_booking_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Завершить бронирование", callback_data="finish_booking")
    return builder.as_markup()

def get_finish_return_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Завершить возврат", callback_data="finish_return")
    return builder.as_markup()

def get_waitlist_choice_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Добавить в лист ожидания", callback_data="waitlist_add")
    builder.button(text="Выбрать другую книгу", callback_data="waitlist_other")
    builder.adjust(1)
    return builder.as_markup()

def get_waitlist_notification_keyboard(book_title: str, office: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="Забронировать эту книгу", callback_data=f"waitlist_book_{book_title}_{office}")
    builder.button(text="Выбрать другую книгу", callback_data="action_book")
    builder.adjust(1)
    return builder.as_markup()

def get_book_again_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Забронировать ещё", callback_data="action_book")
    builder.adjust(1)
    return builder.as_markup()

# --- Вспомогательные функции ---
def format_books_list(books):
    if not books:
        return "В этом офисе сейчас нет доступных книг."
    result = "📚 Доступные книги в этом офисе:\n\n"
    for i, book in enumerate(books, 1):
        result += f"{i}. {book['title']} - {book['author']}"
        if book.get('shelf') and book.get('floor'):
            result += f" (полка {book['shelf']}, этаж {book['floor']})"
        result += "\n"
    return result

async def safe_edit_message(message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            logger.warning("Сообщение не изменено - отправляем как новое")
            await message.answer(text, reply_markup=reply_markup)
        else:
            raise

async def process_start_booking(message: Message, state: FSMContext):
    """Общая логика начала бронирования (вызывается из cmd_book и action_book)"""
    user_info = await get_user_info(message.from_user.id)
    if not user_info:
        await message.answer(
            "Похоже, мы с тобой ещё не знакомились. Напиши, пожалуйста, свои Имя и Фамилию",
            reply_markup=None
        )
        await state.set_state(UserStates.waiting_for_name)
        return

    first_name = user_info['first_name']
    office = user_info['office']

    booking_info = await get_user_booking(message.from_user.id)
    if booking_info and booking_info['current_book']:
        current_book = booking_info['current_book']
        duration = booking_info['booking_duration']
        await message.answer(
            f"{first_name}, у тебя уже есть активное бронирование книги '{current_book}' на срок {duration}. "
            f"Сначала верни эту книгу, прежде чем бронировать новую.",
            reply_markup=get_return_book_keyboard(current_book)
        )
        return

    if office:
        await message.answer(
            f"{first_name}, ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
            reply_markup=get_action_keyboard()
        )
        await state.set_state(UserStates.waiting_for_book_title)
        await state.update_data(first_name=first_name, office=office)
    else:
        await message.answer(
            f"{first_name}, выбери, пожалуйста, офис, в котором ты работаешь, "
            "чтобы я мог подсказать книги в наличии",
            reply_markup=get_office_keyboard()
        )
        await state.set_state(UserStates.waiting_for_office)
        await state.update_data(first_name=first_name)

# --- Фоновая задача для проверки напоминаний ---
async def check_reminders():
    while True:
        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT user_id, current_book, booking_start, booking_duration, booking_end, first_name, office
                    FROM users 
                    WHERE status = 'booked' AND booking_end IS NOT NULL
                ''')
                current_time = datetime.now()
                for user in rows:
                    user_id = user['user_id']
                    book_title = user['current_book']
                    booking_start = user['booking_start']
                    duration = user['booking_duration']
                    booking_end = user['booking_end']
                    first_name = user['first_name']
                    office = user['office']
                    if not booking_start or not booking_end:
                        continue
                    last_reminder_key = f"last_reminder_{user_id}_{book_title}"

                    if duration == "1 час":
                        reminder_time = booking_end - timedelta(minutes=15)
                        if current_time >= reminder_time and current_time < booking_end:
                            try:
                                await bot.send_message(
                                    user_id,
                                    f"*Не забудь вернуть книгу '{book_title}' через 15 минут*",
                                    parse_mode="Markdown",
                                    reply_markup=get_return_book_keyboard(book_title)
                                )
                            except Exception as e:
                                logger.error(f"Ошибка отправки напоминания: {e}")
                        if current_time >= booking_end:
                            last_reminder = getattr(check_reminders, last_reminder_key, None)
                            if last_reminder is None or (current_time - last_reminder) >= timedelta(hours=2):
                                try:
                                    await bot.send_message(
                                        user_id,
                                        f"Бронь книги '{book_title}' закончилась. Пожалуйста, верни книгу.",
                                        reply_markup=get_return_book_keyboard(book_title)
                                    )
                                    setattr(check_reminders, last_reminder_key, current_time)
                                except Exception as e:
                                    logger.error(f"Ошибка отправки напоминания об окончании: {e}")
                    elif duration == "1 неделя":
                        day_5 = booking_start + timedelta(days=5)
                        if current_time.date() == day_5.date() and current_time.hour == 9:
                            try:
                                await bot.send_message(
                                    user_id,
                                    f"Не забудь вернуть книгу '{book_title}' завтра",
                                    reply_markup=get_return_book_keyboard(book_title)
                                )
                            except Exception as e:
                                logger.error(f"Ошибка отправки напоминания за день: {e}")
                        day_6 = booking_start + timedelta(days=6)
                        if current_time.date() == day_6.date() and current_time.hour == 9:
                            try:
                                await bot.send_message(
                                    user_id,
                                    f"Не забудь вернуть книгу '{book_title}' сегодня",
                                    reply_markup=get_return_book_keyboard(book_title)
                                )
                            except Exception as e:
                                logger.error(f"Ошибка отправки напоминания за день: {e}")
                        if current_time >= booking_end:
                            last_reminder = getattr(check_reminders, last_reminder_key, None)
                            if last_reminder is None or (current_time - last_reminder) >= timedelta(hours=2):
                                try:
                                    await bot.send_message(
                                        user_id,
                                        f"Бронь книги '{book_title}' закончилась. Пожалуйста, верни книгу.",
                                        reply_markup=get_return_book_keyboard(book_title)
                                    )
                                    setattr(check_reminders, last_reminder_key, current_time)
                                except Exception as e:
                                    logger.error(f"Ошибка отправки напоминания об окончании: {e}")
                    elif duration == "1 месяц":
                        day_21 = booking_start + timedelta(days=21)
                        if current_time.date() == day_21.date() and current_time.hour == 9:
                            try:
                                await bot.send_message(
                                    user_id,
                                    f"Не забудь вернуть книгу '{book_title}' через неделю"
                                )
                            except Exception as e:
                                logger.error(f"Ошибка отправки напоминания за неделю: {e}")
                        day_27 = booking_start + timedelta(days=27)
                        if current_time.date() == day_27.date() and current_time.hour == 9:
                            try:
                                await bot.send_message(
                                    user_id,
                                    f"Не забудь вернуть книгу '{book_title}' сегодня",
                                    reply_markup=get_return_book_keyboard(book_title)
                                )
                            except Exception as e:
                                logger.error(f"Ошибка отправки напоминания за день: {e}")
                        if current_time >= booking_end:
                            last_reminder = getattr(check_reminders, last_reminder_key, None)
                            if last_reminder is None or (current_time - last_reminder) >= timedelta(hours=2):
                                try:
                                    await bot.send_message(
                                        user_id,
                                        f"Бронь книги '{book_title}' закончилась. Пожалуйста, верни книгу.",
                                        reply_markup=get_return_book_keyboard(book_title)
                                    )
                                    setattr(check_reminders, last_reminder_key, current_time)
                                except Exception as e:
                                    logger.error(f"Ошибка отправки напоминания об окончании: {e}")
        except Exception as e:
            logger.error(f"Ошибка при проверке напоминаний: {e}")
        await asyncio.sleep(300)

# --- Обработчики сообщений ---
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_info = await get_user_info(message.from_user.id)

    if user_info:
        first_name = user_info['first_name']
        office = user_info['office']
        rules_accepted = user_info.get('rules_accepted', False)

        booking_info = await get_user_booking(message.from_user.id)
        has_booking = booking_info is not None and booking_info.get('current_book') is not None
        current_book = booking_info['current_book'] if has_booking else None

        if rules_accepted:
            await update_commands_on_start(message.from_user.id, has_booking, current_book)
        else:
            await message.answer(
                f"{first_name}, перед началом работы прошу Вас ознакомиться с правилами библиотеки и принять их.\n\n"
                f"Правила библиотеки находятся по данной ссылке:\n{RULES_URL}",
                reply_markup=get_accept_rules_keyboard()
            )
            await state.set_state(UserStates.waiting_for_accept_rules)
            await state.update_data(first_name=first_name, office=office, user_exists=True)
            return

        if office:
            await message.answer(
                f"Привет, {first_name}! Вы зашли в библиотеку Stone. Здесь вы сможете ознакомиться со списком книг в наличии, "
                "а также забронировать ту книгу, которая вам интересна. "
                "Ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
                reply_markup=get_action_keyboard()
            )
            await state.set_state(UserStates.waiting_for_book_title)
            await state.update_data(first_name=first_name, office=office)
        else:
            await message.answer(
                f"Привет, {first_name}! Вы зашли в библиотеку Stone. Здесь вы сможете ознакомиться со списком книг в наличии, "
                "а также забронировать ту книгу, которая вам интересна. "
                f"{first_name}, выбери, пожалуйста, офис, в котором ты работаешь, "
                "чтобы я мог подсказать книги в наличии",
                reply_markup=get_office_keyboard()
            )
            await state.set_state(UserStates.waiting_for_office)
            await state.update_data(first_name=first_name)
    else:
        await message.answer(
            "Привет! Вы зашли в библиотеку Stone. Здесь вы сможете ознакомиться со списком книг в наличии, "
            "а также забронировать ту книгу, которая вам интересна. "
            "Для начала давайте познакомимся! Напишите, пожалуйста, свои Имя и Фамилию"
        )
        await state.set_state(UserStates.waiting_for_name)

@router.message(Command("rules"))
async def cmd_rules(message: Message, state: FSMContext):
    await message.answer(
        f"📚 Правила библиотеки Stone:\n{RULES_URL}",
        disable_web_page_preview=False
    )

@router.message(Command("return"))
async def cmd_return(message: Message, state: FSMContext):
    user_id = message.from_user.id
    booking_info = await get_user_booking(user_id)
    if not booking_info or not booking_info['current_book']:
        await message.answer("❌ У вас нет активных бронирований.")
        return
    book_title = booking_info['current_book']
    user_info = await get_user_info(user_id)
    if not user_info:
        await message.answer("❌ Ошибка: пользователь не найден.")
        return
    first_name = user_info['first_name']
    last_name = user_info['last_name']
    office = user_info['office']
    await state.set_state(UserStates.waiting_for_photo)
    await state.update_data(
        book_title=book_title,
        office=office,
        first_name=first_name,
        last_name=last_name
    )
    await message.answer("📸 Отправьте, пожалуйста, фото книги в библиотеке.")

@router.message(Command("book"))
async def cmd_book(message: Message, state: FSMContext):
    user_id = message.from_user.id
    await remove_book_command(user_id)
    await process_start_booking(message, state)

@router.message(StateFilter(UserStates.waiting_for_name))
async def process_name(message: Message, state: FSMContext):
    name_parts = message.text.split()
    if len(name_parts) < 2:
        await message.answer("Пожалуйста, введите ваше Имя и Фамилию через пробел.")
        return
    first_name = name_parts[0]
    last_name = " ".join(name_parts[1:])
    await register_user(message.from_user.id, first_name, last_name)
    await state.update_data(first_name=first_name, last_name=last_name)
    await message.answer(
        f"{first_name}, перед началом работы прошу Вас ознакомиться с правилами библиотеки и принять их.\n\n"
        f"Правила библиотеки находятся по данной ссылке:\n{RULES_URL}",
        reply_markup=get_accept_rules_keyboard()
    )
    await state.set_state(UserStates.waiting_for_accept_rules)

@router.callback_query(StateFilter(UserStates.waiting_for_accept_rules), F.data == "accept_rules")
async def process_accept_rules(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await accept_rules(user_id)
    await set_initial_commands_after_accept(user_id)
    data = await state.get_data()
    first_name = data.get('first_name')
    office = data.get('office')
    user_exists = data.get('user_exists', False)

    if user_exists:
        if office:
            await callback.message.edit_text(
                f"{first_name}, ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
                reply_markup=get_action_keyboard()
            )
            await state.set_state(UserStates.waiting_for_book_title)
        else:
            await callback.message.edit_text(
                f"{first_name}, выбери, пожалуйста, офис, в котором ты работаешь, "
                "чтобы я мог подсказать книги в наличии",
                reply_markup=get_office_keyboard()
            )
            await state.set_state(UserStates.waiting_for_office)
    else:
        await callback.message.edit_text(
            f"{first_name}, выбери, пожалуйста, офис, в котором ты работаешь, "
            "чтобы я мог подсказать книги в наличии",
            reply_markup=get_office_keyboard()
        )
        await state.set_state(UserStates.waiting_for_office)

@router.message(StateFilter(UserStates.waiting_for_office,
                           UserStates.waiting_for_confirmation,
                           UserStates.waiting_for_duration,
                           UserStates.waiting_for_waitlist_choice,
                           UserStates.waiting_for_booking_confirmation,
                           UserStates.waiting_for_return_completion))
async def ignore_text_in_button_states(message: Message):
    await message.answer(
        "Пожалуйста, используй кнопки для выбора. "
        "Текстовые сообщения в этом состоянии не обрабатываются."
    )

@router.callback_query(StateFilter(UserStates.waiting_for_office), F.data.startswith("office_"))
async def process_office(callback: CallbackQuery, state: FSMContext):
    office_map = {
        "office_stone": "Stone Towers",
        "office_manhatten": "Manhatten",
        "office_izvestia": "Известия"
    }
    office = office_map.get(callback.data)
    if not office:
        await callback.answer("Неверный выбор офиса")
        return
    await update_user_office(callback.from_user.id, office)
    await state.update_data(office=office)
    await callback.message.edit_text(
        "Ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
        reply_markup=get_action_keyboard()
    )
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(StateFilter(UserStates.waiting_for_book_title), F.data == "action_book")
async def process_action_book(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Напиши, пожалуйста, название книги")
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(StateFilter(UserStates.waiting_for_book_title), F.data == "action_list")
async def process_action_list(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    office = data.get('office')
    if not office:
        await callback.answer("Ошибка: офис не выбран")
        return
    books = await get_books_by_office(office)
    books_list = format_books_list(books)
    await callback.message.edit_text(
        f"{books_list}\n\n"
        "Как только выберешь нужную книгу, просто напиши мне её название. "
        "Если не нашёл для себя подходящей книги, напиши Нет"
    )
    await state.set_state(UserStates.waiting_for_book_title)

@router.message(StateFilter(UserStates.waiting_for_book_title))
async def process_book_title(message: Message, state: FSMContext):
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
        builder = InlineKeyboardBuilder()
        builder.button(text="Забронировать", callback_data="action_book")
        await message.answer(
            "Если захочешь забронировать книгу, просто нажми кнопку забронировать",
            reply_markup=builder.as_markup()
        )
        return

    book_info = await book_exists_in_office(book_title, office)
    if not book_info:
        await message.answer(
            "Такой книги нет в нашей библиотеке. "
            "Хочешь забронировать другую книгу или не будешь ничего бронировать?",
            reply_markup=get_return_options_keyboard()
        )
        await state.set_state(UserStates.waiting_for_confirmation)
        return

    title = book_info['title']
    author = book_info['author']
    status = book_info['status']
    shelf = book_info['shelf']
    floor = book_info['floor']

    if status == 'booked':
        await message.answer(
            f"Книга '{title}' от автора {author} сейчас находится у другого пользователя. "
            "Хотите ли добавить книгу в лист ожидания?",
            reply_markup=get_waitlist_choice_keyboard()
        )
        await state.update_data(book_title=title, author=author)
        await state.set_state(UserStates.waiting_for_waitlist_choice)
        return

    message_text = f"{first_name}, "
    if office == "Stone Towers" and shelf and floor:
        message_text += f"книга '{title}' находится на этаже {floor} на полке {shelf}. "
    message_text += f"Хочешь забронировать книгу '{title}' от автора {author}?"
    await state.update_data(book_title=title, author=author)
    await message.answer(
        message_text,
        reply_markup=get_confirmation_keyboard()
    )
    await state.set_state(UserStates.waiting_for_confirmation)

@router.callback_query(StateFilter(UserStates.waiting_for_waitlist_choice), F.data == "waitlist_add")
async def process_waitlist_add(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    book_title = data.get('book_title')
    office = data.get('office')
    first_name = data.get('first_name')
    if not book_title or not office:
        await callback.answer("Ошибка: данные не найдены")
        return
    success = await add_to_waiting_list(callback.from_user.id, book_title, office)
    if success:
        await callback.message.edit_text(
            f"Вы добавлены в лист ожидания для книги '{book_title}'. "
            f"Я уведомлю вас, когда книга освободится."
        )
        builder = InlineKeyboardBuilder()
        builder.button(text="Выбрать другую книгу", callback_data="action_book")
        await callback.message.answer(
            "Вы можете выбрать другую книгу, пока ждёте освобождения этой:",
            reply_markup=builder.as_markup()
        )
    else:
        await callback.message.edit_text(
            "Произошла ошибка при добавлении в лист ожидания. Пожалуйста, попробуйте позже."
        )
    await state.clear()

@router.callback_query(StateFilter(UserStates.waiting_for_waitlist_choice), F.data == "waitlist_other")
async def process_waitlist_other(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    first_name = data.get('first_name')
    office = data.get('office')
    await callback.message.edit_text(
        f"{first_name}, ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
        reply_markup=get_action_keyboard()
    )
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(F.data.startswith("waitlist_book_"))
async def process_waitlist_book(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 4:
        await callback.answer("Ошибка в данных")
        return
    book_title = "_".join(parts[2:-1])
    office = parts[-1]
    user_info = await get_user_info(callback.from_user.id)
    if not user_info:
        await callback.answer("Ошибка: пользователь не найден")
        return
    first_name = user_info['first_name']
    book_info = await book_exists_in_office(book_title, office)
    if not book_info or book_info['status'] != 'available':
        await callback.answer("Книга больше не доступна")
        return
    shelf = book_info['shelf']
    floor = book_info['floor']
    await state.update_data(book_title=book_title, author=book_info['author'], office=office, first_name=first_name)
    message_text = f"{first_name}, "
    if office == "Stone Towers" and shelf and floor:
        message_text += f"книга '{book_title}' находится на этаже {floor} на полке {shelf}. "
    message_text += f"Хочешь забронировать книгу '{book_title}' от автора {book_info['author']}?"
    await callback.message.edit_text(
        message_text,
        reply_markup=get_confirmation_keyboard()
    )
    await state.set_state(UserStates.waiting_for_confirmation)

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "confirm_yes")
async def process_confirmation_yes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    first_name = data.get('first_name')
    await callback.message.edit_text(
        f"{first_name}, выбери, пожалуйста, промежуток времени, "
        "на который ты хочешь забронировать книгу",
        reply_markup=get_duration_keyboard()
    )
    await state.set_state(UserStates.waiting_for_duration)

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "confirm_no")
async def process_confirmation_no(callback: CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="Не бронирую", callback_data="return_cancel")
    builder.button(text="Забронировать другую", callback_data="return_another")
    builder.adjust(1)
    await callback.message.edit_text(
        "Ты не будешь бронировать книгу или ты решил забронировать другую?",
        reply_markup=builder.as_markup()
    )
    await state.set_state(UserStates.waiting_for_confirmation)

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "return_cancel")
async def process_return_cancel(callback: CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="Забронировать", callback_data="action_book")
    await callback.message.edit_text(
        "Если захочешь забронировать книгу, просто нажми кнопку забронировать",
        reply_markup=builder.as_markup()
    )
    await state.clear()

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "return_another")
async def process_return_another(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
        reply_markup=get_action_keyboard()
    )
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(StateFilter(UserStates.waiting_for_duration), F.data.startswith("duration_"))
async def process_duration(callback: CallbackQuery, state: FSMContext):
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
        booking_id, end_time = await create_booking(
            callback.from_user.id, book_title, office, duration
        )
        user_info = await get_user_info(callback.from_user.id)
        if user_info:
            last_name = user_info['last_name']
            user_id = callback.from_user.id
            await bot.send_message(
                GROUP_CHAT_ID,
                f"Пользователь {first_name} {last_name} (ID: {user_id}) забронировал книгу '{book_title}' на срок {duration}"
            )

        await safe_edit_message(
            callback.message,
            f"{first_name}, ты бронируешь книгу '{book_title}' на {duration}.",
            reply_markup=get_finish_booking_keyboard()
        )
        await state.update_data(
            book_title=book_title,
            duration=duration,
            office=office,
            first_name=first_name
        )
        await state.set_state(UserStates.waiting_for_booking_confirmation)
    except Exception as e:
        logger.error(f"Ошибка при создании бронирования: {e}")
        builder = InlineKeyboardBuilder()
        builder.button(text="Попробовать снова", callback_data=callback.data)
        await safe_edit_message(
            callback.message,
            "Произошла временная ошибка. Пожалуйста, попробуйте позже.",
            reply_markup=builder.as_markup()
        )
        await state.clear()

@router.callback_query(StateFilter(UserStates.waiting_for_booking_confirmation), F.data == "finish_booking")
async def process_finish_booking(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    first_name = data.get('first_name')
    book_title = data.get('book_title')
    duration = data.get('duration')

    await safe_edit_message(
        callback.message,
        f"{first_name}, ваше бронирование книги '{book_title}' на {duration} активно. "
        "Я напомню, когда вы должны вернуть книгу!"
    )
    await add_return_command(callback.from_user.id, book_title)
    await state.clear()

@router.callback_query(F.data.startswith("return_"))
async def process_return_book(callback: CallbackQuery, state: FSMContext):
    book_title = callback.data.replace("return_", "")
    user_info = await get_user_info(callback.from_user.id)
    if not user_info:
        await callback.answer("Ошибка: пользователь не найден")
        return
    first_name = user_info['first_name']
    last_name = user_info['last_name']
    office = user_info['office']
    booking_info = await get_user_booking(callback.from_user.id)
    if not booking_info or booking_info['current_book'] != book_title:
        await callback.answer("У вас нет активного бронирования этой книги")
        return
    await callback.message.edit_text("📸 Отправь, пожалуйста, фото книги в библиотеке.")
    await state.set_state(UserStates.waiting_for_photo)
    await state.update_data(
        book_title=book_title,
        office=office,
        first_name=first_name,
        last_name=last_name
    )

@router.message(StateFilter(UserStates.waiting_for_photo), F.photo)
async def process_return_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    book_title = data.get('book_title')
    office = data.get('office')
    first_name = data.get('first_name')
    last_name = data.get('last_name')

    try:
        await complete_booking(message.from_user.id, book_title, office)
        photo = message.photo[-1]
        user_id = message.from_user.id
        await bot.send_photo(
            GROUP_CHAT_ID,
            photo.file_id,
            caption=f"Пользователь {first_name} {last_name} (ID: {user_id}) вернул книгу '{book_title}'"
        )
        await message.answer(
            "Спасибо, что вернул книгу. Надеюсь, она была интересной и понравилась тебе.",
            reply_markup=get_finish_return_keyboard()
        )
        await state.update_data(
            book_title=book_title,
            office=office,
            first_name=first_name,
            last_name=last_name
        )
        await state.set_state(UserStates.waiting_for_return_completion)
    except Exception as e:
        logger.error(f"Ошибка при завершении бронирования: {e}")
        builder = InlineKeyboardBuilder()
        builder.button(text="Попробовать снова", callback_data=f"return_{book_title}")
        await message.answer(
            "Произошла ошибка при обработке возврата. Пожалуйста, попробуйте ещё раз.",
            reply_markup=builder.as_markup()
        )

@router.message(StateFilter(UserStates.waiting_for_photo))
async def ignore_text_during_photo(message: Message):
    await message.answer("Пожалуйста, отправьте фото книги, а не текстовое сообщение.")

@router.callback_query(StateFilter(UserStates.waiting_for_return_completion), F.data == "finish_return")
async def process_finish_return(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    first_name = data.get('first_name')
    await remove_return_command(callback.from_user.id)
    await add_book_command(callback.from_user.id)
    await callback.message.edit_text(
        f"{first_name}, вы завершили возврат книги.\n\n"
        "Если вы захотите забронировать ещё одну книгу, вы можете нажать кнопку «Забронировать» в меню.\n"
        "Также в меню Вы сможете повторно ознакомиться с правилами библиотеки."
    )
    await state.clear()

@router.callback_query(F.data == "action_book")
async def process_action_book_any_state(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await remove_book_command(user_id)
    await process_start_booking(callback.message, state)
    await callback.answer()

async def wait_for_db():
    for i in range(10):
        try:
            await db.create_pool()
            return True
        except Exception as e:
            logger.warning(f"Не удалось подключиться к базе данных (попытка {i+1}/10): {e}")
            await asyncio.sleep(5)
    return False

async def main():
    try:
        logger.info("Запуск библиотечного бота...")
        if not await wait_for_db():
            logger.error("Не удалось подключиться к базе данных")
            return
        await init_db()
        asyncio.create_task(check_reminders())
        logger.info("Бот запущен и готов к работе!")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        await db.close()
        await bot.session.close()
        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
