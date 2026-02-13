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

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-5126633040"))
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    logger.error("BOT_TOKEN не найден в переменных окружения!")
    exit(1)
if not DATABASE_URL:
    logger.error("DATABASE_URL не найден в переменных окружения!")
    exit(1)

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

RULES_URL = "https://docs.google.com/document/d/1l9nUMiQPCYPPoV_deUjroP2BZb6MRRRBVtw_D57NAxs/edit?usp=sharing"

# ------------------------------ Глобальные переменные для админ-режимов ------------------------------
group_awaiting_action = None
group_awaiting_author = None

# ------------------------------ База данных ------------------------------
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
            logger.info("Пул соединений с БД создан")
        except Exception as e:
            logger.error(f"Ошибка создания пула: {e}")
            raise

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("Пул соединений закрыт")

db = Database()

# ------------------------------ Состояния FSM ------------------------------
class UserStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_accept_rules = State()
    waiting_for_office = State()
    waiting_for_book_title = State()
    waiting_for_confirmation = State()
    waiting_for_duration = State()
    waiting_for_booking_confirmation = State()
    waiting_for_photo = State()
    waiting_for_return_completion = State()
    waiting_for_waitlist_choice = State()
    waiting_for_book_request = State()

# ------------------------------ Инициализация БД ------------------------------
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
        try:
            await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS rules_accepted BOOLEAN DEFAULT FALSE;')
            logger.info("Колонка rules_accepted добавлена/существует")
        except Exception as e:
            logger.error(f"Ошибка добавления rules_accepted: {e}")

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
        try:
            await conn.execute('ALTER TABLE books ADD COLUMN IF NOT EXISTS shelf INTEGER;')
            await conn.execute('ALTER TABLE books ADD COLUMN IF NOT EXISTS floor INTEGER;')
            logger.info("Колонки shelf/floor добавлены/существуют")
        except Exception as e:
            logger.error(f"Ошибка добавления shelf/floor: {e}")

        # Таблица бронирований
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bookings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                book_id INTEGER REFERENCES books(id),
                book_title TEXT NOT NULL,
                office TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                duration TEXT NOT NULL,
                end_time TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'active',
                extension_made BOOLEAN DEFAULT FALSE,
                overdue_notified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        try:
            await conn.execute('ALTER TABLE bookings ADD COLUMN IF NOT EXISTS book_id INTEGER REFERENCES books(id);')
            await conn.execute('ALTER TABLE bookings ADD COLUMN IF NOT EXISTS extension_made BOOLEAN DEFAULT FALSE;')
            await conn.execute('ALTER TABLE bookings ADD COLUMN IF NOT EXISTS overdue_notified BOOLEAN DEFAULT FALSE;')
            logger.info("Колонки book_id, extension_made, overdue_notified добавлены/существуют")
        except Exception as e:
            logger.error(f"Ошибка добавления колонок в bookings: {e}")

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

        # Проверка наличия начальных книг
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
            logger.info("Начальные книги добавлены")
        else:
            # Обновление полок/этажей для Stone Towers
            stone_books = [("книга а", 1, 5), ("книга в", 4, 5), ("книга с", 3, 6)]
            for title, shelf, floor in stone_books:
                await conn.execute(
                    'UPDATE books SET shelf = $1, floor = $2 WHERE LOWER(title) = LOWER($3) AND office = $4',
                    shelf, floor, title, 'Stone Towers'
                )
            logger.info("Обновлены полки/этажи для Stone Towers")
        logger.info("Инициализация БД завершена")

# ------------------------------ Функции работы с БД ------------------------------
async def register_user(user_id: int, first_name: str, last_name: str):
    async with db.pool.acquire() as conn:
        await conn.execute(
            '''
            INSERT INTO users (user_id, first_name, last_name, status)
            VALUES ($1, $2, $3, 'available')
            ON CONFLICT (user_id) DO UPDATE SET first_name = $2, last_name = $3
            ''',
            user_id, first_name, last_name
        )

async def accept_rules(user_id: int):
    async with db.pool.acquire() as conn:
        await conn.execute('UPDATE users SET rules_accepted = TRUE WHERE user_id = $1', user_id)

async def update_user_office(user_id: int, office: str):
    async with db.pool.acquire() as conn:
        await conn.execute('UPDATE users SET office = $1 WHERE user_id = $2', office, user_id)

async def get_user_info(user_id: int):
    async with db.pool.acquire() as conn:
        return await conn.fetchrow(
            'SELECT first_name, last_name, office, status, rules_accepted FROM users WHERE user_id = $1',
            user_id
        )

async def get_books_by_office(office: str):
    """Возвращает ВСЕ доступные экземпляры книг в офисе"""
    async with db.pool.acquire() as conn:
        return await conn.fetch(
            'SELECT id, title, author, shelf, floor FROM books WHERE office = $1 AND status = $2 ORDER BY title, id',
            office, 'available'
        )

async def get_book_by_title_any_status(title: str, office: str):
    """Проверяет, существует ли книга в офисе (любой статус). Возвращает первую запись."""
    async with db.pool.acquire() as conn:
        return await conn.fetchrow(
            '''
            SELECT id, title, author, shelf, floor, status
            FROM books 
            WHERE LOWER(title) = LOWER($1) AND office = $2
            LIMIT 1
            ''',
            title, office
        )

async def get_available_book_instance(title: str, office: str):
    """Возвращает ОДИН доступный экземпляр книги в офисе (status = 'available')."""
    async with db.pool.acquire() as conn:
        return await conn.fetchrow(
            '''
            SELECT id, title, author, shelf, floor
            FROM books 
            WHERE LOWER(title) = LOWER($1) AND office = $2 AND status = 'available'
            LIMIT 1
            ''',
            title, office
        )

async def update_book_status(book_id: int, status: str):
    """Обновляет статус конкретного экземпляра книги по его ID"""
    async with db.pool.acquire() as conn:
        await conn.execute(
            'UPDATE books SET status = $1 WHERE id = $2',
            status, book_id
        )

async def get_user_booking(user_id: int):
    """Возвращает активное бронирование пользователя с ID брони и ID книги"""
    async with db.pool.acquire() as conn:
        return await conn.fetchrow(
            '''
            SELECT b.id as booking_id, b.book_id, b.book_title, 
                   b.start_time as booking_start, 
                   b.duration as booking_duration, 
                   b.end_time as booking_end
            FROM users u
            JOIN bookings b ON u.user_id = b.user_id AND b.status = 'active'
            WHERE u.user_id = $1 AND u.status = 'booked'
            LIMIT 1
            ''',
            user_id
        )

async def create_booking(user_id: int, book_id: int, book_title: str, office: str, duration: str):
    """Создаёт бронь для конкретного экземпляра книги"""
    async with db.pool.acquire() as conn:
        start_time = datetime.now()
        if duration == "1 час":
            end_time = start_time + timedelta(hours=1)
        elif duration == "1 неделя":
            end_time = start_time + timedelta(weeks=1)
        elif duration == "1 месяц":
            end_time = start_time + timedelta(days=30)
        elif duration == "3 месяца":
            end_time = start_time + timedelta(days=90)
        elif duration == "6 месяцев":
            end_time = start_time + timedelta(days=180)
        else:
            raise ValueError(f"Неизвестная длительность: {duration}")

        async with conn.transaction():
            await update_book_status(book_id, "booked")
            await remove_from_waiting_list(user_id, book_title, office)

            booking_id = await conn.fetchval(
                '''
                INSERT INTO bookings 
                    (user_id, book_id, book_title, office, start_time, duration, end_time, extension_made, overdue_notified)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING id
                ''',
                user_id, book_id, book_title, office, start_time, duration, end_time, False, False
            )

            await conn.execute(
                '''
                UPDATE users 
                SET current_book = $1, booking_start = $2, booking_duration = $3, booking_end = $4, status = 'booked'
                WHERE user_id = $5
                ''',
                book_title, start_time, duration, end_time, user_id
            )
        return booking_id, end_time

async def complete_booking(user_id: int, booking_id: int, book_id: int, book_title: str, office: str):
    """Завершаем бронь и освобождаем конкретный экземпляр"""
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            await update_book_status(book_id, "available")
            await conn.execute(
                '''
                UPDATE users 
                SET current_book = NULL, booking_start = NULL, booking_duration = NULL, booking_end = NULL, status = 'available'
                WHERE user_id = $1
                ''',
                user_id
            )
            
            await conn.execute(
                'UPDATE bookings SET status = $1 WHERE id = $2',
                'completed', booking_id
            )
            
            await notify_next_in_waiting_list(book_title, office)

async def extend_booking(booking_id: int, user_id: int, book_title: str, office: str):
    async with db.pool.acquire() as conn:
        booking = await conn.fetchrow(
            'SELECT duration, end_time, extension_made FROM bookings WHERE id = $1 AND user_id = $2 AND status = $3',
            booking_id, user_id, 'active'
        )
        if not booking:
            raise ValueError("Бронирование не найдено или уже завершено")
        if booking['extension_made']:
            raise ValueError("Бронь уже была продлена ранее")

        original_duration = booking['duration']
        current_end = booking['end_time']

        if original_duration == "1 час":
            extension = timedelta(minutes=15)
            extension_text = "15 минут"
        elif original_duration == "1 неделя":
            extension = timedelta(weeks=1)
            extension_text = "1 неделю"
        elif original_duration == "1 месяц":
            extension = timedelta(days=14)
            extension_text = "2 недели"
        elif original_duration == "3 месяца":
            extension = timedelta(days=30)
            extension_text = "1 месяц"
        elif original_duration == "6 месяцев":
            extension = timedelta(days=60)
            extension_text = "2 месяца"
        else:
            raise ValueError("Неизвестная длительность")

        new_end = current_end + extension

        async with conn.transaction():
            await conn.execute(
                'UPDATE bookings SET end_time = $1, extension_made = TRUE WHERE id = $2',
                new_end, booking_id
            )
            await conn.execute(
                'UPDATE users SET booking_end = $1 WHERE user_id = $2 AND current_book = $3 AND status = $4',
                new_end, user_id, book_title, 'booked'
            )
        return new_end, extension_text

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
            logger.error(f"Ошибка добавления в лист ожидания: {e}")
            return False

async def get_first_in_waiting_list(book_title: str, office: str):
    async with db.pool.acquire() as conn:
        return await conn.fetchrow(
            '''
            SELECT user_id FROM waiting_list 
            WHERE book_title = $1 AND office = $2 AND NOT notified
            ORDER BY added_at ASC LIMIT 1
            ''',
            book_title, office
        )

async def remove_from_waiting_list(user_id: int, book_title: str, office: str):
    async with db.pool.acquire() as conn:
        await conn.execute(
            'DELETE FROM waiting_list WHERE user_id = $1 AND book_title = $2 AND office = $3',
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
                        f"🎉 {first_name}, книга '{book_title}' освободилась! Хотите её забронировать?",
                        reply_markup=get_waitlist_notification_keyboard(book_title, office)
                    )
                    await conn.execute(
                        'UPDATE waiting_list SET notified = TRUE WHERE user_id = $1 AND book_title = $2 AND office = $3',
                        user_id, book_title, office
                    )
                    return True
                except Exception as e:
                    logger.error(f"Ошибка уведомления из листа ожидания: {e}")
        return False

# ------------------------------ Управление командами меню ------------------------------
async def set_user_commands(user_id: int, commands: List[BotCommand]):
    try:
        await bot.set_my_commands(
            commands=commands,
            scope=BotCommandScopeChat(chat_id=user_id)
        )
        logger.info(f"Команды для {user_id}: {[c.command for c in commands]}")
    except Exception as e:
        logger.error(f"Ошибка установки команд для {user_id}: {e}")

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

async def add_book_and_request_commands(user_id: int):
    await set_user_commands(user_id, [
        BotCommand(command="rules", description="📚 Правила библиотеки"),
        BotCommand(command="book", description="📖 Забронировать книгу"),
        BotCommand(command="request", description="📋 Запросить книгу")
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

# ------------------------------ Клавиатуры ------------------------------
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
    builder.button(text="1 неделя", callback_data="duration_1w")
    builder.button(text="1 месяц", callback_data="duration_1m")
    builder.button(text="3 месяца", callback_data="duration_3m")
    builder.button(text="6 месяцев", callback_data="duration_6m")
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

# ------------------------------ Вспомогательные функции ------------------------------
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
            logger.warning("Сообщение не изменено – отправляем новое")
            await message.answer(text, reply_markup=reply_markup)
        else:
            raise

async def process_start_booking(message: Message, state: FSMContext):
    """Общая логика начала бронирования"""
    user_info = await get_user_info(message.from_user.id)
    if not user_info:
        await message.answer(
            "Похоже, мы с вами ещё не знакомились. Напишите, пожалуйста, ваши Имя и Фамилию через пробел",
            reply_markup=None
        )
        await state.set_state(UserStates.waiting_for_name)
        return

    first_name = user_info['first_name']
    office = user_info['office']
    booking_info = await get_user_booking(message.from_user.id)
    if booking_info and booking_info.get('booking_id'):
        current_book = booking_info['book_title']
        duration = booking_info['booking_duration']
        await message.answer(
            f"{first_name}, у вас уже есть активное бронирование книги '{current_book}' на срок {duration}. "
            f"Сначала верни эту книгу, прежде чем бронировать новую.",
            reply_markup=get_return_book_keyboard(current_book)
        )
        return

    if office:
        await message.answer(
            f"{first_name}, вы уже определились с выбором книги для бронирования или хотите сначала ознакомиться со списком доступных книг?",
            reply_markup=get_action_keyboard()
        )
        await state.set_state(UserStates.waiting_for_book_title)
        await state.update_data(first_name=first_name, office=office)
    else:
        await message.answer(
            f"{first_name}, выберите, пожалуйста, офис, в котором работаете, "
            "чтобы я мог подсказать книги в наличии",
            reply_markup=get_office_keyboard()
        )
        await state.set_state(UserStates.waiting_for_office)
        await state.update_data(first_name=first_name)

# ------------------------------ Фоновая задача напоминаний ------------------------------
async def check_reminders():
    while True:
        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT u.user_id, u.first_name, u.last_name, u.office,
                           b.id as booking_id, b.book_id, b.book_title, b.start_time as booking_start,
                           b.duration as booking_duration, b.end_time as booking_end,
                           b.extension_made, b.overdue_notified
                    FROM users u
                    JOIN bookings b ON u.user_id = b.user_id AND b.status = 'active'
                    WHERE u.status = 'booked' AND u.booking_end IS NOT NULL
                ''')
                now = datetime.now()

                for rec in rows:
                    uid = rec['user_id']
                    bid = rec['booking_id']
                    book = rec['book_title']
                    start = rec['booking_start']
                    dur = rec['booking_duration']
                    end = rec['booking_end']
                    fname = rec['first_name']
                    ext_made = rec['extension_made']
                    overdue_not = rec['overdue_notified']

                    if not start or not end:
                        continue

                    last_key = f"last_reminder_{uid}_{bid}"

                    # ----- Напоминания до окончания -----
                    if dur == "1 час":
                        remind_15 = end - timedelta(minutes=15)
                        if now >= remind_15 and now < end:
                            await bot.send_message(
                                uid,
                                f"*Не забудьте вернуть книгу '{book}' через 15 минут*",
                                parse_mode="Markdown",
                                reply_markup=get_return_book_keyboard(book)
                            )
                    elif dur == "1 неделя":
                        day5 = start + timedelta(days=5)
                        if now.date() == day5.date() and now.hour == 9:
                            await bot.send_message(
                                uid,
                                f"Не забудьте вернуть книгу '{book}' завтра",
                                reply_markup=get_return_book_keyboard(book)
                            )
                        day6 = start + timedelta(days=6)
                        if now.date() == day6.date() and now.hour == 9:
                            await bot.send_message(
                                uid,
                                f"Не забудьте вернуть книгу '{book}' сегодня",
                                reply_markup=get_return_book_keyboard(book)
                            )
                    elif dur == "1 месяц":
                        day21 = start + timedelta(days=21)
                        if now.date() == day21.date() and now.hour == 9:
                            await bot.send_message(uid, f"Не забудьте вернуть книгу '{book}' через неделю")
                        day27 = start + timedelta(days=27)
                        if now.date() == day27.date() and now.hour == 9:
                            await bot.send_message(
                                uid,
                                f"Не забудьте вернуть книгу '{book}' сегодня",
                                reply_markup=get_return_book_keyboard(book)
                            )
                    elif dur == "3 месяца":
                        week_before = end - timedelta(days=7)
                        if now.date() == week_before.date() and now.hour == 9:
                            await bot.send_message(uid, f"Не забудьте вернуть книгу '{book}' через неделю")
                        day_before = end - timedelta(days=1)
                        if now.date() == day_before.date() and now.hour == 9:
                            await bot.send_message(
                                uid,
                                f"Не забудьте вернуть книгу '{book}' завтра",
                                reply_markup=get_return_book_keyboard(book)
                            )
                    elif dur == "6 месяцев":
                        month_before = end - timedelta(days=30)
                        if now.date() == month_before.date() and now.hour == 9:
                            await bot.send_message(uid, f"Не забудьте вернуть книгу '{book}' через месяц")
                        week_before = end - timedelta(days=7)
                        if now.date() == week_before.date() and now.hour == 9:
                            await bot.send_message(uid, f"Не забудьте вернуть книгу '{book}' через неделю")
                        day_before = end - timedelta(days=1)
                        if now.date() == day_before.date() and now.hour == 9:
                            await bot.send_message(
                                uid,
                                f"Не забудьте вернуть книгу '{book}' завтра",
                                reply_markup=get_return_book_keyboard(book)
                            )

                    # ----- Бронь закончилась -----
                    if now >= end:
                        last = getattr(check_reminders, last_key, None)
                        if last is None or (now - last) >= timedelta(hours=2):
                            builder = InlineKeyboardBuilder()
                            builder.button(text=f"Вернуть книгу {book}", callback_data=f"return_{book}")
                            if not ext_made:
                                builder.button(text="⏳ Продлить бронь", callback_data=f"extend_{bid}")
                            builder.adjust(1)
                            await bot.send_message(
                                uid,
                                f"Бронь книги '{book}' закончилась. Пожалуйста, верните книгу.",
                                reply_markup=builder.as_markup()
                            )
                            setattr(check_reminders, last_key, now)

                    # ----- Просрочка более суток -----
                    if now >= end + timedelta(days=1) and not overdue_not:
                        await bot.send_message(
                            GROUP_CHAT_ID,
                            f"🆘️ Просрочка: пользователь {fname} {rec['last_name']} (ID: {uid}) "
                            f"не вернул книгу '{book}' спустя сутки от окончания бронирования"
                        )
                        await conn.execute(
                            'UPDATE bookings SET overdue_notified = TRUE WHERE id = $1',
                            bid
                        )

        except Exception as e:
            logger.error(f"Ошибка в check_reminders: {e}")

        await asyncio.sleep(300)

# ------------------------------ Обработчики команд и сообщений ------------------------------
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_info = await get_user_info(message.from_user.id)
    if user_info:
        first_name = user_info['first_name']
        office = user_info['office']
        rules_accepted = user_info.get('rules_accepted', False)
        booking_info = await get_user_booking(message.from_user.id)
        has_booking = booking_info is not None and booking_info.get('booking_id') is not None
        current_book = booking_info['book_title'] if has_booking else None

        if rules_accepted:
            await update_commands_on_start(message.from_user.id, has_booking, current_book)
        else:
            await message.answer(
                f"{first_name}, перед началом работы прошу вас ознакомиться с правилами библиотеки и принять их.\n\n"
                f"Правила библиотеки находятся по данной ссылке:\n{RULES_URL}",
                reply_markup=get_accept_rules_keyboard()
            )
            await state.set_state(UserStates.waiting_for_accept_rules)
            await state.update_data(first_name=first_name, office=office, user_exists=True)
            return

        if office:
            await message.answer(
                f"Добрый день, {first_name}! Вы зашли в библиотеку Stone. Здесь вы сможете ознакомиться со списком книг в наличии, "
                "а также забронировать ту книгу, которая вам интересна. "
                "Вы уже определились с выбором книги для бронирования или хотите сначала ознакомиться со списком доступных книг?",
                reply_markup=get_action_keyboard()
            )
            await state.set_state(UserStates.waiting_for_book_title)
            await state.update_data(first_name=first_name, office=office)
        else:
            await message.answer(
                f"Добрый день, {first_name}! Вы зашли в библиотеку Stone. Здесь вы сможете ознакомиться со списком книг в наличии, "
                "а также забронировать ту книгу, которая вам интересна. "
                f"{first_name}, выберите, пожалуйста, офис, в котором работаете, "
                "чтобы я мог подсказать книги в наличии",
                reply_markup=get_office_keyboard()
            )
            await state.set_state(UserStates.waiting_for_office)
            await state.update_data(first_name=first_name)
    else:
        await message.answer(
            "Добрый день! Вы зашли в библиотеку Stone. Здесь вы сможете ознакомиться со списком книг в наличии, "
            "а также забронировать ту книгу, которая вам интересна. "
            "Для начала давайте познакомимся! Напишите, пожалуйста, свои Имя и Фамилию через пробел"
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
    uid = message.from_user.id
    booking_info = await get_user_booking(uid)
    if not booking_info or not booking_info.get('booking_id'):
        await message.answer("❌ У вас нет активных бронирований.")
        return
    book_title = booking_info['book_title']
    booking_id = booking_info['booking_id']
    book_id = booking_info['book_id']
    user_info = await get_user_info(uid)
    if not user_info:
        await message.answer("❌ Ошибка: пользователь не найден.")
        return
    await state.set_state(UserStates.waiting_for_photo)
    await state.update_data(
        book_title=book_title,
        office=user_info['office'],
        first_name=user_info['first_name'],
        last_name=user_info['last_name'],
        booking_id=booking_id,
        book_id=book_id
    )
    await message.answer("📸 Отправьте, пожалуйста, фотографию книги в библиотеке")

@router.message(Command("book"))
async def cmd_book(message: Message, state: FSMContext):
    await remove_book_command(message.from_user.id)
    await process_start_booking(message, state)

@router.message(Command("request"))
async def cmd_request(message: Message, state: FSMContext):
    uid = message.from_user.id
    booking_info = await get_user_booking(uid)
    if booking_info and booking_info.get('booking_id'):
        await message.answer("❌ У вас уже есть активное бронирование. Сначала верните книгу.")
        return

    await remove_book_command(uid)
    user_info = await get_user_info(uid)
    if not user_info:
        await message.answer("❌ Ошибка: пользователь не найден. Напишите /start")
        return

    await state.update_data(
        first_name=user_info['first_name'],
        last_name=user_info['last_name']
    )
    await state.set_state(UserStates.waiting_for_book_request)
    await message.answer(
        "📚 Хотите направить запрос на заказ книги для библиотеки?\n\n"
        "Мы рады пополнению! Новые книги должны соответствовать одному из критериев:\n"
        "• О бизнесе и управлении\n"
        "• О процессах и культуре нашей компании\n"
        "• О социально-психологических процессах\n\n"
        "Не добавляем: учебники, техническую документацию, современную массовую литературу без художественной ценности.\n\n"
        "Напишите, пожалуйста, название интересующей вас книги и автора данной книги."
    )

@router.message(StateFilter(UserStates.waiting_for_name))
async def process_name(message: Message, state: FSMContext):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Пожалуйста, введите ваши Имя и Фамилию через пробел.")
        return
    first_name = parts[0]
    last_name = " ".join(parts[1:])
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
    uid = callback.from_user.id
    await accept_rules(uid)
    await set_initial_commands_after_accept(uid)
    data = await state.get_data()
    first_name = data.get('first_name')
    office = data.get('office')
    user_exists = data.get('user_exists', False)

    if user_exists:
        if office:
            await callback.message.edit_text(
                f"{first_name}, вы уже определились с выбором книги для бронирования или хотите сначала ознакомиться со списком доступных книг?",
                reply_markup=get_action_keyboard()
            )
            await state.set_state(UserStates.waiting_for_book_title)
        else:
            await callback.message.edit_text(
                f"{first_name}, выберите, пожалуйста, офис, в котором работаете, "
                "чтобы я мог подсказать книги в наличии",
                reply_markup=get_office_keyboard()
            )
            await state.set_state(UserStates.waiting_for_office)
    else:
        await callback.message.edit_text(
            f"{first_name}, выберите, пожалуйста, офис, в котором работаете, "
            "чтобы я мог подсказать книги в наличии",
            reply_markup=get_office_keyboard()
        )
        await state.set_state(UserStates.waiting_for_office)

@router.message(StateFilter(
    UserStates.waiting_for_office,
    UserStates.waiting_for_confirmation,
    UserStates.waiting_for_duration,
    UserStates.waiting_for_waitlist_choice,
    UserStates.waiting_for_booking_confirmation,
    UserStates.waiting_for_return_completion))
async def ignore_text_in_button_states(message: Message):
    await message.answer(
        "Пожалуйста, используйте кнопки для выбора. "
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
        "Вы уже определились с выбором книги для бронирования или хотите сначала ознакомиться со списком доступных книг?",
        reply_markup=get_action_keyboard()
    )
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(StateFilter(UserStates.waiting_for_book_title), F.data == "action_book")
async def process_action_book(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Напишите, пожалуйста, название книги")
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(StateFilter(UserStates.waiting_for_book_title), F.data == "action_list")
async def process_action_list(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    office = data.get('office')
    if not office:
        await callback.answer("Ошибка: офис не выбран")
        return
    books = await get_books_by_office(office)
    await callback.message.edit_text(
        f"{format_books_list(books)}\n\n"
        "Как только выберете нужную книгу, напишите ее название. "
        "Если не нашли подходящий вариант, напишите Нет"
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

    title_input = message.text.strip()
    if title_input.lower() == "нет":
        await add_book_and_request_commands(message.from_user.id)
        await message.answer(
            f"{first_name}, к сожалению, подходящей книги сейчас нет. \n\n"
            "Вы можете забронировать любую другую книгу или направить запрос в HR для заказа интересующей Вас книги.\n"
            "Для этого нажмите соответствующие кнопки в меню."
        )
        await state.clear()
        return

    #Проверяем, есть ли такая книга в офисе
    book_any = await get_book_by_title_any_status(title_input, office)
    if not book_any:
        await message.answer(
            "Такой книги нет в нашей библиотеке. "
            "Хотите забронировать другую книгу или не будете ничего бронировать?",
            reply_markup=get_return_options_keyboard()
        )
        await state.set_state(UserStates.waiting_for_confirmation)
        return

    #Проверяем, есть ли доступный экземпляр
    available_book = await get_available_book_instance(title_input, office)
    if not available_book:
        #Книга есть, но все экземпляры заняты → лист ожидания
        await message.answer(
            f"Книга '{book_any['title']}' от автора {book_any['author']} сейчас находится у другого пользователя. "
            "Хотите добавить книгу в лист ожидания?",
            reply_markup=get_waitlist_choice_keyboard()
        )
        await state.update_data(
            book_title=book_any['title'],
            author=book_any['author'],
            office=office
        )
        await state.set_state(UserStates.waiting_for_waitlist_choice)
        return

    #Книга доступна
    title = available_book['title']
    author = available_book['author']
    book_id = available_book['id']
    shelf = available_book.get('shelf')
    floor = available_book.get('floor')

    msg = f"{first_name}, "
    if office == "Stone Towers" and shelf and floor:
        msg += f"книга '{title}' находится на этаже {floor} на полке {shelf}. "
    msg += f"Хотите забронировать книгу '{title}' от автора {author}?"

    await state.update_data(
        book_title=title,
        author=author,
        book_id=book_id,
        shelf=shelf,
        floor=floor
    )
    await message.answer(msg, reply_markup=get_confirmation_keyboard())
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
    if await add_to_waiting_list(callback.from_user.id, book_title, office):
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
        f"{first_name}, Вы уже определились с выбором книги для бронирования или хотите сначала ознакомиться со списком доступных книг?",
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

    # Проверяем, есть ли доступный экземпляр
    available_book = await get_available_book_instance(book_title, office)
    if not available_book:
        await callback.answer("❌ Книга больше не доступна", show_alert=True)
        return

    book_id = available_book['id']
    shelf = available_book.get('shelf')
    floor = available_book.get('floor')
    author = available_book['author']

    await state.update_data(
        book_title=book_title,
        author=author,
        office=office,
        first_name=first_name,
        book_id=book_id
    )

    msg = f"{first_name}, "
    if office == "Stone Towers" and shelf and floor:
        msg += f"книга '{book_title}' находится на этаже {floor} на полке {shelf}. "
    msg += f"Хотите забронировать книгу '{book_title}' от автора {author}?"

    await callback.message.edit_text(msg, reply_markup=get_confirmation_keyboard())
    await state.set_state(UserStates.waiting_for_confirmation)

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "confirm_yes")
async def process_confirmation_yes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    first_name = data.get('first_name')
    await callback.message.edit_text(
        f"{first_name}, выберите, пожалуйста, промежуток времени, "
        "на который вы хотите забронировать книгу",
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
        "Вы не будете бронировать книгу или хотите выбрать другую?",
        reply_markup=builder.as_markup()
    )
    await state.set_state(UserStates.waiting_for_confirmation)

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "return_cancel")
async def process_return_cancel(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    first_name = data.get('first_name', '')
    await add_book_command(callback.from_user.id)
    await callback.message.edit_text(
        f"{first_name}, вы отказались от бронирования книги.\n\n"
        "Если вы захотите забронировать книгу, вы можете нажать кнопку «Забронировать» в меню.\n"
        "Также в меню вы сможете повторно ознакомиться с правилами библиотеки."
    )
    await state.clear()

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "return_another")
async def process_return_another(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Вы уже определились с выбором книги для бронирования или хотите сначала ознакомиться со списком доступных книг?",
        reply_markup=get_action_keyboard()
    )
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(StateFilter(UserStates.waiting_for_duration), F.data.startswith("duration_"))
async def process_duration(callback: CallbackQuery, state: FSMContext):
    duration_map = {
        "duration_1h": "1 час",
        "duration_1w": "1 неделя",
        "duration_1m": "1 месяц",
        "duration_3m": "3 месяца",
        "duration_6m": "6 месяцев"
    }
    dur = duration_map.get(callback.data)
    if not dur:
        await callback.answer("Неверный выбор длительности")
        return

    data = await state.get_data()
    book_title = data.get('book_title')
    book_id = data.get('book_id')
    office = data.get('office')
    first_name = data.get('first_name')

    try:
        bid, end_time = await create_booking(
            callback.from_user.id,
            book_id,
            book_title,
            office,
            dur
        )
        user_info = await get_user_info(callback.from_user.id)
        if user_info:
            last_name = user_info['last_name']
            await bot.send_message(
                GROUP_CHAT_ID,
                f"✅️ Бронирование: Пользователь {first_name} {last_name} (ID: {callback.from_user.id}) "
                f"забронировал книгу '{book_title}' на срок {dur}"
            )
        await safe_edit_message(
            callback.message,
            f"{first_name}, вы бронируете книгу '{book_title}' на {dur}.",
            reply_markup=get_finish_booking_keyboard()
        )
        await state.update_data(book_title=book_title, duration=dur, office=office, first_name=first_name)
        await state.set_state(UserStates.waiting_for_booking_confirmation)
    except Exception as e:
        logger.error(f"Ошибка создания бронирования: {e}")
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
    booking_info = await get_user_booking(callback.from_user.id)
    if not booking_info or booking_info['book_title'] != book_title:
        await callback.answer("У вас нет активного бронирования этой книги")
        return

    booking_id = booking_info['booking_id']
    book_id = booking_info['book_id']

    await callback.message.edit_text("📸 Отправьте, пожалуйста, фотографию книги в библиотеки")
    await state.set_state(UserStates.waiting_for_photo)
    await state.update_data(
        book_title=book_title,
        office=user_info['office'],
        first_name=user_info['first_name'],
        last_name=user_info['last_name'],
        booking_id=booking_id,
        book_id=book_id
    )

@router.message(StateFilter(UserStates.waiting_for_photo), F.photo)
async def process_return_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    book_title = data.get('book_title')
    office = data.get('office')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    booking_id = data.get('booking_id')
    book_id = data.get('book_id')
    try:
        await complete_booking(
            message.from_user.id,
            booking_id,
            book_id,
            book_title,
            office
        )
        photo = message.photo[-1]
        await bot.send_photo(
            GROUP_CHAT_ID,
            photo.file_id,
            caption=f"❎️ Возврат: Пользователь {first_name} {last_name} (ID: {message.from_user.id}) вернул книгу '{book_title}'"
        )
        await message.answer(
            "Спасибо, что вернули книгу. Надеемся, что она была интересной и понравилась вам.",
            reply_markup=get_finish_return_keyboard()
        )
        await state.update_data(book_title=book_title, office=office, first_name=first_name, last_name=last_name)
        await state.set_state(UserStates.waiting_for_return_completion)
    except Exception as e:
        logger.error(f"Ошибка завершения бронирования: {e}")
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
    await remove_book_command(callback.from_user.id)
    await process_start_booking(callback.message, state)
    await callback.answer()

@router.callback_query(F.data.startswith("extend_"))
async def process_extend_booking(callback: CallbackQuery, state: FSMContext):
    booking_id = int(callback.data.replace("extend_", ""))
    uid = callback.from_user.id

    async with db.pool.acquire() as conn:
        booking = await conn.fetchrow(
            '''SELECT b.*, u.first_name, u.last_name 
               FROM bookings b
               JOIN users u ON b.user_id = u.user_id
               WHERE b.id = $1 AND b.user_id = $2 AND b.status = 'active' ''',
            booking_id, uid
        )
        if not booking:
            await callback.answer("❌ Бронирование не найдено или уже завершено", show_alert=True)
            return
        if booking['extension_made']:
            await callback.answer("❌ Вы уже продлевали это бронирование", show_alert=True)
            return

        try:
            new_end, ext_text = await extend_booking(booking_id, uid, booking['book_title'], booking['office'])
            await bot.send_message(
                GROUP_CHAT_ID,
                f"⚠️ Продление: Пользователь {booking['first_name']} {booking['last_name']} (ID: {uid}) "
                f"продлил бронь на {ext_text}"
            )
            await callback.message.edit_text(
                f"{booking['first_name']}, вы продлили бронь книги '{booking['book_title']}' на {ext_text}.\n"
                f"Новая дата возврата: {new_end.strftime('%d.%m.%Y %H:%M')}"
            )
            await callback.answer("✅ Бронь продлена")
        except Exception as e:
            logger.error(f"Ошибка продления: {e}")
            await callback.answer("❌ Не удалось продлить бронь", show_alert=True)

@router.message(StateFilter(UserStates.waiting_for_book_request))
async def process_book_request(message: Message, state: FSMContext):
    uid = message.from_user.id
    data = await state.get_data()
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    if not first_name or not last_name:
        ui = await get_user_info(uid)
        if ui:
            first_name = ui['first_name']
            last_name = ui['last_name']
        else:
            await message.answer("❌ Ошибка: пользователь не найден.")
            await state.clear()
            return

    req_text = message.text.strip()
    if not req_text:
        await message.answer("Пожалуйста, напишите название книги и автора.")
        return

    try:
        await bot.send_message(
            GROUP_CHAT_ID,
            f"🆕 Заказ: Пользователь {first_name} {last_name} (ID: {uid}) просит заказать в библиотеку:\n\n{req_text}"
        )
    except Exception as e:
        logger.error(f"Ошибка отправки запроса в группу: {e}")
        await message.answer("❌ Не удалось отправить запрос. Попробуйте позже.")
        await state.clear()
        return

    await add_book_command(uid)
    await message.answer(
        f"{first_name}, спасибо! Направили ваш запрос в HR-департамент!\n\n"
        "Если хотите забронировать книгу из уже имеющегося списка, нажмите в меню кнопку «Забронировать»."
    )
    await state.clear()

# ------------------------------ Админ-функции ------------------------------
async def send_all_books_list(target_message: Message):
    """Отправляет в группу полный каталог книг (с полками/этажами для Stone Towers)"""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT office, title, author, shelf, floor 
            FROM books 
            ORDER BY office, title
        ''')
    
    if not rows:
        await target_message.reply("📚 В библиотеке пока нет ни одной книги.")
        return

    offices = {}
    for r in rows:
        off = r['office']
        if off not in offices:
            offices[off] = []
        offices[off].append(r)

    text_lines = ["📚 **Полный каталог библиотеки:**\n"]
    for office, books in offices.items():
        text_lines.append(f"\n🏢 **{office}**")
        for b in books:
            line = f"  • {b['title']} — {b['author']}"
            if office == "Stone Towers" and b['shelf'] and b['floor']:
                line += f" (полка {b['shelf']}, этаж {b['floor']})"
            text_lines.append(line)

    full_text = "\n".join(text_lines)
    if len(full_text) <= 4096:
        await target_message.reply(full_text, parse_mode="Markdown")
    else:
        part = 1
        chunk = ""
        for line in text_lines:
            if len(chunk) + len(line) > 4000:
                await target_message.reply(f"📚 Каталог (часть {part}):\n{chunk}", parse_mode="Markdown")
                part += 1
                chunk = line + "\n"
            else:
                chunk += line + "\n"
        if chunk:
            await target_message.reply(f"📚 Каталог (часть {part}):\n{chunk}", parse_mode="Markdown")

async def start_books_edit(cmd_message: Message):
    global group_awaiting_action, group_awaiting_author
    group_awaiting_action = 'books'
    group_awaiting_author = cmd_message.from_user.id
    await cmd_message.reply(
        "📘 **Режим редактирования каталога**\n\n"
        "**➕ Добавление книги:**\n"
        "`+, Название, Автор, Офис, Этаж, Полка`\n"
        "• Этаж и полка — числа, **обязательны для Stone Towers**.\n"
        "• Для других офисов ставьте `-`.\n"
        "• Дубликаты **разрешены** – можно добавить сколько угодно копий.\n\n"
        "**➖ Удаление книги (ВСЕХ копий):**\n"
        "`-, Название, Офис`\n"
        "• Удаляются **все строки** с таким названием и офисом.\n\n"
        "📌 **Примеры:**\n"
        "`+, Мастер и Маргарита, Булгаков, Stone Towers, 7, 2`\n"
        "`-, Мастер и Маргарита, Stone Towers`"
    )

async def process_books_edit(message: Message):
    global group_awaiting_action, group_awaiting_author
    lines = message.text.strip().split('\n')
    results = []
    async with db.pool.acquire() as conn:
        for line in lines:
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 2:
                results.append(f"❌ Пропущена строка (недостаточно данных): {line}")
                continue

            action = parts[0]
            title = parts[1]

            if action == '+':
                if len(parts) < 6:
                    results.append(f"❌ Добавление: нужно минимум 6 полей: {line}")
                    continue
                author = parts[2]
                office = parts[3]
                try:
                    floor = int(parts[4]) if parts[4].strip() not in ('-', '') else None
                    shelf = int(parts[5]) if parts[5].strip() not in ('-', '') else None
                except ValueError:
                    results.append(f"❌ Этаж/полка должны быть числами или '-': {line}")
                    continue

                if office == "Stone Towers" and (floor is None or shelf is None):
                    results.append(f"❌ Для Stone Towers нужно указать и этаж, и полку: {line}")
                    continue

                await conn.execute(
                    '''
                    INSERT INTO books (title, author, office, shelf, floor, status)
                    VALUES ($1, $2, $3, $4, $5, 'available')
                    ''',
                    title, author, office, shelf, floor
                )
                results.append(f"✅ Добавлена копия: '{title}' ({office})")

            elif action == '-':
                if len(parts) < 3:
                    results.append(f"❌ Удаление: нужно указать название и офис: {line}")
                    continue
                office = parts[2]

                deleted = await conn.execute(
                    'DELETE FROM books WHERE LOWER(title) = LOWER($1) AND office = $2',
                    title, office
                )
                deleted_count = deleted.split()[1] if hasattr(deleted, 'split') else '0'
                results.append(f"✅ Удалено копий: {deleted_count} — '{title}' ({office})")

            else:
                results.append(f"❌ Неизвестное действие (ожидалось + или -): {line}")

    report = "📊 **Результат обработки каталога:**\n\n" + "\n".join(results)
    await message.reply(report, parse_mode="Markdown")
    group_awaiting_action = None
    group_awaiting_author = None

async def start_users_edit(cmd_message: Message):
    global group_awaiting_action, group_awaiting_author
    group_awaiting_action = 'users'
    group_awaiting_author = cmd_message.from_user.id
    await cmd_message.reply(
        "👥 **Режим управления пользователями**\n\n"
        "Направьте список действий в формате:\n"
        "`!!! telegram_id` — **удалить** пользователя (все его данные)\n"
        "`? telegram_id, имя, фамилия` — **отредактировать** имя/фамилию\n\n"
        "Каждое действие с новой строки.\n"
        "📌 **Пример:**\n"
        "!!! 123456789\n"
        "? 987654321, Иван, Петров"
    )

async def process_users_edit(message: Message):
    global group_awaiting_action, group_awaiting_author
    lines = message.text.strip().split('\n')
    results = []
    async with db.pool.acquire() as conn:
        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith('!!!'):
                parts = line[3:].strip().split()
                if len(parts) < 1:
                    results.append(f"❌ Не указан ID: {line}")
                    continue
                try:
                    uid = int(parts[0])
                except ValueError:
                    results.append(f"❌ Некорректный ID: {parts[0]}")
                    continue

                user = await conn.fetchval('SELECT user_id FROM users WHERE user_id = $1', uid)
                if not user:
                    results.append(f"❌ Пользователь {uid} не найден")
                    continue

                async with conn.transaction():
                    await conn.execute('DELETE FROM waiting_list WHERE user_id = $1', uid)
                    await conn.execute('DELETE FROM bookings WHERE user_id = $1', uid)
                    await conn.execute('DELETE FROM users WHERE user_id = $1', uid)
                results.append(f"✅ Пользователь {uid} полностью удалён")

            elif line.startswith('?'):
                parts = line[1:].strip().split(',', 2)
                if len(parts) < 3:
                    results.append(f"❌ Недостаточно данных: {line}")
                    continue
                try:
                    uid = int(parts[0].strip())
                except ValueError:
                    results.append(f"❌ Некорректный ID: {parts[0]}")
                    continue
                first_name = parts[1].strip()
                last_name = parts[2].strip()

                user = await conn.fetchval('SELECT user_id FROM users WHERE user_id = $1', uid)
                if not user:
                    results.append(f"❌ Пользователь {uid} не найден")
                    continue

                await conn.execute(
                    'UPDATE users SET first_name = $1, last_name = $2 WHERE user_id = $3',
                    first_name, last_name, uid
                )
                results.append(f"✅ Пользователь {uid} обновлён: {first_name} {last_name}")

            else:
                results.append(f"❌ Неизвестный формат: {line}")

    report = "📊 **Результат обработки пользователей:**\n\n" + "\n".join(results)
    await message.reply(report, parse_mode="Markdown")
    group_awaiting_action = None
    group_awaiting_author = None

# ------------------------------ Статистика ------------------------------
async def send_statistics(trigger_message: Message):
    """Собирает статистику по всем пользователям и отправляет в группу"""
    async with db.pool.acquire() as conn:
        users = await conn.fetch('SELECT user_id, first_name, last_name FROM users ORDER BY user_id')
        
        if not users:
            await trigger_message.reply("❌ В базе нет пользователей.")
            return

        lines = []
        for user in users:
            uid = user['user_id']
            first = user['first_name'] or ''
            last = user['last_name'] or ''
            full_name = f"{first} {last}".strip()

            active = await conn.fetchval(
                'SELECT COUNT(*) FROM bookings WHERE user_id = $1 AND status = $2',
                uid, 'active'
            ) or 0

            completed_no_ext = await conn.fetchval(
                'SELECT COUNT(*) FROM bookings WHERE user_id = $1 AND status = $2 AND extension_made = $3',
                uid, 'completed', False
            ) or 0

            completed_ext = await conn.fetchval(
                'SELECT COUNT(*) FROM bookings WHERE user_id = $1 AND status = $2 AND extension_made = $3',
                uid, 'completed', True
            ) or 0

            overdue = await conn.fetchval(
                'SELECT COUNT(*) FROM bookings WHERE user_id = $1 AND overdue_notified = $2',
                uid, True
            ) or 0

            line = (
                f"• {uid} — {full_name}\n"
                f"  ▫️ Активных: {active} | Заверш. без продл.: {completed_no_ext} | "
                f"С продл.: {completed_ext} | Просрочек: {overdue}\n"
            )
            lines.append(line)

    full_text = "📊 Статистика пользователей библиотеки:\n\n" + "".join(lines)
    
    if len(full_text) <= 4096:
        await trigger_message.reply(full_text)   # 👈 убрали parse_mode
    else:
        parts = []
        current_part = "📊 Статистика (часть 1):\n\n"
        part_num = 1
        for line in lines:
            if len(current_part) + len(line) > 4000:
                parts.append(current_part)
                part_num += 1
                current_part = f"📊 Статистика (часть {part_num}):\n\n"
            current_part += line
        parts.append(current_part)

        for part in parts:
            await trigger_message.reply(part)    # 👈 и здесь тоже
            await asyncio.sleep(0.3)

# ------------------------------ Единый обработчик группы ------------------------------
@router.message(F.chat.id == GROUP_CHAT_ID, F.text, ~F.from_user.is_bot)
async def group_text_handler(message: Message):
    """Единый обработчик всех текстовых сообщений в группе"""
    global group_awaiting_action, group_awaiting_author
    text = message.text.strip()
    user_id = message.from_user.id

    # ---------- АДМИН-КОМАНДЫ ----------
    if text.lower() == "книги":
        await send_all_books_list(message)
        return

    if text == "!книги!":
        await start_books_edit(message)
        return

    if text == "!пользователи!":
        await start_users_edit(message)
        return

    # ---------- ОЖИДАНИЕ ВВОДА СПИСКОВ ----------
    if group_awaiting_action and user_id == group_awaiting_author:
        if group_awaiting_action == 'books':
            await process_books_edit(message)
        elif group_awaiting_action == 'users':
            await process_users_edit(message)
        return

    # ---------- СТАТИСТИКА ----------
    if text.lower() == "статистика":
        await send_statistics(message)
        return

    # ---------- МАССОВАЯ РАССЫЛКА ----------
    async with db.pool.acquire() as conn:
        user_ids = await conn.fetch('SELECT user_id FROM users')
    if not user_ids:
        logger.info("Нет пользователей для рассылки")
        return

    broadcast_text = f"📢 Сообщение от администрации:\n\n{message.text}"
    sent = 0
    failed = 0
    for rec in user_ids:
        uid = rec['user_id']
        try:
            await bot.send_message(uid, broadcast_text)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Не удалось отправить пользователю {uid}: {e}")
            failed += 1
    logger.info(f"Рассылка завершена. Отправлено: {sent}, ошибок: {failed}")
    await message.reply(f"✅ Сообщение разослано {sent} пользователям. Ошибок: {failed}")

# ------------------------------ Запуск ------------------------------
async def wait_for_db():
    for i in range(10):
        try:
            await db.create_pool()
            return True
        except Exception as e:
            logger.warning(f"Не удалось подключиться к БД (попытка {i+1}/10): {e}")
            await asyncio.sleep(5)
    return False

async def main():
    try:
        logger.info("Запуск библиотечного бота...")
        if not await wait_for_db():
            logger.error("Не удалось подключиться к БД")
            return
        await init_db()
        asyncio.create_task(check_reminders())
        logger.info("Бот готов к работе!")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        await db.close()
        await bot.session.close()
        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())


