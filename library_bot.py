import logging
import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup
from aiogram.filters import CommandStart, StateFilter
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

# Проверяем обязательные переменные
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

# Класс для работы с базой данных
class Database:
    def __init__(self):
        self.pool: Optional[Pool] = None
    
    async def create_pool(self):
        """Создание пула соединений с базой данных"""
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
        """Закрытие пула соединений"""
        if self.pool:
            await self.pool.close()
            logger.info("Пул соединений закрыт")

# Глобальный объект базы данных
db = Database()

# Состояния FSM
class UserStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_office = State()
    waiting_for_book_title = State()
    waiting_for_confirmation = State()
    waiting_for_duration = State()
    waiting_for_photo = State()
    waiting_for_waitlist_choice = State()

# Инициализация базы данных
async def init_db():
    """Инициализация таблиц и начальных данных"""
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
        
        # Проверяем и добавляем колонки shelf и floor, если их нет
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
            # Добавляем книги в базу данных с полками и этажами для Stone Towers
            books_data = [
                # Stone Towers - с полками и этажами
                ("книга а", "автор А", "Stone Towers", 1, 5),
                ("книга в", "автор В", "Stone Towers", 4, 5),
                ("книга с", "автор С", "Stone Towers", 3, 6),
                # Manhatten - без полок и этажей
                ("книга d", "автор D", "Manhatten", None, None),
                ("книга е", "автор E", "Manhatten", None, None),
                # Известия - без полок и этажей
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
            # Обновляем существующие записи для Stone Towers с полками и этажами
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

# Получение книг по офису
async def get_books_by_office(office: str):
    """Получение списка доступных книг в офисе"""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT title, author, shelf, floor FROM books WHERE office = $1 AND status = $2',
            office, 'available'
        )
        return rows

# Проверка существования книги в офисе
async def book_exists_in_office(title: str, office: str):
    """Проверка наличия книги в указанном офисе"""
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT title, author, status, shelf, floor FROM books WHERE LOWER(title) = LOWER($1) AND office = $2',
            title, office
        )
        return row

# Обновление статуса книги
async def update_book_status(title: str, office: str, status: str):
    """Обновление статуса книги"""
    async with db.pool.acquire() as conn:
        await conn.execute(
            'UPDATE books SET status = $1 WHERE LOWER(title) = LOWER($2) AND office = $3',
            status, title, office
        )

# Создание бронирования
async def create_booking(user_id: int, book_title: str, office: str, duration: str):
    """Создание новой брони"""
    async with db.pool.acquire() as conn:
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
            end_time = start_time + timedelta(days=30)  # Упрощённо
        
        # Начинаем транзакцию
        async with conn.transaction():
            # Обновляем статус книги
            await update_book_status(book_title, office, "booked")
            
            # Удаляем пользователя из листа ожидания для этой книги
            await remove_from_waiting_list(user_id, book_title, office)
            
            # Создаем запись о бронировании
            booking_id = await conn.fetchval(
                '''
                INSERT INTO bookings (user_id, book_title, office, start_time, duration, end_time)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
                ''',
                user_id, book_title, office, start_time, duration, end_time
            )
            
            # Обновляем информацию о пользователе
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

# Получение информации о бронировании пользователя
async def get_user_booking(user_id: int):
    """Получение активной брони пользователя"""
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            '''
            SELECT current_book, booking_start, booking_duration, booking_end 
            FROM users WHERE user_id = $1 AND status = 'booked'
            ''',
            user_id
        )
        return row

# Завершение бронирования
async def complete_booking(user_id: int, book_title: str, office: str):
    """Завершение бронирования книги"""
    async with db.pool.acquire() as conn:
        # Начинаем транзакцию
        async with conn.transaction():
            # Обновляем статус книги
            await update_book_status(book_title, office, "available")
            
            # Обновляем статус пользователя
            await conn.execute(
                '''
                UPDATE users 
                SET current_book = NULL, booking_start = NULL, 
                    booking_duration = NULL, booking_end = NULL, status = 'available'
                WHERE user_id = $1
                ''',
                user_id
            )
            
            # Обновляем статус бронирования
            await conn.execute(
                '''
                UPDATE bookings 
                SET status = 'completed' 
                WHERE user_id = $1 AND book_title = $2 AND status = 'active'
                ''',
                user_id, book_title
            )
            
            # Уведомляем первого в листе ожидания
            await notify_next_in_waiting_list(book_title, office)

# Регистрация нового пользователя
async def register_user(user_id: int, first_name: str, last_name: str):
    """Регистрация или обновление пользователя"""
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

# Обновление офиса пользователя
async def update_user_office(user_id: int, office: str):
    """Обновление офиса пользователя"""
    async with db.pool.acquire() as conn:
        await conn.execute(
            'UPDATE users SET office = $1 WHERE user_id = $2',
            office, user_id
        )

# Получение информации о пользователе
async def get_user_info(user_id: int):
    """Получение информации о пользователе"""
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT first_name, last_name, office, status FROM users WHERE user_id = $1',
            user_id
        )
        return row

# Добавление в лист ожидания
async def add_to_waiting_list(user_id: int, book_title: str, office: str):
    """Добавление пользователя в лист ожидания для книги"""
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

# Получение первого в листе ожидания
async def get_first_in_waiting_list(book_title: str, office: str):
    """Получение первого пользователя в листе ожидания для книги"""
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

# Удаление из листа ожидания
async def remove_from_waiting_list(user_id: int, book_title: str, office: str):
    """Удаление пользователя из листа ожидания"""
    async with db.pool.acquire() as conn:
        await conn.execute(
            '''
            DELETE FROM waiting_list 
            WHERE user_id = $1 AND book_title = $2 AND office = $3
            ''',
            user_id, book_title, office
        )

# Уведомление следующего в листе ожидания
async def notify_next_in_waiting_list(book_title: str, office: str):
    """Уведомление следующего пользователя в листе ожидания"""
    async with db.pool.acquire() as conn:
        # Получаем первого в очереди
        waiting_user = await get_first_in_waiting_list(book_title, office)
        
        if waiting_user:
            user_id = waiting_user['user_id']
            
            # Получаем информацию о пользователе
            user_info = await get_user_info(user_id)
            if user_info:
                first_name = user_info['first_name']
                
                # Отправляем уведомление
                try:
                    await bot.send_message(
                        user_id,
                        f"🎉 {first_name}, книга '{book_title}' освободилась! "
                        f"Хотите её забронировать?",
                        reply_markup=get_waitlist_notification_keyboard(book_title, office)
                    )
                    
                    # Помечаем как уведомленного
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

# Форматирование списка книг для сообщения
def format_books_list(books):
    """Форматирование списка книг для вывода"""
    if not books:
        return "В этом офисе сейчас нет доступных книг."
    
    result = "📚 Доступные книги в этом офисе:\n\n"
    for i, book in enumerate(books, 1):
        result += f"{i}. {book['title']} - {book['author']}"
        if book.get('shelf') and book.get('floor'):
            result += f" (полка {book['shelf']}, этаж {book['floor']})"
        result += "\n"
    return result

# Клавиатуры
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
    """Кнопка для возврата книги"""
    builder = InlineKeyboardBuilder()
    builder.button(text=f"Вернуть книгу {book_title}", callback_data=f"return_{book_title}")
    builder.adjust(1)
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

# Безопасное редактирование сообщений
async def safe_edit_message(message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    """Безопасное редактирование сообщения с обработкой ошибок"""
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            logger.warning("Сообщение не изменено - отправляем как новое")
            await message.answer(text, reply_markup=reply_markup)
        else:
            raise

# Фоновая задача для проверки напоминаний
async def check_reminders():
    """Фоновая задача для проверки и отправки напоминаний"""
    while True:
        try:
            async with db.pool.acquire() as conn:
                # Получаем всех пользователей с активными бронированиями
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
                    
                    # Храним время последнего напоминания для каждого пользователя
                    last_reminder_key = f"last_reminder_{user_id}_{book_title}"
                    
                    # Проверяем напоминания для разных сроков
                    if duration == "1 час":
                        # Напоминание за 15 минут до окончания
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
                        
                        # Напоминание об окончании брони каждые 2 часа
                        if current_time >= booking_end:
                            # Проверяем, когда было последнее напоминание
                            last_reminder = getattr(check_reminders, last_reminder_key, None)
                            
                            if last_reminder is None or (current_time - last_reminder) >= timedelta(hours=2):
                                try:
                                    await bot.send_message(
                                        user_id,
                                        f"Бронь книги '{book_title}' закончилась. Пожалуйста, верни книгу.",
                                        reply_markup=get_return_book_keyboard(book_title)
                                    )
                                    # Сохраняем время последнего напоминания
                                    setattr(check_reminders, last_reminder_key, current_time)
                                except Exception as e:
                                    logger.error(f"Ошибка отправки напоминания об окончании: {e}")
                    
                    elif duration == "1 неделя":
                        # Напоминание на 5-й день
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
                        
                        # Напоминание на 6-й день
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
                        
                        # Напоминание об окончании каждые 2 часа
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
                        # Напоминание на 21-й день (начало 4-й недели)
                        day_21 = booking_start + timedelta(days=21)
                        if current_time.date() == day_21.date() and current_time.hour == 9:
                            try:
                                await bot.send_message(
                                    user_id,
                                    f"Не забудь вернуть книгу '{book_title}' через неделю"
                                )
                            except Exception as e:
                                logger.error(f"Ошибка отправки напоминания за неделю: {e}")
                        
                        # Напоминание на 27-й день
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
                        
                        # Напоминание об окончании каждые 2 часа
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
        
        # Проверяем каждые 5 минут
        await asyncio.sleep(300)

# Обработчики сообщений
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start - сразу начинаем диалог"""
    await state.clear()
    
    # Проверяем, есть ли пользователь в базе данных
    user_info = await get_user_info(message.from_user.id)
    
    if user_info:
        first_name = user_info['first_name']
        office = user_info['office']
        
        if office:
            # Если офис уже известен
            await message.answer(
                f"Привет, {first_name}! Вы зашли в библиотеку Stone. Здесь вы сможете ознакомиться со списком книг в наличии, "
                "а также забронировать ту книгу, которая вам интересна. "
                "Ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
                reply_markup=get_action_keyboard()
            )
            await state.set_state(UserStates.waiting_for_book_title)
            await state.update_data(first_name=first_name, office=office)
        else:
            # Если офис не известен
            await message.answer(
                f"Привет, {first_name}! Вы зашли в библиотеку Stone. Здесь вы сможете ознакомиться со спиком книг в наличии, "
                "а также забронировать ту книгу, которая вам интересна. "
                f"{first_name}, выбери, пожалуйста, офис, в котором ты работаешь, "
                "чтобы я мог подсказать книги в наличии",
                reply_markup=get_office_keyboard()
            )
            await state.set_state(UserStates.waiting_for_office)
            await state.update_data(first_name=first_name)
    else:
        # Если пользователя нет в базе - сразу просим имя и фамилию
        await message.answer(
            "Привет! Вы зашли в библиотеку Stone. Здесь вы сможете ознакомиться со списком книг в наличии, "
            "а также забронировать ту книгу, которая вам интересна. "
            "Для начала давайте познакомимся! Напишите, пожалуйста свои Имя и Фамилию"
        )
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
    
    # Регистрируем пользователя
    await register_user(message.from_user.id, first_name, last_name)
    
    await state.update_data(first_name=first_name, last_name=last_name)
    await message.answer(
        f"{first_name}, выбери, пожалуйста, офис, в котором ты работаешь, "
        "чтобы я мог подсказать книги в наличии",
        reply_markup=get_office_keyboard()
    )
    await state.set_state(UserStates.waiting_for_office)

# Обработчик для текстовых сообщений в состояниях ожидания кнопок
@router.message(StateFilter(UserStates.waiting_for_office, 
                           UserStates.waiting_for_confirmation,
                           UserStates.waiting_for_duration,
                           UserStates.waiting_for_waitlist_choice))
async def ignore_text_in_button_states(message: Message):
    """Игнорирование текстовых сообщений в состояниях с кнопками"""
    await message.answer(
        "Пожалуйста, используй кнопки для выбора. "
        "Текстовые сообщения в этом состоянии не обрабатываются."
    )

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
    await update_user_office(callback.from_user.id, office)
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
        
        builder = InlineKeyboardBuilder()
        builder.button(text="Забронировать", callback_data="action_book")
        
        await message.answer(
            "Если захочешь забронировать книгу, просто нажми кнопку забронировать",
            reply_markup=builder.as_markup()
        )
        return
    
    # Проверяем наличие книги
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
        # Книга уже забронирована, предлагаем лист ожидания
        await message.answer(
            f"Книга '{title}' от автора {author} сейчас находится у другого пользователя. "
            "Хотите ли добавить книгу в лист ожидания?",
            reply_markup=get_waitlist_choice_keyboard()
        )
        await state.update_data(book_title=title, author=author)
        await state.set_state(UserStates.waiting_for_waitlist_choice)
        return
    
    # Книга доступна - формируем сообщение с учетом полки и этажа
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
    """Обработчик добавления в лист ожидания"""
    data = await state.get_data()
    book_title = data.get('book_title')
    office = data.get('office')
    first_name = data.get('first_name')
    
    if not book_title or not office:
        await callback.answer("Ошибка: данные не найдены")
        return
    
    # Добавляем в лист ожидания
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
    """Обработчик выбора другой книги"""
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
    """Обработчик бронирования книги из листа ожидания"""
    # Извлекаем данные из callback_data
    parts = callback.data.split("_")
    if len(parts) < 4:
        await callback.answer("Ошибка в данных")
        return
    
    book_title = "_".join(parts[2:-1])
    office = parts[-1]
    
    # Получаем информацию о пользователе
    user_info = await get_user_info(callback.from_user.id)
    if not user_info:
        await callback.answer("Ошибка: пользователь не найден")
        return
    
    first_name = user_info['first_name']
    
    # Проверяем, доступна ли книга
    book_info = await book_exists_in_office(book_title, office)
    
    if not book_info or book_info['status'] != 'available':
        await callback.answer("Книга больше не доступна")
        return
    
    shelf = book_info['shelf']
    floor = book_info['floor']
    
    await state.update_data(book_title=book_title, author=book_info['author'], office=office, first_name=first_name)
    
    # Формируем сообщение с учетом полки и этажа
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
    """Обработчик подтверждения бронирования (Да)"""
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
    """Обработчик отказа от бронирования (Нет)"""
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
    """Обработчик кнопки 'Не бронирую'"""
    builder = InlineKeyboardBuilder()
    builder.button(text="Забронировать", callback_data="action_book")
    
    await callback.message.edit_text(
        "Если захочешь забронировать книгу, просто нажми кнопку забронировать",
        reply_markup=builder.as_markup()
    )
    await state.clear()

@router.callback_query(StateFilter(UserStates.waiting_for_confirmation), F.data == "return_another")
async def process_return_another(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Забронировать другую'"""
    await callback.message.edit_text(
        "Ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
        reply_markup=get_action_keyboard()
    )
    await state.set_state(UserStates.waiting_for_book_title)

@router.callback_query(StateFilter(UserStates.waiting_for_duration), F.data.startswith("duration_"))
async def process_duration(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора длительности бронирования"""
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
        
        # Отправляем уведомление в группу
        user_info = await get_user_info(callback.from_user.id)
        if user_info:
            last_name = user_info['last_name']
            await bot.send_message(
                GROUP_CHAT_ID,
                f"Пользователь {first_name} {last_name} забронировал книгу '{book_title}' на срок {duration}"
            )
        
        # После бронирования сразу показываем кнопку для возврата книги
        await safe_edit_message(
            callback.message,
            f"{first_name}, ты забронировал книгу '{book_title}' на {duration}. "
            "Я напомню тебе, когда ты должен вернуть книгу!",
            reply_markup=get_return_book_keyboard(book_title)
        )
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

# Обработчик кнопки возврата книги
@router.callback_query(F.data.startswith("return_"))
async def process_return_book(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки возврата книги"""
    book_title = callback.data.replace("return_", "")
    
    # Получаем информацию о пользователе
    user_info = await get_user_info(callback.from_user.id)
    if not user_info:
        await callback.answer("Ошибка: пользователь не найден")
        return
    
    first_name = user_info['first_name']
    last_name = user_info['last_name']
    office = user_info['office']
    
    # Проверяем, есть ли у пользователя это бронирование
    booking_info = await get_user_booking(callback.from_user.id)
    if not booking_info or booking_info['current_book'] != book_title:
        await callback.answer("У вас нет активного бронирования этой книги")
        return
    
    await callback.message.edit_text("Отправь пожалуйста фото книги в библиотеке")
    await state.set_state(UserStates.waiting_for_photo)
    await state.update_data(
        book_title=book_title,
        office=office,
        first_name=first_name,
        last_name=last_name
    )

# Обработчик фото при возврате
@router.message(StateFilter(UserStates.waiting_for_photo), F.photo)
async def process_return_photo(message: Message, state: FSMContext):
    """Обработчик фотографии при возврате книги"""
    data = await state.get_data()
    book_title = data.get('book_title')
    office = data.get('office')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    
    # Завершаем бронирование
    try:
        await complete_booking(message.from_user.id, book_title, office)
        
        # Отправляем уведомление в группу с фото
        photo = message.photo[-1]
        await bot.send_photo(
            GROUP_CHAT_ID,
            photo.file_id,
            caption=f"Пользователь {first_name} {last_name} вернул книгу '{book_title}'"
        )
        
        # После возврата книги - пользователь свободен, можно бронировать новую
        # Получаем актуальную информацию о пользователе
        user_info = await get_user_info(message.from_user.id)
        if user_info:
            first_name = user_info['first_name']
            office = user_info['office']
            
            await message.answer(
                "Спасибо, что вернул книгу. Надеюсь она была интересной и понравилась тебе."
            )
            
            # Если офис известен - предлагаем сразу выбрать действие
            if office:
                await message.answer(
                    f"{first_name}, ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
                    reply_markup=get_action_keyboard()
                )
                await state.set_state(UserStates.waiting_for_book_title)
                await state.update_data(first_name=first_name, office=office)
            else:
                # Если офис не известен - просим выбрать офис
                await message.answer(
                    f"{first_name}, выбери, пожалуйста, офис, в котором ты работаешь, "
                    "чтобы я мог подсказать книги в наличии",
                    reply_markup=get_office_keyboard()
                )
                await state.set_state(UserStates.waiting_for_office)
                await state.update_data(first_name=first_name)
        
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка при завершении бронирования: {e}")
        
        builder = InlineKeyboardBuilder()
        builder.button(text="Попробовать снова", callback_data=f"return_{book_title}")
        
        await message.answer(
            "Произошла ошибка при обработке возврата. Пожалуйста, попробуйте ещё раз.",
            reply_markup=builder.as_markup()
        )

# Игнорируем текстовые сообщения в состоянии ожидания фото
@router.message(StateFilter(UserStates.waiting_for_photo))
async def ignore_text_during_photo(message: Message):
    """Игнорирование текстовых сообщений при ожидании фото"""
    await message.answer("Пожалуйста, отправьте фото книги, а не текстовое сообщение.")

# Обработчик для кнопки "Забронировать" в любом состоянии
@router.callback_query(F.data == "action_book")
async def process_action_book_any_state(callback: CallbackQuery, state: FSMContext):
    """Универсальный обработчик кнопки 'Забронировать'"""
    # Получаем информацию о пользователе из базы данных
    user_info = await get_user_info(callback.from_user.id)
    
    if not user_info:
        await callback.message.edit_text(
            "Похоже, мы с тобой ещё не знакомились. Напиши, пожалуйста, свои Имя и Фамилию",
            reply_markup=None
        )
        await state.set_state(UserStates.waiting_for_name)
        return
    
    first_name = user_info['first_name']
    office = user_info['office']
    
    # Если у пользователя уже есть активное бронирование
    booking_info = await get_user_booking(callback.from_user.id)
    if booking_info and booking_info['current_book']:
        current_book = booking_info['current_book']
        duration = booking_info['booking_duration']
        
        await callback.message.edit_text(
            f"{first_name}, у тебя уже есть активное бронирование книги '{current_book}' на срок {duration}. "
            f"Сначала верни эту книгу, прежде чем бронировать новую.",
            reply_markup=get_return_book_keyboard(current_book)
        )
        return
    
    # Если офис уже известен - переходим к выбору действия
    if office:
        await callback.message.edit_text(
            f"{first_name}, ты уже знаешь, какую книгу хочешь забронировать или хочешь для начала ознакомиться со списком книг в наличии?",
            reply_markup=get_action_keyboard()
        )
        await state.set_state(UserStates.waiting_for_book_title)
        await state.update_data(first_name=first_name, office=office)
    else:
        # Если офис не известен - просим выбрать офис
        await callback.message.edit_text(
            f"{first_name}, выбери, пожалуйста, офис, в котором ты работаешь, "
            "чтобы я мог подсказать книги в наличии",
            reply_markup=get_office_keyboard()
        )
        await state.set_state(UserStates.waiting_for_office)
        await state.update_data(first_name=first_name)

# Функция ожидания подключения к базе данных
async def wait_for_db():
    """Ожидание подключения к базе данных"""
    for i in range(10):
        try:
            await db.create_pool()
            return True
        except Exception as e:
            logger.warning(f"Не удалось подключиться к базе данных (попытка {i+1}/10): {e}")
            await asyncio.sleep(5)
    return False

# Основная функция запуска бота
async def main():
    """Основная функция запуска бота"""
    try:
        logger.info("Запуск библиотечного бота...")
        
        # Ожидаем подключения к базе данных
        if not await wait_for_db():
            logger.error("Не удалось подключиться к базе данных")
            return
        
        # Инициализируем базу данных
        await init_db()
        
        # Запускаем фоновую задачу для напоминаний
        asyncio.create_task(check_reminders())
        
        logger.info("Бот запущен и готов к работе!")
        
        # Запускаем polling
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        # Закрываем соединения
        await db.close()
        await bot.session.close()
        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
