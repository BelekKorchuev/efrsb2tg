import asyncio
import datetime
from logScript import logger
import re
from pprint import pprint

from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from dotenv import load_dotenv
import os
import html

from db import fetch_unsent_links, mark_as_sent
from sender import link_parser

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

@dp.message(CommandStart())
async def start_command(message: types.Message):
    await message.answer(
        "👋 Привет! Я бот ЕФРСБ Монитор.\n"
        "Я публикую объявления и оценки с ЕФРСБ в наш Telegram-канал.\n"
        f"Подписывайтесь: {CHANNEL_ID}"
    )

def filter_messages_by_current_month(messages: list) -> list:
    """
    Принимает список сообщений (каждое сообщение — словарь, где ключ "Дата публикации" содержит дату в формате "%d.%m.%Y")
    и возвращает только те, у которых год и месяц совпадают с текущим.
    """
    current_date = datetime.datetime.now()
    filtered = []
    for msg in messages:
        pub_date_str = msg.get("Дата публикации", None)
        if pub_date_str:
            try:
                pub_date = datetime.datetime.strptime(pub_date_str, "%d.%m.%Y")
                if pub_date.year == current_date.year and pub_date.month == current_date.month:
                    filtered.append(msg)
            except Exception as e:
                # Если не удается распарсить дату, можно пропустить сообщение или добавить его по умолчанию
                continue
    return filtered

def build_message(data: dict) -> str:
    # Извлечение первых 10–15 слов описания
    description_words = data.get('Описание', 'Описание отсутствует').split()
    short_description = ' '.join(description_words[:15]) + ('...' if len(description_words) > 15 else '')


    au_info = data.get('Арбитражный управляющий', '').strip()
    if not au_info or au_info == 'Неизвестно':
        au_info = data.get('Организатор торгов', 'Неизвестно').strip()

    if au_info and au_info != 'Неизвестно':
        fio_au = au_info.split(' (ИНН')[0] if ' (ИНН' in au_info else au_info
        match = re.search(r'ИНН[:\s]*(\d+)', au_info)
        if match:
            inn = match.group(1)
        else:
            inn = 'Неизвестно'
    else:
        fio_au = 'Неизвестно'
        inn = 'Неизвестно'

    message = (
        f"#{html.escape(data.get('Классификация', 'Без категории'))}\n\n"
        f"📅 <b>Дата публикации:</b> {html.escape(data.get('Дата публикации', 'Не указана'))}\n"
        f"🏷️ <b>Формат торгов:</b> {html.escape(data.get('Вид торгов', 'Не указан'))}\n\n"
        f"📝 <b>Описание:</b> {html.escape(short_description)}\n\n"
        f"💰 <b>Цена:</b> {html.escape(data.get('Цена', 'Не указана'))}\n\n"
        f"👨‍💼 <b>Арбитражный управляющий:</b>\n"
        f"ФИО: {html.escape(fio_au)}\n"
        f"ИНН: {html.escape(inn)}\n"
        f"Телефон: отсутствует\n"
        f"E-mail: {html.escape(data.get('E-mail', 'отсутствует'))}\n\n"
        f"🏢 <b>Должник:</b>\n"
        f"Наименование: {html.escape(data.get('ФИО должника', 'Не указано'))}\n"
        f"ИНН: {html.escape(data.get('ИНН', 'Не указан'))}\n\n"
        f"🔗 <a href=\"{html.escape(data.get('Ссылка', '#'))}\">Открыть сообщение</a>"
    )
    return message


async def send_message_to_group(message_text: str):
    """
    Отправляет сообщение в группу или канал.
    Используется CHANNEL_ID из переменных окружения.
    """
    try:
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=message_text,
            disable_web_page_preview=True
        )
        print("Сообщение успешно отправлено!")
    except TelegramBadRequest as e:
        error_msg = str(e)
        if "Too Many Requests" in error_msg and "retry after" in error_msg:
            # Ищем число секунд, которое нужно ждать
            m = re.search(r"retry after (\d+)", error_msg)
            if m:
                retry_after = int(m.group(1))
            else:
                retry_after = 10  # значение по умолчанию
            logger.error(f"Flood control: ждем {retry_after} секунд перед повторной отправкой")
            await asyncio.sleep(retry_after)
            # Повторяем отправку
            await send_message_to_group(message_text)
        else:
            logger.error(f"Ошибка при отправке сообщения: {e}")

    except Exception as e:
        print(f"Ошибка при отправке сообщения: {e}")

async def process_unsent_links():
    """
    Получает данные из базы, парсит страницу по ссылке и отправляет каждый лот отдельным сообщением.
    После успешной отправки помечает запись как обработанную.
    """
    # Получаем генератор записей (каждая запись: (message_id, ссылка))
    unsent_generator = fetch_unsent_links(batch_size=100)
    if unsent_generator is None:
        logger.error("Не удалось получить данные из базы.")
        return

    for record in unsent_generator:
        message_id, link = record
        logger.info(f'обработка: {message_id}, {link}')
        try:
            messages = link_parser(link)
            messages = filter_messages_by_current_month(messages)
            pprint(messages)
            logger.info('\n\n\n')
            if messages:
                for msg  in messages:
                    message_text = build_message(msg)
                    await send_message_to_group(message_text)
                    # Добавляем небольшую задержку между отправками
                    await asyncio.sleep(1.5)
            # Если сообщение успешно обработано, помечаем его как отправленное
                mark_as_sent(message_id)
        except Exception as e:
            logger.error(f"Ошибка обработки записи {message_id} с ссылкой {link}: {e}")

async def main():
    while True:
        try:
            await process_unsent_links()
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")
        # Ждём 60 секунд перед следующей проверкой
        await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        # Закрываем сессию бота при завершении работы
        asyncio.run(bot.close())
