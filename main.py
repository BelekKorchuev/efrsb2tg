import asyncio
import logging
import re

from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart

from dotenv import load_dotenv
import os

from db import fetch_unsent_links, mark_as_sent
from sender import link_parser

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
dp = Dispatcher()

@dp.message(CommandStart())
async def start_command(message: types.Message):
    await message.answer(
        "👋 Привет! Я бот ЕФРСБ Монитор.\n"
        "Я публикую объявления и оценки с ЕФРСБ в наш Telegram-канал.\n"
        f"Подписывайтесь: {CHANNEL_ID}"
    )


def build_message(data: dict) -> str:
    """
    Формирует текст сообщения на основе словаря data.
    Ожидаемые ключи в data:
    "Классификация", "Дата публикации", "Вид торгов", "Описание", "Цена",
    "Арбитражный управляющий", "ФИО должника", "ИНН", "Ссылка".
    Если какие-то ключи отсутствуют – подставляются значения по умолчанию.
    """
    # Извлечение первых 10–15 слов описания
    description_words = data.get('Описание', 'Описание отсутствует').split()
    short_description = ' '.join(description_words[:15]) + ('...' if len(description_words) > 15 else '')

    # Парсинг ФИО АУ из строки (можно усовершенствовать при необходимости)
    au_info = data.get('Арбитражный управляющий', 'Неизвестно')
    fio_au = au_info.split(' (ИНН')[0] if ' (ИНН' in au_info else au_info
    print(fio_au)
    match = re.search(r'ИНН[:\s]*(\d+)', str(au_info))
    inn = match.group(1)

    message = (
        f"#{data.get('Классификация', 'Без категории')}\n\n"
        f"📅 Дата публикации: {data.get('Дата публикации', 'Не указана')}\n"
        f"🏷️ Формат торгов: {data.get('Вид торгов', 'Не указан')}\n\n"
        f"📝 Описание: {short_description}\n\n"
        f"💰 Цена: {data.get('Цена', 'Не указана')}\n\n"
        f"👨‍💼 Арбитражный управляющий:\n"
        f"ФИО: {fio_au}\n"
        f"ИНН: {inn}\n"
        f"Телефон: отсутствует\n"
        f"E-mail: отсутствует\n\n"
        f"🏢 Должник:\n"
        f"Наименование: {data.get('ФИО должника', 'Не указано')}\n"
        f"ИНН: {data.get('ИНН', 'Не указан')}\n\n"
        f"🔗 [Открыть сообщение]({data.get('Ссылка', '#')})"
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
        logging.error("Не удалось получить данные из базы.")
        return

    for record in unsent_generator:
        message_id, link = record
        print(f'обработка: {message_id}, {link}')
        try:
            lots = link_parser(link)
            if lots:
                for lot in lots:
                    message_text = build_message(lot)
                    await send_message_to_group(message_text)
                    # Добавляем небольшую задержку между отправками
                    await asyncio.sleep(0.5)
            # Если сообщение успешно обработано, помечаем его как отправленное
            mark_as_sent(message_id)
        except Exception as e:
            logging.error(f"Ошибка обработки записи {message_id} с ссылкой {link}: {e}")

async def main():
    while True:
        try:
            await process_unsent_links()
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}")
        # Ждём 60 секунд перед следующей проверкой
        await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        # Закрываем сессию бота при завершении работы
        asyncio.run(bot.close())
