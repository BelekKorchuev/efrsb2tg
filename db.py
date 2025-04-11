import os

import psycopg2
from dotenv import load_dotenv
from logScript import logger
load_dotenv(dotenv_path='.env')

db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

def get_db_connection():
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        return connection
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        return None

def fetch_unsent_links(batch_size=100):
    conn = get_db_connection()
    if conn is None:
        return  # Если не удалось получить соединение, функция ничего не возвращает.
    try:
        # Используем контекстный менеджер для соединения и курсора
        with conn:
            with conn.cursor() as cur:
                query = '''
                        SELECT id, сообщение_ссылка 
                        FROM messages
                        WHERE send_to_channel = FALSE 
                          AND тип_сообщения IN ('Аукцион', 'Публичка', 'Отчет оценщика')
                    '''
                cur.execute(query)
                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break
                    for row in rows:
                        yield row
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса: {e}")
        return None

def mark_as_sent(message_id):
    conn = get_db_connection()
    if conn is None:
        return

    try:
        with conn:
            with conn.cursor() as cur:
                query = '''
                        UPDATE messages SET send_to_channel = TRUE WHERE id = %s
                        '''
                cur.execute(query, (message_id,))
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса: {e}")

