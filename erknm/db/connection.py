"""Подключение к PostgreSQL"""
import psycopg2
from psycopg2.extras import RealDictCursor
from erknm.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def get_connection():
    """Получить подключение к БД"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        # Устанавливаем кодировку UTF-8 явно
        conn.set_client_encoding('UTF8')
        return conn
    except Exception as e:
        error_msg = str(e)
        # Обработка ошибок кодировки
        if 'codec' in error_msg.lower() or 'encoding' in error_msg.lower():
            raise ConnectionError(f"Ошибка кодировки при подключении к БД. Проверьте настройки PostgreSQL.")
        raise ConnectionError(f"Ошибка подключения к БД: {error_msg}. Проверьте параметры в .env файле.")


def get_cursor(connection):
    """Получить курсор с RealDictCursor"""
    return connection.cursor(cursor_factory=RealDictCursor)


