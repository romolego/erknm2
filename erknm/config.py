"""Конфигурация приложения"""
import os
from pathlib import Path
from dotenv import load_dotenv
import re

load_dotenv()

# PostgreSQL - поддержка DATABASE_URL или отдельных параметров
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Парсим строку подключения postgresql://user:pass@host:port/dbname
    match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', DATABASE_URL)
    if match:
        DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = match.groups()
        DB_PORT = int(DB_PORT)
    else:
        # Если формат неверный, используем значения по умолчанию
        DB_HOST = os.getenv("DB_HOST", "localhost")
        DB_PORT = int(os.getenv("DB_PORT", "5432"))
        DB_NAME = os.getenv("DB_NAME", "erknm")
        DB_USER = os.getenv("DB_USER", "postgres")
        DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
else:
    # Используем отдельные параметры
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "erknm")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# Source URL
SOURCE_URL = os.getenv("SOURCE_URL", "https://proverki.gov.ru/portal/public-open-data")

# Download directory
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "./downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Log level
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Extract ZIPs to disk (deprecated, always False)
# ZIP files are processed directly from archive without extraction
EXTRACT_ZIPS = os.getenv("EXTRACT_ZIPS", "false").lower() == "true"


def get_setting(key, default=None):
    """Получить настройку из БД или дефолтное значение"""
    try:
        from erknm.db.models import Settings
        Settings.set_defaults()  # Инициализируем если нужно
        value = Settings.get(key, default)
        # Преобразуем строковые значения в нужные типы
        if value is None:
            return default
        value_str = str(value).lower()
        if value_str in ('true', 'false'):
            return value_str == 'true'
        try:
            # Пробуем преобразовать в число
            if '.' in str(value):
                return float(value)
            return int(value)
        except:
            return value
    except Exception:
        # Если БД еще не инициализирована или другие ошибки - используем дефолт
        return default


# Настройки из БД с дефолтами из переменных окружения
SCHEDULE_ENABLED = get_setting('schedule_enabled', os.getenv("SCHEDULE_ENABLED", "false").lower() == "true")
SCHEDULE_MODE = get_setting('schedule_mode', os.getenv("SCHEDULE_MODE", "daily"))
SCHEDULE_TIME = get_setting('schedule_time', os.getenv("SCHEDULE_TIME", "02:00"))
SCHEDULE_DAY_OF_WEEK = get_setting('schedule_day_of_week', int(os.getenv("SCHEDULE_DAY_OF_WEEK", "1")))
SCHEDULE_DAY_OF_MONTH = get_setting('schedule_day_of_month', int(os.getenv("SCHEDULE_DAY_OF_MONTH", "1")))
ON_ERROR = get_setting('on_error', os.getenv("ON_ERROR", "pause"))
RETRY_POLICY = get_setting('retry_policy', os.getenv("RETRY_POLICY", "fixed"))
RETRY_COUNT = get_setting('retry_count', int(os.getenv("RETRY_COUNT", "3")))
RETRY_DELAY_SECONDS = get_setting('retry_delay_seconds', int(os.getenv("RETRY_DELAY_SECONDS", "60")))
THROTTLE_SECONDS = get_setting('throttle_seconds', float(os.getenv("THROTTLE_SECONDS", "10.0")))
PROCESS_ONLY_ZIP = get_setting('process_only_zip', os.getenv("PROCESS_ONLY_ZIP", "true").lower() == "true")
UNKNOWN_POLICY = get_setting('unknown_policy', os.getenv("UNKNOWN_POLICY", "skip"))


