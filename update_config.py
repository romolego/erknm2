"""Обновление конфигурации из строки подключения"""
import re
from pathlib import Path

# Строка подключения от пользователя
connection_string = "postgresql://erknm_user:erknm_pass@localhost:5432/erknm"

# Парсим строку подключения
match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', connection_string)
if match:
    user, password, host, port, database = match.groups()
    
    # Создаем содержимое .env файла с DATABASE_URL
    env_content = f"""DATABASE_URL=postgresql://{user}:{password}@{host}:{port}/{database}
SOURCE_URL=https://proverki.gov.ru/portal/public-open-data
DOWNLOAD_DIR=./downloads
LOG_LEVEL=INFO
"""
    
    # Записываем в .env
    env_path = Path('.env')
    env_path.write_text(env_content, encoding='utf-8')
    
    print("=" * 60)
    print("Конфигурация обновлена!")
    print("=" * 60)
    print(f"Хост: {host}")
    print(f"Порт: {port}")
    print(f"База данных: {database}")
    print(f"Пользователь: {user}")
    print("=" * 60)
    print("\nТеперь можно запустить веб-интерфейс:")
    print("  python run_web.py")
else:
    print("Ошибка: неверный формат строки подключения")

