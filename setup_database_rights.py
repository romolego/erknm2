"""Скрипт для выдачи прав доступа к базе данных"""
import psycopg2
import sys
import os
from erknm.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

print("=" * 60)
print("Настройка прав доступа к базе данных")
print("=" * 60)

# Подключаемся как postgres для выдачи прав
print("\nПопытка подключения к PostgreSQL...")

try:
    # Получаем пароль из аргументов или переменной окружения
    if len(sys.argv) > 1:
        postgres_password = sys.argv[1]
    else:
        postgres_password = os.getenv('POSTGRES_PASSWORD', '')
    
    if not postgres_password:
        print("\nИспользование:")
        print("  python setup_database_rights.py <пароль_postgres>")
        print("\nИли установите переменную окружения:")
        print("  $env:POSTGRES_PASSWORD='ваш_пароль'")
        print("  python setup_database_rights.py")
        print("\nАльтернативно, выполните команды вручную через SQL клиент:")
        print(f"GRANT ALL PRIVILEGES ON DATABASE {DB_NAME} TO {DB_USER};")
        print(f"\\c {DB_NAME}")
        print(f"GRANT ALL ON SCHEMA public TO {DB_USER};")
        print(f"GRANT CREATE ON SCHEMA public TO {DB_USER};")
        print(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {DB_USER};")
        sys.exit(1)
    
    if postgres_password:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database='postgres',  # Подключаемся к postgres для выдачи прав на erknm
            user='postgres',
            password=postgres_password
        )
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        
        print("Подключение успешно!")
        print("\nВыдача прав...")
        
        # Выдача прав на базу данных
        try:
            cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {DB_NAME} TO {DB_USER}")
            print(f"✓ Права на базу данных {DB_NAME} выданы")
        except Exception as e:
            print(f"⚠ Не удалось выдать права на БД: {e}")
        
        # Подключаемся к нужной базе данных
        conn.close()
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user='postgres',
            password=postgres_password
        )
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        
        # Выдача прав на схему
        try:
            cur.execute(f"GRANT ALL ON SCHEMA public TO {DB_USER}")
            cur.execute(f"GRANT CREATE ON SCHEMA public TO {DB_USER}")
            print(f"✓ Права на схему public выданы")
        except Exception as e:
            print(f"⚠ Не удалось выдать права на схему: {e}")
        
        # Выдача прав на существующие таблицы
        try:
            cur.execute(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {DB_USER}")
            cur.execute(f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {DB_USER}")
            print(f"✓ Права на существующие таблицы выданы")
        except Exception as e:
            print(f"⚠ Не удалось выдать права на таблицы: {e}")
        
        # Установка прав по умолчанию
        try:
            cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {DB_USER}")
            cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {DB_USER}")
            print(f"✓ Права по умолчанию установлены")
        except Exception as e:
            print(f"⚠ Не удалось установить права по умолчанию: {e}")
        
        conn.commit()
        conn.close()
        
        print("\n" + "=" * 60)
        print("Настройка прав завершена!")
        print("=" * 60)
        print("\nТеперь можно инициализировать БД через веб-интерфейс.")
        
except Exception as e:
    print(f"\nОшибка: {e}")
    print("\nАльтернативный способ:")
    print("1. Откройте любой SQL клиент (pgAdmin, DBeaver, или psql если установлен)")
    print("2. Подключитесь как пользователь postgres")
    print("3. Выполните команды из файла setup_database.sql")
    print("\nИли выполните команды вручную:")
    print(f"GRANT ALL PRIVILEGES ON DATABASE {DB_NAME} TO {DB_USER};")
    print(f"\\c {DB_NAME}")
    print(f"GRANT ALL ON SCHEMA public TO {DB_USER};")
    print(f"GRANT CREATE ON SCHEMA public TO {DB_USER};")
    print(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {DB_USER};")

