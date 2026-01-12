-- Скрипт для настройки прав доступа к базе данных ERKNM

-- Выдача прав на базу данных
GRANT ALL PRIVILEGES ON DATABASE erknm TO erknm_user;

-- Подключение к базе данных erknm
\c erknm

-- Выдача прав на схему public
GRANT ALL ON SCHEMA public TO erknm_user;
GRANT CREATE ON SCHEMA public TO erknm_user;

-- Выдача прав на все существующие таблицы
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO erknm_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO erknm_user;

-- Установка прав по умолчанию для будущих таблиц
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO erknm_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO erknm_user;

-- Проверка прав
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'public';







