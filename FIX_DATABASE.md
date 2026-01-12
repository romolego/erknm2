# Исправление ошибки прав доступа к БД

## Проблема

При инициализации БД возникает ошибка:
```
Ошибка: нет прав на создание таблиц в схеме public
```

## Решение

Выполните следующие команды в PostgreSQL:

### Вариант 1: Через psql (рекомендуется)

```bash
psql -U postgres -d erknm
```

Затем выполните:

```sql
-- Выдача прав на базу данных
GRANT ALL PRIVILEGES ON DATABASE erknm TO erknm_user;

-- Выдача прав на схему public
GRANT ALL ON SCHEMA public TO erknm_user;
GRANT CREATE ON SCHEMA public TO erknm_user;

-- Выдача прав на все существующие таблицы
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO erknm_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO erknm_user;

-- Установка прав по умолчанию для будущих таблиц
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO erknm_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO erknm_user;
```

### Вариант 2: Использовать готовый скрипт

```bash
psql -U postgres -d erknm -f setup_database.sql
```

### Вариант 3: Одной командой

```bash
psql -U postgres -d erknm -c "GRANT ALL PRIVILEGES ON DATABASE erknm TO erknm_user; GRANT ALL ON SCHEMA public TO erknm_user; GRANT CREATE ON SCHEMA public TO erknm_user; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO erknm_user;"
```

## После выполнения

1. Обновите страницу веб-интерфейса (F5)
2. Нажмите кнопку "Инициализировать БД" снова
3. Должно появиться сообщение об успешной инициализации

## Проверка прав

Для проверки прав выполните:

```sql
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'public';
```







