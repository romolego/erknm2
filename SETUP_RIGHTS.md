# Настройка прав доступа к базе данных

## Проблема

У пользователя `erknm_user` нет прав на создание таблиц в базе данных.

## Решение

### Вариант 1: Через Python скрипт (рекомендуется)

Выполните команду, указав пароль пользователя `postgres`:

```powershell
python setup_database_rights.py ваш_пароль_postgres
```

Или через переменную окружения:

```powershell
$env:POSTGRES_PASSWORD='ваш_пароль_postgres'
python setup_database_rights.py
```

### Вариант 2: Через SQL клиент

Если у вас установлен pgAdmin, DBeaver или другой SQL клиент:

1. Подключитесь к PostgreSQL как пользователь `postgres`
2. Выполните следующие команды:

```sql
-- Выдача прав на базу данных
GRANT ALL PRIVILEGES ON DATABASE erknm TO erknm_user;

-- Подключение к базе erknm
\c erknm

-- Выдача прав на схему public
GRANT ALL ON SCHEMA public TO erknm_user;
GRANT CREATE ON SCHEMA public TO erknm_user;

-- Выдача прав на существующие таблицы
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO erknm_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO erknm_user;

-- Установка прав по умолчанию для будущих таблиц
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO erknm_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO erknm_user;
```

### Вариант 3: Найти psql и использовать его

Если PostgreSQL установлен, но psql не в PATH:

1. Найдите установку PostgreSQL (обычно `C:\Program Files\PostgreSQL\<версия>\bin\`)
2. Выполните полный путь:

```powershell
& "C:\Program Files\PostgreSQL\16\bin\psql.exe" -U postgres -d erknm -c "GRANT ALL PRIVILEGES ON DATABASE erknm TO erknm_user; GRANT ALL ON SCHEMA public TO erknm_user; GRANT CREATE ON SCHEMA public TO erknm_user; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO erknm_user;"
```

Или добавьте путь в PATH:

```powershell
$env:PATH += ";C:\Program Files\PostgreSQL\16\bin"
psql -U postgres -d erknm -f setup_database.sql
```

## После выполнения

1. Обновите страницу веб-интерфейса (F5)
2. Нажмите кнопку "Инициализировать БД"
3. Должно появиться сообщение об успешной инициализации







