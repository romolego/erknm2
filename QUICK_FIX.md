# Быстрое исправление прав доступа

## Самый простой способ:

### 1. Через Python скрипт (если знаете пароль postgres):

```powershell
python setup_database_rights.py ваш_пароль_postgres
```

### 2. Через SQL команды (если есть SQL клиент):

Откройте любой SQL клиент (pgAdmin, DBeaver, или найдите psql) и выполните:

```sql
GRANT ALL PRIVILEGES ON DATABASE erknm TO erknm_user;
\c erknm
GRANT ALL ON SCHEMA public TO erknm_user;
GRANT CREATE ON SCHEMA public TO erknm_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO erknm_user;
```

### 3. Найти psql в системе:

Выполните в PowerShell:

```powershell
Get-ChildItem -Path "C:\Program Files\PostgreSQL" -Recurse -Filter "psql.exe" -ErrorAction SilentlyContinue
```

Затем используйте найденный путь:

```powershell
& "C:\Program Files\PostgreSQL\16\bin\psql.exe" -U postgres -d erknm -c "GRANT ALL PRIVILEGES ON DATABASE erknm TO erknm_user; GRANT ALL ON SCHEMA public TO erknm_user; GRANT CREATE ON SCHEMA public TO erknm_user; ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO erknm_user;"
```

## После выполнения:

1. Обновите страницу http://localhost:5000
2. Нажмите "Инициализировать БД"
3. Готово!







