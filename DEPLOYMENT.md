# Инструкция по развертыванию

## Требования

- Python 3.8+
- PostgreSQL 12+
- Chromium (устанавливается автоматически через Playwright)

## Установка

### 1. Клонирование и установка зависимостей

```bash
# Установка зависимостей Python
pip install -r requirements.txt

# Установка браузера Chromium для Playwright
playwright install chromium
```

### 2. Настройка базы данных PostgreSQL

```bash
# Создание базы данных
createdb erknm

# Или через psql
psql -U postgres -c "CREATE DATABASE erknm;"
```

### 3. Настройка конфигурации

Создайте файл `.env` на основе `.env.example`:

```bash
cp .env.example .env
```

Отредактируйте `.env` и укажите параметры подключения к БД:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=erknm
DB_USER=postgres
DB_PASSWORD=your_password
```

### 4. Инициализация схемы базы данных

```bash
python -m erknm.cli init
```

## Запуск

### Однократный запуск синхронизации

```bash
python -m erknm.cli sync-cmd
```

### Запуск по расписанию (через cron)

Добавьте в crontab:

```bash
# Синхронизация каждый день в 3:00
0 3 * * * cd /path/to/erknm && /usr/bin/python3 -m erknm.cli sync-cmd >> /var/log/erknm.log 2>&1
```

### Запуск как сервис (systemd)

Создайте файл `/etc/systemd/system/erknm.service`:

```ini
[Unit]
Description=ERKNM Data Sync Service
After=network.target postgresql.service

[Service]
Type=simple
User=erknm
WorkingDirectory=/path/to/erknm
Environment="PATH=/usr/bin:/usr/local/bin"
ExecStart=/usr/bin/python3 -m erknm.scheduler
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
```

Запуск сервиса:

```bash
sudo systemctl enable erknm
sudo systemctl start erknm
sudo systemctl status erknm
```

## Мониторинг

### Просмотр журнала операций

```bash
python -m erknm.cli show-logs --limit 100
```

### Просмотр запусков синхронизации

```bash
python -m erknm.cli show-runs --limit 20
```

### Прямой запрос к БД

```sql
-- Статистика по запускам
SELECT status, COUNT(*) as count, 
       SUM(files_processed) as total_files,
       SUM(records_loaded) as total_records
FROM sync_runs
GROUP BY status;

-- Неклассифицированные данные
SELECT COUNT(*) as unclassified_count
FROM xml_fragments
WHERE data_type IS NULL OR data_type = 'unknown';

-- Статистика по типам данных
SELECT data_type, COUNT(*) as count
FROM xml_fragments
GROUP BY data_type;
```

## Устранение неполадок

### Ошибка подключения к БД

Проверьте параметры в `.env` и доступность PostgreSQL:

```bash
psql -h localhost -U postgres -d erknm -c "SELECT 1;"
```

### Ошибка скачивания list.xml

Проверьте доступность сайта и работу браузера:

```bash
# Проверка доступности сайта
curl -I https://proverki.gov.ru/portal/public-open-data

# Переустановка браузера
playwright install chromium --force
```

### Проблемы с памятью при обработке больших ZIP

Увеличьте лимиты памяти PostgreSQL в `postgresql.conf`:

```conf
shared_buffers = 256MB
work_mem = 16MB
maintenance_work_mem = 128MB
```

## Резервное копирование

Рекомендуется настроить регулярное резервное копирование БД:

```bash
# Создание бэкапа
pg_dump -U postgres erknm > erknm_backup_$(date +%Y%m%d).sql

# Восстановление
psql -U postgres erknm < erknm_backup_20250101.sql
```








