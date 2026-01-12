# Быстрый старт

## Шаг 1: Установка зависимостей

```bash
# Установка Python пакетов
pip install -r requirements.txt

# Установка браузера Chromium для Playwright
playwright install chromium
```

## Шаг 2: Настройка базы данных PostgreSQL

### Создание базы данных

```bash
# Вариант 1: через psql
psql -U postgres -c "CREATE DATABASE erknm;"

# Вариант 2: через createdb
createdb erknm
```

### Настройка конфигурации

Создайте файл `.env` в корне проекта:

```bash
# Windows (PowerShell)
Copy-Item .env.example .env

# Linux/Mac
cp .env.example .env
```

Откройте `.env` и укажите параметры подключения к вашей БД:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=erknm
DB_USER=postgres
DB_PASSWORD=ваш_пароль
```

## Шаг 3: Инициализация схемы базы данных

```bash
python -m erknm.cli init
```

Должно появиться сообщение: `✓ Схема базы данных успешно инициализирована`

## Шаг 4: Запуск синхронизации

### Первый запуск (полная синхронизация)

```bash
python -m erknm.cli sync-cmd
```

Этот процесс может занять некоторое время, так как:
1. Откроется браузер и скачается list.xml
2. Будут обработаны все наборы данных
3. Скачаются ZIP-архивы (сохраняются в `downloads/zips/`)
4. XML файлы читаются напрямую из ZIP (streaming), без распаковки на диск
5. Данные загружаются в БД потоковым парсингом

### Просмотр результатов

```bash
# Посмотреть последние запуски
python -m erknm.cli show-runs

# Посмотреть журнал операций
python -m erknm.cli show-logs
```

## Дополнительные команды

### Ручная загрузка файла

Если у вас есть локальный файл (ZIP или XML):

```bash
# ZIP файл
python -m erknm.cli load-file путь/к/файлу.zip

# XML файл
python -m erknm.cli load-file путь/к/файлу.xml --xml
```

### Запуск по расписанию

Для автоматического запуска каждые 24 часа:

```bash
python -m erknm.scheduler
```

Для запуска каждые 12 часов:

```bash
python -m erknm.scheduler 12
```

## Проверка работы

После первого запуска проверьте данные в БД:

```sql
-- Подключитесь к БД
psql -U postgres -d erknm

-- Проверьте количество наборов данных
SELECT COUNT(*) FROM datasets;

-- Проверьте количество загруженных записей
SELECT 
    (SELECT COUNT(*) FROM plans_raw) as plans_count,
    (SELECT COUNT(*) FROM inspections_raw) as inspections_count;

-- Посмотрите последние запуски
SELECT * FROM sync_runs ORDER BY started_at DESC LIMIT 5;
```

## Устранение проблем

### Ошибка подключения к БД

Проверьте:
1. PostgreSQL запущен
2. Параметры в `.env` правильные
3. База данных `erknm` создана

```bash
# Проверка подключения
psql -h localhost -U postgres -d erknm -c "SELECT 1;"
```

### Ошибка при скачивании list.xml

Проверьте доступность сайта:

```bash
curl -I https://proverki.gov.ru/portal/public-open-data
```

Если сайт недоступен, попробуйте позже.

### Ошибка "ModuleNotFoundError"

Убедитесь, что все зависимости установлены:

```bash
pip install -r requirements.txt
```

### Ошибка с Playwright

Переустановите браузер:

```bash
playwright install chromium --force
```

## Хранение данных

### Структура папок

- `downloads/zips/` - скачанные ZIP-архивы (компактное хранилище)
- `downloads/meta/` - мета-XML файлы
- `downloads/extracted/` - **НЕ ИСПОЛЬЗУЕТСЯ** (устаревшая папка, можно удалить)

**Важно:** Система больше не распаковывает XML на диск. Все данные читаются напрямую из ZIP архивов через потоковый парсинг.

### Очистка устаревших файлов

Если у вас есть старая папка `downloads/extracted/`, её можно безопасно удалить:

```bash
python cleanup_extracted.py
```

Или автоматически (без подтверждения):

```bash
python cleanup_extracted.py --yes
```

Эта папка больше не используется системой, так как ZIP-архивы обрабатываются напрямую без распаковки.

## Следующие шаги

После успешного запуска:

1. Настройте автоматический запуск через cron (Linux) или Task Scheduler (Windows)
2. Настройте мониторинг через команды `show-logs` и `show-runs`
3. При необходимости используйте переклассификацию для исправления типов данных



