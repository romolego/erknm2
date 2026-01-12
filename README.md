# Робот сбора и инкрементальной загрузки открытых данных ФГИС ЕРКНМ

Автономный сервис для автоматического сбора открытых данных ФГИС «Единый реестр контрольных (надзорных) мероприятий» и загрузки их в PostgreSQL.

## Возможности

- Автоматическая загрузка данных через браузерную автоматизацию (Playwright)
- Инкрементальная загрузка только новых ZIP-архивов
- Классификация данных (планы проверок / проверки)
- Защита от дублей
- Детальное журналирование всех операций
- Ручной режим запуска и загрузки файлов

## Установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
playwright install chromium
```

2. Настройте базу данных PostgreSQL и создайте файл `.env` на основе `.env.example`

3. Инициализируйте схему базы данных:
```bash
python -m erknm.db init
```

## Использование

### Инициализация базы данных
```bash
python -m erknm.cli init
```

### Автоматическая синхронизация
```bash
python -m erknm.cli sync-cmd
```

### Запуск по расписанию (каждые 24 часа)
```bash
python -m erknm.scheduler
```

### Ручная загрузка файла
```bash
python -m erknm.cli load-file path/to/file.zip
```

### Просмотр журнала
```bash
python -m erknm.cli show-logs
```

### Просмотр запусков
```bash
python -m erknm.cli show-runs
```

## Структура проекта

- `erknm/` - основной пакет
  - `db/` - модуль работы с БД
    - `connection.py` - подключение к PostgreSQL
    - `schema.py` - схема базы данных
    - `models.py` - модели для работы с БД
  - `browser/` - браузерная автоматизация
    - `downloader.py` - загрузка list.xml через Playwright
  - `parser/` - парсинг XML
    - `list_parser.py` - парсинг list.xml
    - `meta_parser.py` - парсинг мета-XML файлов
  - `classifier/` - классификация данных
    - `classifier.py` - классификатор данных
  - `loader/` - загрузка в БД
    - `zip_loader.py` - обработка ZIP-архивов
    - `xml_loader.py` - загрузка XML в БД
  - `sync/` - модуль синхронизации
    - `synchronizer.py` - основной модуль синхронизации
  - `reclassify.py` - переклассификация данных
  - `scheduler.py` - планировщик запусков
  - `cli.py` - CLI интерфейс
  - `config.py` - конфигурация

## Примеры использования

### Полный цикл синхронизации

```bash
# 1. Инициализация БД
python -m erknm.cli init

# 2. Запуск синхронизации
python -m erknm.cli sync-cmd

# 3. Просмотр результатов
python -m erknm.cli show-runs
python -m erknm.cli show-logs
```

### Ручная загрузка файла

```bash
# Загрузка ZIP-архива
python -m erknm.cli load-file data.zip

# Загрузка XML файла
python -m erknm.cli load-file data.xml --xml
```

### Переклассификация данных

```bash
# Переклассифицировать набор данных
python -m erknm.cli reclassify-dataset-cmd 1 plan

# Переклассифицировать XML-фрагмент
python -m erknm.cli reclassify-fragment-cmd 5 inspection
```

### Запуск по расписанию

```bash
# Запуск каждые 24 часа
python -m erknm.scheduler

# Запуск каждые 12 часов
python -m erknm.scheduler 12
```

## Структура базы данных

- `sync_runs` - запуски синхронизации
- `datasets` - наборы данных
- `dataset_versions` - версии наборов данных
- `zip_archives` - ZIP-архивы
- `xml_fragments` - XML-фрагменты
- `plans_raw` - планы проверок (сырой XML)
- `inspections_raw` - проверки (сырой XML)
- `operation_log` - журнал операций

