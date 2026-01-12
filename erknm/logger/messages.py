"""Словарь русских сообщений для журнала операций"""
# Централизованный словарь для русификации сообщений журнала

MESSAGES = {
    # ZIP операции (Dataset)
    'zip_processing_started': 'Обработка ZIP запущена',
    'zip_processed_ok': 'ZIP обработан успешно',
    'zip_downloaded': 'ZIP скачан',
    'zip_downloading': 'Скачивание ZIP',
    'zip_already_downloaded': 'ZIP уже скачан',
    'zip_already_processed': 'ZIP уже обработан, пропускаем разбор',
    'zip_processing_finished': 'Обработка ZIP завершена',
    'zip_processing_error': 'Ошибка обработки ZIP',
    'zip_not_found': 'ZIP не найден',
    'zip_marked_not_zip': 'ZIP помечен как NOT_ZIP, пропускаем',
    
    # XML операции (Dataset)
    'streaming_parse_started': 'Потоковый разбор XML запущен',
    'selected_inner_xml': 'выбран внутренний XML',
    'xml_not_found_in_zip': 'ZIP inner xml selected: нет XML файлов в архиве',
    
    # Задержки (Dataset)
    'delay_before_next_request': 'Задержка {delay:.1f}с перед следующим запросом',
    'additional_delay_before_retry': 'Дополнительная задержка {delay:.1f}с перед повтором',
    
    # Прогресс обработки записей (Data)
    'processed_records': 'Обработано {count} записей...',
    'processed_records_with_file': 'Обработано {count} записей... (файл: {filename})',
    'processed_records_with_total': 'Обработано {count}/{total} записей... (файл: {filename})',
    'records_inserted_updated': 'Вставлено/обновлено записей: {count} записей из {filename}',
    
    # Ошибки и предупреждения
    'retry_attempt': 'Повторная попытка {attempt}/{max_retries} через {wait_time:.1f}с',
    'not_zip_error': 'NOT_ZIP ошибка (не ретраим)',
    'connection_error': 'Ошибка соединения/сервера при скачивании',
    'download_error': 'Ошибка при скачивании',
    'download_error_after_retries': 'Ошибка при скачивании ZIP {url} после {max_retries} попыток',
    'download_failed': 'Не удалось скачать ZIP {url} после {max_retries} попыток',
    'parsing_error': 'Ошибка потокового парсинга',
    'insert_error': 'Ошибка вставки записи',
    'unclassified_file': 'Неклассифицированный файл или нет записей',
    'xml_selection_error': 'Ошибка при выборе XML из ZIP',
    'extraction_error': 'Ошибка при распаковке ZIP',
    'extraction_skipped': 'EXTRACT_ZIPS отключен, распаковка пропущена',
}


def get_message(key, **kwargs):
    """
    Получить русское сообщение по ключу с подстановкой параметров
    
    Args:
        key: Ключ сообщения из словаря MESSAGES
        **kwargs: Параметры для форматирования сообщения
    
    Returns:
        Отформатированное сообщение или ключ, если сообщение не найдено
    """
    template = MESSAGES.get(key)
    if template is None:
        return key  # Возвращаем ключ, если сообщение не найдено
    
    try:
        return template.format(**kwargs)
    except (KeyError, ValueError):
        # Если форматирование не удалось, возвращаем шаблон как есть
        return template

