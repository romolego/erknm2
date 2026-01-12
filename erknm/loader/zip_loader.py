"""Загрузчик ZIP-архивов с потоковым чтением XML"""
import zipfile
import hashlib
import io
from pathlib import Path
from typing import List, Optional, Tuple
import requests
from lxml import etree
from erknm.config import DOWNLOAD_DIR, EXTRACT_ZIPS
from erknm.db.models import ZipArchive, XmlFragment, OperationLog
from erknm.logger.messages import get_message


def calculate_sha256(file_path: Path) -> str:
    """Вычислить SHA256 хеш файла"""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def download_zip(url: str, output_path: Path, sync_run_id=None, max_retries=5, delay=10.0) -> Path:
    """
    Скачать ZIP-архив с retry механизмом и правильными заголовками браузера
    
    Args:
        url: URL ZIP-архива
        output_path: Путь для сохранения файла
        sync_run_id: ID запуска синхронизации для логирования
        max_retries: Максимальное количество попыток
        delay: Базовая задержка между попытками в секундах
    
    Returns:
        Path к скачанному файлу
    """
    import time
    import random
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    # Проверяем, не скачан ли уже файл
    if output_path.exists():
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", 
                          get_message('zip_already_downloaded') + f": {output_path.name}", 
                          stage='dataset')
        return output_path
    
    # Заголовки как у реального браузера (проверено в тестах)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://proverki.gov.ru/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
    }
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Задержка между попытками (кроме первой)
            if attempt > 0:
                # Экспоненциальная задержка с jitter
                wait_time = delay * (2 ** attempt) + random.uniform(0, 2)
                if sync_run_id:
                    OperationLog.log(sync_run_id, "dataset", 
                                   get_message('retry_attempt', 
                                              attempt=attempt + 1, 
                                              max_retries=max_retries, 
                                              wait_time=wait_time), 
                                   stage='dataset')
                time.sleep(wait_time)
            
            if sync_run_id:
                OperationLog.log(sync_run_id, "dataset", 
                               get_message('zip_downloading') + f": {url} (попытка {attempt + 1}/{max_retries})", 
                               stage='dataset')
            
            # Создаем сессию с retry
            session = requests.Session()
            session.headers.update(headers)
            retry_strategy = Retry(
                total=1,  # Уменьшаем retry на уровне адаптера, т.к. у нас свой retry
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "HEAD"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            response = session.get(
                url, 
                timeout=(30, 300),  # connect timeout, read timeout (увеличено)
                stream=True
            )
            
            # Валидация HTTP статуса - должен быть 200
            if response.status_code != 200:
                raise Exception(f"HTTP статус {response.status_code} вместо 200. Content-Type: {response.headers.get('Content-Type', 'unknown')}")
            
            # Атомарная загрузка: сначала скачиваем в .part файл
            output_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = output_path.with_suffix(output_path.suffix + '.part')
            
            # Удаляем старый .part файл если есть
            if temp_path.exists():
                temp_path.unlink()
            
            file_size = 0
            content_type = response.headers.get('Content-Type', '')
            
            # Скачиваем в .part файл
            from erknm.db.models import SyncRun
            chunk_count = 0
            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    # Проверяем остановку каждые 1000 chunk-ов (примерно каждые 8MB)
                    if sync_run_id and chunk_count > 0 and chunk_count % 1000 == 0:
                        if SyncRun.is_stop_requested(sync_run_id):
                            response.close()
                            session.close()
                            if temp_path.exists():
                                temp_path.unlink()
                            raise StopIteration("Остановка запрошена пользователем")
                    if chunk:
                        f.write(chunk)
                        file_size += len(chunk)
                        chunk_count += 1
            
            # Проверяем, что файл не пустой
            if not temp_path.exists() or temp_path.stat().st_size == 0:
                if temp_path.exists():
                    temp_path.unlink()
                raise Exception("Файл не был создан или пуст")
            
            # Валидация ZIP: проверяем сигнатуру и используем zipfile.is_zipfile
            is_valid_zip = False
            first_bytes = b''
            try:
                with open(temp_path, 'rb') as f:
                    first_bytes = f.read(4)  # Читаем первые 4 байта для диагностики
                    # PK - это сигнатура ZIP файла (PK\x03\x04 или PK\x05\x06)
                    if first_bytes[:2] == b'PK':
                        # Дополнительная проверка через zipfile
                        import zipfile
                        is_valid_zip = zipfile.is_zipfile(temp_path)
            except Exception as zip_check_error:
                is_valid_zip = False
            
            if not is_valid_zip:
                # Quarantine: сохраняем диагностическую информацию
                error_details = {
                    'url': url,
                    'content_type': content_type,
                    'file_size': file_size,
                    'first_bytes_hex': first_bytes.hex() if first_bytes else 'empty',
                    'first_bytes_repr': repr(first_bytes[:100]) if first_bytes else 'empty'
                }
                
                # Перемещаем файл в quarantine с диагностикой
                quarantine_dir = output_path.parent / 'quarantine'
                quarantine_dir.mkdir(parents=True, exist_ok=True)
                quarantine_path = quarantine_dir / f"{output_path.stem}_not_zip{output_path.suffix}"
                
                if temp_path.exists():
                    temp_path.rename(quarantine_path)
                
                error_msg = (
                    f"File is not a zip file. "
                    f"URL: {url}, "
                    f"Content-Type: {content_type}, "
                    f"Size: {file_size} bytes, "
                    f"First bytes: {error_details['first_bytes_hex']}"
                )
                
                if sync_run_id:
                    OperationLog.log(sync_run_id, "dataset", 
                                   f"NOT_ZIP: {error_msg}. Файл перемещен в quarantine: {quarantine_path.name}", 
                                   level="ERROR", stage='dataset')
                
                # Обновляем статус архива в БД как bad_download/not_zip
                try:
                    from erknm.db.connection import get_connection, get_cursor
                    conn = get_connection()
                    cur = get_cursor(conn)
                    try:
                        cur.execute("""
                            UPDATE zip_archives 
                            SET status = 'error', 
                                error_message = %s
                            WHERE url = %s
                        """, (f"NOT_ZIP: {error_msg}", url))
                        conn.commit()
                    finally:
                        cur.close()
                        conn.close()
                except:
                    pass
                
                raise Exception(f"NOT_ZIP: {error_msg}")
            
            # Файл валидный - атомарно перемещаем из .part в финальный файл
            temp_path.rename(output_path)
            
            if sync_run_id:
                OperationLog.log(sync_run_id, "dataset", 
                               get_message('zip_downloaded') + f": {output_path.name} ({output_path.stat().st_size} bytes)", 
                               stage='dataset')
            
            # КРИТИЧЕСКИ ВАЖНО: Задержка перед следующим запросом
            # Аналогично мета-XML, задержка 10+ секунд работает надежно
            wait_time = delay + random.uniform(3, 7)  # 10-17 секунд
            if sync_run_id:
                OperationLog.log(sync_run_id, "dataset", 
                               get_message('delay_before_next_request', delay=wait_time), 
                               stage='dataset')
            time.sleep(wait_time)
            
            return output_path
            
        except Exception as e:
            error_str = str(e)
            
            # Очищаем .part файл при ошибке
            temp_path = output_path.with_suffix(output_path.suffix + '.part')
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except:
                    pass
            
            # Проверяем тип ошибки для ретраев
            is_retryable_error = False
            is_connection_error = any(keyword in error_str.lower() for keyword in [
                'connection', 'reset', 'aborted', 'closed', 'timeout', 'network', 
                'max retries', 'принудительно разорвал', 'ssl', 'certificate'
            ])
            is_server_error = any(keyword in error_str.lower() for keyword in [
                '500', '502', '503', '504', '429'
            ])
            is_not_zip_error = 'not_zip' in error_str.lower() or 'not a zip' in error_str.lower()
            
            # NOT_ZIP ошибки не ретраим - это не временная проблема
            if is_not_zip_error:
                last_error = error_str
                if sync_run_id:
                    OperationLog.log(sync_run_id, "dataset", 
                                   get_message('not_zip_error') + f": {error_str}", 
                                   level="ERROR", stage='dataset')
                raise Exception(error_str)
            
            # Ретраи на SSL/timeout/5xx ошибки
            if is_connection_error or is_server_error:
                is_retryable_error = True
                last_error = get_message('connection_error') + f": {error_str}"
            else:
                last_error = get_message('download_error') + f": {error_str}"
            
            if sync_run_id:
                OperationLog.log(sync_run_id, "dataset", last_error, level="WARNING", stage='dataset')
            
            # Для ретраируемых ошибок делаем задержку перед повтором
            if is_retryable_error and attempt < max_retries - 1:
                extra_wait = delay * (2 ** attempt) + random.uniform(2, 5)
                if sync_run_id:
                    OperationLog.log(sync_run_id, "dataset", 
                                   get_message('additional_delay_before_retry', delay=extra_wait), 
                                   stage='dataset')
                time.sleep(extra_wait)
            
            if attempt == max_retries - 1:
                error_msg = get_message('download_error_after_retries', url=url, max_retries=max_retries) + f": {last_error}"
                if sync_run_id:
                    OperationLog.log(sync_run_id, "dataset", error_msg, level="ERROR", stage='dataset')
                raise Exception(error_msg)
            continue
    
    raise Exception(get_message('download_failed', url=url, max_retries=max_retries) + f": {last_error}")


def select_xml_from_zip(zip_path: Path, sync_run_id=None) -> Optional[Tuple[str, zipfile.ZipInfo]]:
    """
    Выбрать XML файл из ZIP архива.
    Правило: выбираем самый крупный *.xml файл, если несколько - первый по алфавиту среди самых крупных.
    
    Args:
        zip_path: Путь к ZIP архиву
        sync_run_id: ID запуска синхронизации для логирования
    
    Returns:
        Tuple (имя файла, ZipInfo) или None если XML не найден
    """
    try:
        xml_candidates = []
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member_name in zip_ref.namelist():
                if member_name.lower().endswith('.xml'):
                    zip_info = zip_ref.getinfo(member_name)
                    xml_candidates.append((member_name, zip_info))
        
        if not xml_candidates:
            if sync_run_id:
                OperationLog.log(sync_run_id, "dataset", 
                               get_message('xml_not_found_in_zip') + f" {zip_path.name}", 
                               level="WARNING", stage='dataset')
            return None
        
        # Сортируем по размеру (по убыванию), затем по имени (по возрастанию)
        xml_candidates.sort(key=lambda x: (-x[1].file_size, x[0]))
        
        selected_name, selected_info = xml_candidates[0]
        
        if sync_run_id:
            total_size = sum(info.file_size for _, info in xml_candidates)
            if len(xml_candidates) > 1:
                OperationLog.log(sync_run_id, "dataset", 
                               get_message('selected_inner_xml') + f": {selected_name} size={selected_info.file_size} bytes "
                               f"(из {len(xml_candidates)} XML файлов, общий размер XML: {total_size} bytes)", 
                               stage='dataset')
            else:
                OperationLog.log(sync_run_id, "dataset", 
                               get_message('selected_inner_xml') + f": {selected_name} size={selected_info.file_size} bytes", 
                               stage='dataset')
        
        return (selected_name, selected_info)
        
    except Exception as e:
        error_msg = get_message('xml_selection_error') + f": {zip_path}: {str(e)}"
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", error_msg, level="ERROR", stage='dataset')
        raise Exception(error_msg)


def stream_parse_xml_from_zip(zip_path: Path, xml_name: str, zip_info: zipfile.ZipInfo,
                               archive_id: int, sync_run_id=None) -> int:
    """
    Потоковый парсинг XML из ZIP и загрузка данных в БД.
    Использует iterparse для обработки больших XML без загрузки всего файла в память.
    
    Args:
        zip_path: Путь к ZIP архиву
        xml_name: Имя XML файла в архиве
        zip_info: ZipInfo объект для XML файла
        archive_id: ID архива в БД
        sync_run_id: ID запуска синхронизации для логирования
    
    Returns:
        Количество загруженных записей
    """
    from erknm.db.connection import get_connection, get_cursor
    from erknm.classifier.classifier import classify_xml_file
    
    conn = get_connection()
    cur = get_cursor(conn)
    
    records_count = 0
    data_type = None
    
    try:
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", 
                           get_message('streaming_parse_started') + f": {xml_name} (estimated size: {zip_info.file_size} bytes)", 
                           stage='dataset')
        
        # Используем ZipInfo.file_size для оценки размера XML без распаковки
        
        # Создаем запись о XML-фрагменте (без file_path, т.к. не распаковываем)
        fragment = XmlFragment.create(
            zip_archive_id=archive_id,
            file_name=xml_name,
            file_path=None,  # Не распаковываем на диск
            status='parsing'
        )
        fragment_id = fragment['id']
        
        # Открываем ZIP и читаем XML как поток байтов
        # КРИТИЧНО: lxml.etree.iterparse требует bytes stream, где .read() возвращает bytes
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # zipfile.ZipFile.open() БЕЗ параметра mode по умолчанию открывает в бинарном режиме
            # Но явно указываем, что нужен bytes stream для iterparse
            zip_file_obj = zip_ref.open(xml_name)  # По умолчанию бинарный режим
            import io
            
            # Оборачиваем в BufferedReader для гарантии правильного интерфейса
            # BufferedReader гарантирует, что .read() возвращает bytes
            zip_file = io.BufferedReader(zip_file_obj, buffer_size=8192)
            
            try:
                # lxml.etree.iterparse требует bytes stream, где .read() возвращает bytes
                context = etree.iterparse(zip_file, events=('end',), huge_tree=True, recover=True)
                
                # Переменные для отслеживания
                root_elem = None
                first_elem_processed = False
                
                for event, elem in context:
                    tag_lower = elem.tag.lower() if elem.tag else ''
                    
                    # Определяем корневой элемент при первом проходе
                    if root_elem is None and event == 'end':
                        # Находим корневой элемент
                        parent = elem.getparent()
                        while parent is not None:
                            root_elem = parent
                            parent = parent.getparent()
                        if root_elem is None:
                            root_elem = elem.getroottree().getroot()
                        
                        # Определяем тип по корневому элементу
                        root_tag_lower = root_elem.tag.lower() if root_elem.tag else ''
                        if 'plan' in root_tag_lower:
                            data_type = 'plan'
                        elif 'inspection' in root_tag_lower:
                            data_type = 'inspection'
                    
                    # Обрабатываем только конечные элементы PLAN и INSPECTION
                    # Извлекаем имя тега без namespace
                    tag_name = tag_lower.split('}')[-1] if '}' in tag_lower else tag_lower
                    is_plan_tag = (tag_name == 'plan')
                    is_inspection_tag = (tag_name == 'inspection')
                    
                    if event == 'end' and (is_plan_tag or is_inspection_tag):
                        # Если тип еще не определен, определяем по самому элементу
                        if not data_type:
                            if is_plan_tag:
                                data_type = 'plan'
                            elif is_inspection_tag:
                                data_type = 'inspection'
                        
                        if not first_elem_processed and data_type:
                            XmlFragment.update_status(fragment_id, 'parsing', data_type=data_type)
                            first_elem_processed = True
                        
                        if data_type:
                            xml_content = etree.tostring(elem, encoding='unicode')
                            try:
                                # Извлекаем базовые метаданные из элемента
                                record_key = None
                                record_date = None
                                payload_json = {}
                                
                                # Пробуем извлечь GUID/номер/дату и другие поля из элементов
                                try:
                                    # Ищем типичные поля
                                    guid_elem = elem.find('.//{*}GUID')
                                    if guid_elem is None:
                                        guid_elem = elem.find('.//{*}guid')
                                    if guid_elem is None:
                                        guid_elem = elem.find('.//{*}Id')
                                    if guid_elem is not None and guid_elem.text:
                                        record_key = guid_elem.text
                                        payload_json['guid'] = guid_elem.text
                                    
                                    # Ищем дату
                                    date_elem = elem.find('.//{*}Date')
                                    if date_elem is None:
                                        date_elem = elem.find('.//{*}date')
                                    if date_elem is not None and date_elem.text:
                                        try:
                                            from datetime import datetime
                                            date_str = date_elem.text[:10]  # Первые 10 символов
                                            for fmt in ['%Y-%m-%d', '%d.%m.%Y']:
                                                try:
                                                    record_date = datetime.strptime(date_str, fmt).date()
                                                    payload_json['date'] = date_elem.text
                                                    break
                                                except:
                                                    continue
                                        except:
                                            pass
                                    
                                    # Извлекаем дополнительные важные поля
                                    # Список типичных полей для извлечения
                                    important_fields = [
                                        'Number', 'number', 'Num', 'num',  # Номер
                                        'Name', 'name', 'Title', 'title',  # Название
                                        'Status', 'status', 'State', 'state',  # Статус
                                        'Type', 'type', 'Kind', 'kind',  # Тип
                                        'Region', 'region', 'Subject', 'subject',  # Регион
                                        'Organization', 'organization', 'Org', 'org',  # Организация
                                        'INN', 'inn', 'OGRN', 'ogrn', 'KPP', 'kpp',  # Реквизиты
                                        'StartDate', 'startDate', 'EndDate', 'endDate',  # Даты
                                        'Address', 'address', 'Location', 'location',  # Адрес
                                        'Inspector', 'inspector', 'Executor', 'executor',  # Исполнитель
                                        'Result', 'result', 'Conclusion', 'conclusion',  # Результат
                                        'Violations', 'violations', 'ViolationsCount',  # Нарушения
                                        'ActNumber', 'actNumber', 'OrderNumber', 'orderNumber',  # Номера документов
                                    ]
                                    
                                    for field_name in important_fields:
                                        field_elem = None
                                        # Попробуем найти с namespace
                                        if elem.nsmap:
                                            ns = elem.nsmap.get(None, "")
                                            if ns:
                                                field_elem = elem.find(f'.//{{{ns}}}{field_name}')
                                        # Попробуем найти по local-name
                                        if field_elem is None:
                                            try:
                                                field_elem = elem.find(f'.//*[local-name()="{field_name}"]')
                                            except:
                                                pass
                                        # Попробуем найти напрямую
                                        if field_elem is None:
                                            field_elem = elem.find(f'.//{field_name}')
                                        if field_elem is not None and field_elem.text:
                                            # Нормализуем имя поля (первая буква маленькая)
                                            normalized_name = field_name[0].lower() + field_name[1:] if field_name else field_name
                                            # Обрезаем очень длинные значения
                                            value = field_elem.text.strip()
                                            if len(value) > 500:
                                                value = value[:500] + '...'
                                            if value:
                                                payload_json[normalized_name] = value
                                except:
                                    pass
                                
                                # Сохраняем в plans_raw/inspections_raw
                                if data_type == 'plan':
                                    cur.execute("""
                                        INSERT INTO plans_raw (xml_fragment_id, xml_content)
                                        VALUES (%s, %s::xml)
                                    """, (fragment_id, xml_content))
                                elif data_type == 'inspection':
                                    cur.execute("""
                                        INSERT INTO inspections_raw (xml_fragment_id, xml_content)
                                        VALUES (%s, %s::xml)
                                    """, (fragment_id, xml_content))
                                
                                # Сохраняем в parsed_records для витрины (если таблица существует)
                                try:
                                    import json
                                    cur.execute("""
                                        INSERT INTO parsed_records 
                                        (zip_archive_id, xml_fragment_id, record_type, record_key, record_date, payload_json)
                                        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                                    """, (archive_id, fragment_id, data_type, record_key, record_date, 
                                          json.dumps(payload_json) if payload_json else None))
                                except Exception as e:
                                    # Таблица может не существовать - это нормально, пропускаем
                                    pass
                                
                                records_count += 1
                                
                                # Коммитим пакетами для производительности
                                if records_count % 100 == 0:
                                    conn.commit()
                                    # Проверяем остановку каждые 100 записей
                                    if sync_run_id:
                                        from erknm.db.models import SyncRun
                                        if SyncRun.is_stop_requested(sync_run_id):
                                            conn.rollback()
                                            raise StopIteration("Остановка запрошена пользователем")
                                    if sync_run_id and records_count % 1000 == 0:
                                        OperationLog.log(sync_run_id, "data", 
                                                       get_message('processed_records_with_file', 
                                                                 count=records_count, 
                                                                 filename=xml_name), 
                                                       stage='data')
                            except Exception as e:
                                # Логируем ошибку, но продолжаем обработку
                                if sync_run_id:
                                    OperationLog.log(sync_run_id, "data", 
                                                   get_message('insert_error') + f": {str(e)}", 
                                                   level="WARNING", stage='data')
                                conn.rollback()
                        
                        # Очищаем элемент из памяти
                        elem.clear()
                        # Очищаем предков для освобождения памяти
                        while elem.getprevious() is not None:
                            del elem.getparent()[0]
                
                # Финальный коммит
                if records_count > 0:
                    conn.commit()
            finally:
                zip_file.close()
        
        # Обновляем статус фрагмента
        if data_type and records_count > 0:
            XmlFragment.update_status(fragment_id, 'loaded', records_count=records_count)
            if sync_run_id:
                OperationLog.log(sync_run_id, "data", 
                               get_message('records_inserted_updated', count=records_count, filename=xml_name), 
                               stage='data')
        else:
            XmlFragment.update_status(fragment_id, 'error', 
                                     error_message='Неклассифицированные данные или нет записей',
                                     data_type='unknown')
            if sync_run_id:
                OperationLog.log(sync_run_id, "data", 
                               get_message('unclassified_file') + f": {xml_name}", 
                               level="WARNING", stage='data')
        
        return records_count
        
    except StopIteration:
        # Остановка запрошена - пробрасываем дальше
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        error_msg = str(e)
        if fragment_id:
            XmlFragment.update_status(fragment_id, 'error', error_message=error_msg)
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", 
                           get_message('parsing_error') + f" {xml_name}: {error_msg}", 
                           level="ERROR", stage='dataset')
        raise
    finally:
        cur.close()
        conn.close()


def extract_zip(zip_path: Path, extract_to: Path, sync_run_id=None) -> List[Path]:
    """
    Распаковать ZIP-архив (DEPRECATED - используется только если EXTRACT_ZIPS=True)
    
    Returns:
        Список путей к распакованным XML файлам
    """
    if not EXTRACT_ZIPS:
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", 
                           get_message('extraction_skipped'), 
                           level="WARNING", stage='dataset')
        return []
    
    try:
        extract_to.mkdir(parents=True, exist_ok=True)
        
        xml_files = []
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.namelist():
                if member.lower().endswith('.xml'):
                    zip_ref.extract(member, extract_to)
                    xml_path = extract_to / member
                    xml_files.append(xml_path)
        
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", 
                           f"Распаковано {len(xml_files)} XML файлов из {zip_path.name}", 
                           stage='dataset')
        
        return xml_files
    except Exception as e:
        error_msg = get_message('extraction_error') + f": {zip_path}: {str(e)}"
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", error_msg, level="ERROR", stage='dataset')
        raise Exception(error_msg)


def process_zip_archive(url: str, sync_run_id=None) -> int:
    """
    Обработать ZIP-архив: скачать, прочитать XML напрямую из ZIP (streaming), загрузить в БД.
    Не распаковывает XML на диск.
    Строгая последовательность: download → parse → insert → next
    
    Returns:
        Количество обработанных записей (не файлов)
    """
    from erknm.db.models import SyncRun
    
    zip_filename = Path(url).name
    zip_path = DOWNLOAD_DIR / "zips" / zip_filename
    
    # Проверяем остановку перед началом обработки
    if sync_run_id and SyncRun.is_stop_requested(sync_run_id):
        raise StopIteration("Остановка запрошена пользователем")
    
    # Шаг 1: Проверка "уже обработан?"
    from erknm.db.connection import get_connection, get_cursor
    conn = get_connection()
    cur = get_cursor(conn)
    try:
        # Проверяем по URL
        existing = ZipArchive.exists(url)
        if existing:
            cur.execute("SELECT id, status, sha256_hash, file_path, error_message FROM zip_archives WHERE id = %s", (existing['id'],))
            existing_row = cur.fetchone()
            if existing_row:
                if existing_row['status'] == 'processed':
                    if sync_run_id:
                        sha_short = existing_row['sha256_hash'][:16] + '...' if existing_row['sha256_hash'] else 'N/A'
                        OperationLog.log(sync_run_id, "dataset", 
                                       get_message('zip_already_processed') + f": {zip_filename} (sha256: {sha_short})", 
                                       stage='dataset')
                    return 0
                # Если файл помечен как NOT_ZIP, не обрабатываем его снова
                if existing_row['status'] == 'error' and existing_row['error_message'] and 'NOT_ZIP' in existing_row['error_message']:
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "dataset", 
                                       get_message('zip_marked_not_zip') + f": {zip_filename}", 
                                       level="WARNING", stage='dataset')
                    return 0
        
        # Если файл уже скачан, проверяем по хешу
        if zip_path.exists():
            file_size = zip_path.stat().st_size
            sha256_hash = calculate_sha256(zip_path)
            
            # Проверяем по хешу
            cur.execute("SELECT id, status FROM zip_archives WHERE sha256_hash = %s", (sha256_hash,))
            existing_by_hash = cur.fetchone()
            if existing_by_hash and existing_by_hash['status'] == 'processed':
                if sync_run_id:
                    OperationLog.log(sync_run_id, "dataset", 
                                   get_message('zip_already_processed') + f" (by hash): {zip_filename} (sha256: {sha256_hash[:16]}...)", 
                                   stage='dataset')
                return 0
        
    finally:
        cur.close()
        conn.close()
    
    # Шаг 2: Создаем/обновляем запись об архиве
    archive = ZipArchive.create(url, status='pending', sync_run_id=sync_run_id)
    if not archive:
        # Возможно, был создан в другом процессе - проверяем еще раз
        existing = ZipArchive.exists(url)
        if existing:
            conn2 = get_connection()
            cur2 = get_cursor(conn2)
            try:
                cur2.execute("SELECT status FROM zip_archives WHERE id = %s", (existing['id'],))
                status_row = cur2.fetchone()
                if status_row and status_row['status'] == 'processed':
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "dataset", 
                                       get_message('zip_already_processed') + f": {zip_filename}", 
                                       stage='dataset')
                    return 0
            finally:
                cur2.close()
                conn2.close()
        
        # Если все еще не удалось, пробуем еще раз
        archive = ZipArchive.exists(url)
        if not archive:
            raise Exception(f"Не удалось создать запись об архиве: {url}")
    
    # Получаем archive_id
    if isinstance(archive, dict) and 'id' in archive:
        archive_id = archive['id']
    elif existing and isinstance(existing, dict) and 'id' in existing:
        archive_id = existing['id']
    else:
        # Последняя попытка - создаем запись заново
        archive = ZipArchive.create(url, status='pending', sync_run_id=sync_run_id)
        if not archive or not isinstance(archive, dict) or 'id' not in archive:
            raise Exception(f"Не удалось создать запись об архиве: {url}")
        archive_id = archive['id']
    
    try:
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", 
                           get_message('zip_processing_started') + f": {zip_filename}", 
                           stage='dataset')
        
        # Шаг 3: Скачивание (или пропуск если уже скачан)
        if not zip_path.exists():
            try:
                download_zip(url, zip_path, sync_run_id)
            except Exception as download_error:
                error_str = str(download_error)
                # Если это NOT_ZIP ошибка, она уже обработана в download_zip (статус обновлен в БД)
                if 'NOT_ZIP' in error_str:
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "data", 
                                       f"NOT_ZIP файл обнаружен при скачивании: {zip_filename}. Пропускаем обработку.", 
                                       level="WARNING", stage='data')
                    return 0
                # Для других ошибок пробрасываем дальше
                raise
        else:
            if sync_run_id:
                OperationLog.log(sync_run_id, "dataset", 
                               get_message('zip_already_downloaded') + f": {zip_filename}", 
                               stage='dataset')
            
            # Проверяем, что существующий файл действительно ZIP
            import zipfile
            if not zipfile.is_zipfile(zip_path):
                error_msg = f"Существующий файл не является ZIP: {zip_filename}"
                ZipArchive.update_status(archive_id, 'error', error_message=f"NOT_ZIP: {error_msg}")
                if sync_run_id:
                    OperationLog.log(sync_run_id, "dataset", 
                                   f"NOT_ZIP: {error_msg}. Пропускаем обработку.", 
                                   level="ERROR", stage='dataset')
                return 0
        
        # Вычисляем хеш и размер
        file_size = zip_path.stat().st_size
        sha256_hash = calculate_sha256(zip_path)
        
        # Обновляем статус архива
        ZipArchive.update_status(archive_id, 'downloaded', 
                                 file_path=str(zip_path), 
                                 file_size=file_size, 
                                 sha256_hash=sha256_hash)
        
        # Финальная проверка - может быть обработан между проверками
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("SELECT status FROM zip_archives WHERE id = %s", (archive_id,))
            status_check = cur.fetchone()
            if status_check and status_check['status'] == 'processed':
                if sync_run_id:
                    sha_short = sha256_hash[:16] + '...'
                    OperationLog.log(sync_run_id, "dataset", 
                                   get_message('zip_already_processed') + f" (race condition): {zip_filename} (sha256: {sha_short})", 
                                   stage='dataset')
                return 0
        finally:
            cur.close()
            conn.close()
        
        # Проверяем остановку перед парсингом
        if sync_run_id and SyncRun.is_stop_requested(sync_run_id):
            ZipArchive.update_status(archive_id, 'error', error_message='Остановка запрошена пользователем')
            raise StopIteration("Остановка запрошена пользователем")
        
        # Шаг 4: Выбираем XML файл из ZIP (не распаковывая)
        xml_selection = select_xml_from_zip(zip_path, sync_run_id)
        
        if not xml_selection:
            ZipArchive.update_status(archive_id, 'error', error_message='XML файлы не найдены в архиве')
            if sync_run_id:
                OperationLog.log(sync_run_id, "dataset", 
                               get_message('zip_processing_finished') + f": {zip_filename} - XML не найден", 
                               level="WARNING", stage='dataset')
            return 0
        
        xml_name, zip_info = xml_selection
        
        # Логируем выбор XML
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", 
                           get_message('selected_inner_xml') + f": {xml_name} size={zip_info.file_size} bytes", 
                           stage='dataset')
        
        # Проверяем остановку перед потоковым парсингом
        if sync_run_id and SyncRun.is_stop_requested(sync_run_id):
            ZipArchive.update_status(archive_id, 'error', error_message='Остановка запрошена пользователем')
            raise StopIteration("Остановка запрошена пользователем")
        
        # Шаг 5: Потоковый парсинг и загрузка в БД
        records_count = stream_parse_xml_from_zip(zip_path, xml_name, zip_info, archive_id, sync_run_id)
        
        # Шаг 6: Обновляем статус архива
        ZipArchive.update_status(archive_id, 'processed')
        
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", 
                           get_message('zip_processed_ok') + f": {zip_filename}, records_written={records_count}", 
                           stage='dataset')
        
        return records_count
        
    except StopIteration:
        # Остановка запрошена - пробрасываем дальше
        raise
    except Exception as e:
        error_msg = str(e)
        ZipArchive.update_status(archive_id, 'error', error_message=error_msg)
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", 
                           get_message('zip_processing_error') + f": {zip_filename}: {error_msg}", 
                           level="ERROR", stage='dataset')
        raise
