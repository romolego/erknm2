"""Основной модуль синхронизации"""
from pathlib import Path
import time
import random
from erknm.browser.downloader import download_list_xml
from erknm.parser.list_parser import parse_list_xml
from erknm.parser.meta_parser import download_meta_xml, parse_meta_xml
from erknm.classifier.classifier import classify_dataset
from erknm.loader.zip_loader import process_zip_archive
from erknm.loader.xml_loader import load_xml_to_db
from erknm.db.models import (
    SyncRun, Dataset, DatasetVersion, ZipArchive, 
    XmlFragment, OperationLog
)
from erknm.config import DOWNLOAD_DIR, SOURCE_URL


def sync(is_manual=False):
    """Выполнить полную синхронизацию"""
    run = None
    run_id = None
    files_processed = 0
    records_loaded = 0
    
    try:
        run = SyncRun.create(is_manual=is_manual)
        run_id = run['id']
        
        # Читаем настройки синхронизации
        from erknm.db.models import Settings
        Settings.set_defaults()
        sync_order = Settings.get('sync_order', 'old_to_new')
        stop_on_repeats_enabled = Settings.get('stop_on_repeats_enabled', 'false') == 'true'
        stop_on_repeats_count = int(Settings.get('stop_on_repeats_count', '3'))
        
        # Логируем параметры синхронизации
        order_text = "От старых к новым" if sync_order == 'old_to_new' else "От новых к старым"
        OperationLog.log(run_id, "sync", f"Начало синхронизации. Порядок обработки: {order_text}", stage='general')
        
        if stop_on_repeats_enabled:
            OperationLog.log(run_id, "sync", 
                           f"Остановка по повторам включена: остановка после {stop_on_repeats_count} подряд уже обработанных наборов данных", 
                           stage='general')
        else:
            OperationLog.log(run_id, "sync", "Остановка по повторам выключена: обработка всех наборов данных", stage='general')
        
        # Шаг 1: Скачиваем list.xml через браузер (этап A: list)
        OperationLog.log(run_id, "list", "Начало обработки списка наборов данных (list.xml)", stage='list')
        OperationLog.log(run_id, "list", "Скачивание list.xml", stage='list')
        list_xml_path = download_list_xml(run_id)
        
        if not list_xml_path or not list_xml_path.exists():
            OperationLog.log(run_id, "list", "Не удалось скачать list.xml", level='ERROR', stage='list')
            raise Exception("Не удалось скачать list.xml")
        
        OperationLog.log(run_id, "list", f"list.xml успешно скачан: {list_xml_path} ({list_xml_path.stat().st_size if list_xml_path.exists() else 0} байт)", stage='list')
        
        # Шаг 2: Парсим list.xml
        OperationLog.log(run_id, "list", "Парсинг list.xml", stage='list')
        datasets_list = parse_list_xml(list_xml_path)
        OperationLog.log(run_id, "list", f"Парсинг list.xml завершен. Найдено {len(datasets_list)} наборов данных", stage='list')
        
        # Применяем порядок обработки к списку наборов данных
        if sync_order == 'new_to_old':
            datasets_list = list(reversed(datasets_list))
            OperationLog.log(run_id, "list", "Применен обратный порядок обработки наборов данных (от новых к старым)", stage='list')
        else:
            OperationLog.log(run_id, "list", "Применен исходный порядок обработки наборов данных (от старых к новым)", stage='list')
        
        # Проверяем, какие наборы данных новые (сравнение с уже известными)
        from erknm.db.connection import get_connection, get_cursor
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("SELECT identifier FROM datasets")
            existing_identifiers = {row['identifier'] for row in cur.fetchall()}
            new_datasets = [ds for ds in datasets_list if ds['identifier'] not in existing_identifiers]
            
            if new_datasets:
                OperationLog.log(run_id, "list", f"Найдено {len(new_datasets)} новых наборов данных. Начинаем их обработку", stage='list')
                for ds in new_datasets[:5]:  # Логируем первые 5 для краткости
                    OperationLog.log(run_id, "list", f"Найден новый набор данных: {ds.get('identifier', 'unknown')} - {ds.get('title', 'без названия')}", stage='list')
                if len(new_datasets) > 5:
                    OperationLog.log(run_id, "list", f"... и еще {len(new_datasets) - 5} новых наборов данных", stage='list')
            else:
                OperationLog.log(run_id, "list", "Новых наборов данных не найдено. Все наборы уже известны", stage='list')
        finally:
            cur.close()
            conn.close()
        
        # Шаг 3: Обрабатываем каждый набор данных порциями с паузами
        # Обрабатываем по 3 файла, затем пауза 30-60 секунд для предотвращения блокировки
        BATCH_SIZE = 3
        BATCH_PAUSE_MIN = 30
        BATCH_PAUSE_MAX = 60
        
        total_datasets = len(datasets_list)
        OperationLog.log(run_id, "sync", 
                        f"Начинаем обработку {total_datasets} наборов данных порциями по {BATCH_SIZE} с паузами", stage='general')
        
        # Счётчик подряд идущих повторов для остановки по повторам
        consecutive_repeats = 0
        
        for batch_start in range(0, total_datasets, BATCH_SIZE):
            # Проверяем, не запрошена ли остановка перед каждой порцией
            if SyncRun.is_stop_requested(run_id):
                OperationLog.log(run_id, "sync", "Остановка всех процессов запрошена пользователем", level='WARNING', stage='general')
                OperationLog.log(run_id, "sync", f"Остановлено на шаге: обработка порций наборов данных (обработано файлов: {files_processed}, записей: {records_loaded})", level='INFO', stage='general')
                # Используем 'stopped' для явной остановки (finish переведет stopping -> stopped автоматически)
                SyncRun.finish(run_id, 'stopped', 'Остановлено пользователем', files_processed, records_loaded)
                return
            
            batch_end = min(batch_start + BATCH_SIZE, total_datasets)
            batch_datasets = datasets_list[batch_start:batch_end]
            
            OperationLog.log(run_id, "sync", 
                           f"Обработка порции {batch_start + 1}-{batch_end} из {total_datasets}", stage='general')
            
            for dataset_info in batch_datasets:
                # Проверяем остановку перед каждым файлом
                if SyncRun.is_stop_requested(run_id):
                    OperationLog.log(run_id, "sync", "Остановка всех процессов запрошена пользователем", level='WARNING', stage='general')
                    OperationLog.log(run_id, "dataset", f"Завершаю текущий набор данных: {dataset_info.get('identifier', 'unknown')}", level='INFO', stage='dataset')
                    OperationLog.log(run_id, "sync", f"Остановлено на шаге: обработка набора данных (обработано файлов: {files_processed}, записей: {records_loaded})", level='INFO', stage='general')
                    # Используем 'stopped' для явной остановки
                    SyncRun.finish(run_id, 'stopped', 'Остановлено пользователем', files_processed, records_loaded)
                    return
                
                try:
                    identifier = dataset_info['identifier']
                    title = dataset_info['title']
                    link = dataset_info['link']
                    
                    # Этап B: Обработка набора данных (dataset)
                    OperationLog.log(run_id, "dataset", f"Обработка набора данных: {identifier} - {title} (URL: {link})", stage='dataset')
                    
                    # Классифицируем набор
                    data_type = classify_dataset(identifier, title, link)
                    
                    # Создаем или обновляем запись о наборе данных
                    dataset = Dataset.get_or_create(identifier, title, link, data_type)
                    dataset_id = dataset['id']
                    is_new = dataset['identifier'] == identifier and dataset_id
                    
                    # Если тип изменился, обновляем
                    if dataset['data_type'] != data_type and data_type:
                        Dataset.update_type(dataset_id, data_type)
                    
                    # Скачиваем мета-XML
                    meta_xml_path = DOWNLOAD_DIR / "meta" / f"{identifier}.xml"
                    try:
                        # Проверяем, не скачан ли уже файл
                        if not meta_xml_path.exists():
                            OperationLog.log(run_id, "dataset", f"Скачивание мета-XML для набора {identifier}", stage='dataset')
                            try:
                                download_meta_xml(link, meta_xml_path, max_retries=5, delay=10.0, sync_run_id=run_id)
                            except Exception as e:
                                # Если не удалось скачать, но файл появился (race condition), используем его
                                if meta_xml_path.exists():
                                    OperationLog.log(run_id, "dataset", 
                                                   f"Ошибка скачивания, но файл появился: {meta_xml_path.name}", stage='dataset')
                                else:
                                    raise
                            
                            # Задержка между запросами уже реализована в download_meta_xml_browser
                            # Дополнительная задержка не требуется, так как браузерная автоматизация
                            # уже включает задержки 10-17 секунд между запросами
                        else:
                            OperationLog.log(run_id, "dataset", f"Используется уже скачанный файл: {meta_xml_path.name}", stage='dataset')
                        
                        OperationLog.log(run_id, "dataset", f"Парсинг мета-XML для набора {identifier}", stage='dataset')
                        meta_data = parse_meta_xml(meta_xml_path)
                        
                        # Обрабатываем версии данных
                        data_versions = meta_data.get('data_versions', [])
                        OperationLog.log(run_id, "dataset", f"В мета-XML набора {identifier} найдено {len(data_versions)} ссылок на данные", stage='dataset')
                        
                        # Применяем порядок обработки к элементам внутри набора
                        if sync_order == 'new_to_old':
                            data_versions = list(reversed(data_versions))
                            OperationLog.log(run_id, "dataset", 
                                           f"Применен обратный порядок обработки элементов набора (от новых к старым)", 
                                           stage='dataset')
                        else:
                            OperationLog.log(run_id, "dataset", 
                                           f"Применен исходный порядок обработки элементов набора (от старых к новым)", 
                                           stage='dataset')
                        
                        if not data_versions:
                            OperationLog.log(run_id, "dataset", f"В наборе {identifier} не найдено ссылок на данные", stage='dataset')
                            # Если нет версий данных, пропускаем проверку повторов (это не повтор)
                            continue
                        
                        # Флаг для отслеживания, был ли обработан хотя бы один архив в этом наборе
                        dataset_has_new_data = False
                        
                        for version in data_versions:
                            # Проверяем остановку перед каждым файлом
                            if SyncRun.is_stop_requested(run_id):
                                OperationLog.log(run_id, "sync", "Остановка всех процессов запрошена пользователем", level='WARNING', stage='general')
                                OperationLog.log(run_id, "dataset", f"Завершаю обработку набора данных: {identifier}", level='INFO', stage='dataset')
                                OperationLog.log(run_id, "sync", f"Остановлено на шаге: обработка версий набора данных (обработано файлов: {files_processed}, записей: {records_loaded})", level='INFO', stage='general')
                                SyncRun.finish(run_id, 'stopped', 'Остановлено пользователем', files_processed, records_loaded)
                                return
                            
                            source_url = version['source']
                            
                            # Создаем запись о версии
                            DatasetVersion.create(
                                dataset_id=dataset_id,
                                source_url=source_url,
                                created_date=version.get('created', ''),
                                provenance=version.get('provenance', ''),
                                structure_version=version.get('structure', '')
                            )
                            
                            OperationLog.log(run_id, "dataset", f"Найдена ссылка на данные: {source_url}. Запускаем обработку ZIP", stage='dataset')
                            
                            # Обрабатываем ZIP-архив (инкрементальная загрузка) - этап C: data
                            # Строгая последовательность: download → parse → insert → next
                            # process_zip_archive теперь сам загружает данные в БД через потоковый парсинг
                            try:
                                records_count = process_zip_archive(source_url, run_id)
                                
                                # records_count может быть 0 если уже обработан (skip) - это нормально
                                # Увеличиваем счетчики только если была реальная обработка
                                if records_count >= 0:  # 0 = пропущен, >0 = обработан
                                    if records_count > 0:
                                        files_processed += 1
                                        records_loaded += records_count
                                        dataset_has_new_data = True
                                        
                            except StopIteration as stop_ex:
                                # Остановка запрошена - завершаем синхронизацию
                                OperationLog.log(run_id, "sync", "Остановка всех процессов запрошена пользователем", level='WARNING', stage='general')
                                OperationLog.log(run_id, "sync", f"Остановлено на шаге: обработка ZIP-архива (обработано файлов: {files_processed}, записей: {records_loaded})", level='INFO', stage='general')
                                SyncRun.finish(run_id, 'stopped', 'Остановлено пользователем', files_processed, records_loaded)
                                return
                            except Exception as e:
                                OperationLog.log(run_id, "dataset", 
                                             f"Ошибка обработки ZIP {source_url}: {str(e)}", 
                                             level="ERROR", stage='dataset')
                                continue
                        
                        # Проверяем остановку по повторам на уровне набора данных (уровень A)
                        # Набор считается "повтором", если все его архивы уже обработаны (dataset_has_new_data = False)
                        if stop_on_repeats_enabled:
                            if not dataset_has_new_data:
                                # Все архивы в наборе уже обработаны - это повтор
                                consecutive_repeats += 1
                                OperationLog.log(run_id, "sync", 
                                               f"Набор данных '{identifier}' уже обработан (повтор {consecutive_repeats}/{stop_on_repeats_count})", 
                                               stage='general')
                                
                                # Проверяем, достигнут ли порог остановки
                                if consecutive_repeats >= stop_on_repeats_count:
                                    OperationLog.log(run_id, "sync", 
                                                   f"Остановка синхронизации: достигнут порог {stop_on_repeats_count} подряд уже обработанных наборов данных", 
                                                   level='INFO', stage='general')
                                    SyncRun.finish(run_id, 'completed', 
                                                  f'Остановлено по повторам: {stop_on_repeats_count} подряд уже обработанных наборов данных', 
                                                  files_processed, records_loaded)
                                    return
                            else:
                                # Набор содержит новые данные - сбрасываем счётчик повторов
                                consecutive_repeats = 0
                        
                    except Exception as e:
                        error_msg = str(e)
                        # Если файл уже скачан, пробуем его обработать
                        if meta_xml_path.exists():
                            try:
                                OperationLog.log(run_id, "dataset", 
                                             f"Ошибка скачивания, но файл существует. Пробуем обработать: {identifier}", stage='dataset')
                                meta_data = parse_meta_xml(meta_xml_path)
                                # Продолжаем обработку версий данных
                                data_versions_fallback = meta_data.get('data_versions', [])
                                
                                if not data_versions_fallback:
                                    # Если нет версий данных, пропускаем проверку повторов (это не повтор)
                                    continue
                                
                                # Применяем порядок обработки к элементам внутри набора
                                if sync_order == 'new_to_old':
                                    data_versions_fallback = list(reversed(data_versions_fallback))
                                
                                dataset_has_new_data_fallback = False
                                for version in data_versions_fallback:
                                    # Проверяем остановку перед каждым файлом
                                    if SyncRun.is_stop_requested(run_id):
                                        OperationLog.log(run_id, "sync", "Остановка всех процессов запрошена пользователем", level='WARNING', stage='general')
                                        OperationLog.log(run_id, "dataset", f"Завершаю обработку набора данных: {identifier}", level='INFO', stage='dataset')
                                        OperationLog.log(run_id, "sync", f"Остановлено на шаге: обработка версий набора данных (обработано файлов: {files_processed}, записей: {records_loaded})", level='INFO', stage='general')
                                        SyncRun.finish(run_id, 'stopped', 'Остановлено пользователем', files_processed, records_loaded)
                                        return
                                    
                                    source_url = version['source']
                                    DatasetVersion.create(
                                        dataset_id=dataset_id,
                                        source_url=source_url,
                                        created_date=version.get('created', ''),
                                        provenance=version.get('provenance', ''),
                                        structure_version=version.get('structure', '')
                                    )
                                    try:
                                        records_count = process_zip_archive(source_url, run_id)
                                        if records_count > 0:
                                            files_processed += 1
                                            records_loaded += records_count
                                            dataset_has_new_data_fallback = True
                                    except StopIteration as stop_ex:
                                        # Остановка запрошена - завершаем синхронизацию
                                        OperationLog.log(run_id, "sync", "Остановка всех процессов запрошена пользователем", level='WARNING', stage='general')
                                        OperationLog.log(run_id, "sync", f"Остановлено на шаге: обработка ZIP-архива (обработано файлов: {files_processed}, записей: {records_loaded})", level='INFO', stage='general')
                                        SyncRun.finish(run_id, 'stopped', 'Остановлено пользователем', files_processed, records_loaded)
                                        return
                                    except:
                                        pass
                                
                                # Проверяем остановку по повторам для fallback случая
                                if stop_on_repeats_enabled:
                                    if not dataset_has_new_data_fallback:
                                        consecutive_repeats += 1
                                        OperationLog.log(run_id, "sync", 
                                                       f"Набор данных '{identifier}' уже обработан (повтор {consecutive_repeats}/{stop_on_repeats_count})", 
                                                       stage='general')
                                        
                                        if consecutive_repeats >= stop_on_repeats_count:
                                            OperationLog.log(run_id, "sync", 
                                                           f"Остановка синхронизации: достигнут порог {stop_on_repeats_count} подряд уже обработанных наборов данных", 
                                                           level='INFO', stage='general')
                                            SyncRun.finish(run_id, 'completed', 
                                                          f'Остановлено по повторам: {stop_on_repeats_count} подряд уже обработанных наборов данных', 
                                                          files_processed, records_loaded)
                                            return
                                    else:
                                        consecutive_repeats = 0
                            except Exception as e2:
                                OperationLog.log(run_id, "dataset", 
                                             f"Ошибка обработки мета-XML для {identifier}: {str(e2)}", 
                                             level="ERROR", stage='dataset')
                                continue
                        else:
                            OperationLog.log(run_id, "dataset", 
                                         f"Ошибка обработки мета-XML для {identifier}: {error_msg}", 
                                         level="ERROR", stage='dataset')
                            continue
                
                except Exception as e:
                    OperationLog.log(run_id, "dataset", 
                                   f"Ошибка обработки набора данных: {str(e)}", 
                                   level="ERROR", stage='dataset')
                    continue
            
            # Пауза после каждой порции (кроме последней)
            if batch_end < total_datasets:
                pause_time = random.uniform(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                OperationLog.log(run_id, "sync", 
                               f"Пауза {pause_time:.1f}с после порции {batch_start + 1}-{batch_end} для предотвращения блокировки", stage='general')
                
                # Проверяем остановку во время паузы
                pause_steps = int(pause_time)
                for _ in range(pause_steps):
                    if SyncRun.is_stop_requested(run_id):
                        OperationLog.log(run_id, "sync", "Остановка синхронизации запрошена пользователем", stage='general')
                        SyncRun.finish(run_id, 'stopped', 'Остановлено пользователем', files_processed, records_loaded)
                        return
                    time.sleep(1)
                time.sleep(pause_time - pause_steps)
        
        # Финальная проверка остановки перед завершением
        if SyncRun.is_stop_requested(run_id):
            OperationLog.log(run_id, "sync", "Остановка завершена. Запуск остановлен", level='INFO', stage='general')
            SyncRun.finish(run_id, 'stopped', 'Остановлено пользователем', files_processed, records_loaded)
        else:
            OperationLog.log(run_id, "sync", 
                           f"Синхронизация завершена. Обработано файлов: {files_processed}, загружено записей: {records_loaded}", stage='general')
            SyncRun.finish(run_id, 'completed', None, files_processed, records_loaded)
        
    except Exception as e:
        error_msg = str(e)
        # Завершаем запуск, если он был создан
        if run_id is not None:
            try:
                OperationLog.log(run_id, "sync", f"Критическая ошибка синхронизации: {error_msg}", level="ERROR", stage='general')
            except:
                pass  # Игнорируем ошибки логирования
            try:
                SyncRun.finish(run_id, 'error', error_msg, files_processed, records_loaded)
            except:
                pass  # Игнорируем ошибки завершения (но стараемся завершить)
        raise


def process_manual_file(file_path: Path, is_zip: bool = None):
    """Обработать файл вручную"""
    run = SyncRun.create(is_manual=True)
    run_id = run['id']
    
    files_processed = 0
    records_loaded = 0
    
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        OperationLog.log(run_id, "manual", f"Обработка файла: {file_path}", stage='general')
        
        # Определяем тип файла
        if is_zip is None:
            is_zip = file_path.suffix.lower() == '.zip'
        
        if is_zip:
            # Обрабатываем как ZIP через новый API (streaming)
            # Создаем временный URL для идентификации
            temp_url = f"manual://{file_path.name}"
            
            # Проверяем, не обработан ли уже
            existing = ZipArchive.exists(temp_url)
            if existing:
                OperationLog.log(run_id, "manual", "Файл уже обработан", stage='general')
                SyncRun.finish(run_id, 'completed', None, 0, 0)
                return
            
            # Создаем запись об архиве
            archive = ZipArchive.create(temp_url, file_path=str(file_path), 
                                       file_size=file_path.stat().st_size,
                                       status='downloaded', sync_run_id=run_id)
            if archive:
                archive_id = archive['id']
                
                # Используем потоковую обработку
                from erknm.loader.zip_loader import select_xml_from_zip, stream_parse_xml_from_zip
                from erknm.loader.zip_loader import calculate_sha256
                
                # Вычисляем хеш
                sha256_hash = calculate_sha256(file_path)
                ZipArchive.update_status(archive_id, 'downloaded', 
                                        sha256_hash=sha256_hash)
                
                # Выбираем XML из ZIP
                xml_selection = select_xml_from_zip(file_path, run_id)
                
                if xml_selection:
                    xml_name, zip_info = xml_selection
                    # Потоковый парсинг и загрузка в БД
                    records_count = stream_parse_xml_from_zip(file_path, xml_name, zip_info, archive_id, run_id)
                    records_loaded += records_count
                    
                    if records_count > 0:
                        ZipArchive.update_status(archive_id, 'processed')
                        files_processed = 1
                else:
                    ZipArchive.update_status(archive_id, 'error', error_message='XML файлы не найдены в архиве')
                    OperationLog.log(run_id, "manual", 
                                   "XML файлы не найдены в архиве", 
                                   level="WARNING", stage='general')
        else:
            # Обрабатываем как XML
            # Создаем временную запись о ZIP (для совместимости)
            temp_url = f"manual://{file_path.name}"
            archive = ZipArchive.create(temp_url, status='processed', sync_run_id=run_id)
            archive_id = archive['id'] if archive else None
            
            fragment = XmlFragment.create(
                zip_archive_id=archive_id,
                file_name=file_path.name,
                file_path=str(file_path),
                status='pending'
            )
            
            # Загружаем XML
            records = load_xml_to_db(fragment['id'], run_id)
            records_loaded += records
            files_processed = 1
        
        OperationLog.log(run_id, "manual", 
                       f"Обработка завершена. Обработано файлов: {files_processed}, загружено записей: {records_loaded}", stage='general')
        
        SyncRun.finish(run_id, 'completed', None, files_processed, records_loaded)
        
    except Exception as e:
        error_msg = str(e)
        OperationLog.log(run_id, "manual", f"Ошибка обработки файла: {error_msg}", level="ERROR", stage='general')
        SyncRun.finish(run_id, 'error', error_msg, files_processed, records_loaded)
        raise

