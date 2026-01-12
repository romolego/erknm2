"""Обработка уже скачанных мета-XML файлов"""
from pathlib import Path
from erknm.config import DOWNLOAD_DIR
from erknm.parser.meta_parser import parse_meta_xml
from erknm.classifier.classifier import classify_dataset
from erknm.loader.zip_loader import process_zip_archive
from erknm.loader.xml_loader import load_xml_to_db
from erknm.db.models import Dataset, DatasetVersion, OperationLog, SyncRun
from erknm.db.connection import get_connection, get_cursor
import time

def process_existing_meta_files():
    """Обработать все уже скачанные мета-XML файлы"""
    meta_dir = DOWNLOAD_DIR / "meta"
    
    if not meta_dir.exists():
        print("Папка meta не найдена")
        return
    
    # Создаем запуск синхронизации
    run = SyncRun.create(is_manual=True)
    run_id = run['id']
    
    files_processed = 0
    records_loaded = 0
    
    try:
        meta_files = list(meta_dir.glob("*.xml"))
        print(f"Найдено {len(meta_files)} мета-XML файлов")
        
        for meta_file in meta_files:
            try:
                print(f"Обработка: {meta_file.name}")
                
                # Парсим мета-XML
                meta_data = parse_meta_xml(meta_file)
                identifier = meta_data.get('identifier', meta_file.stem)
                title = meta_data.get('title', '')
                link = meta_data.get('link', '')
                
                if not link:
                    # Пробуем восстановить ссылку из list.xml
                    from erknm.parser.list_parser import parse_list_xml
                    list_xml = DOWNLOAD_DIR / "list.xml"
                    if list_xml.exists():
                        datasets = parse_list_xml(list_xml)
                        for ds in datasets:
                            if ds['identifier'] == identifier:
                                link = ds['link']
                                break
                
                # Классифицируем
                data_type = classify_dataset(identifier, title, link)
                
                # Создаем или обновляем набор данных
                dataset = Dataset.get_or_create(identifier, title, link, data_type)
                dataset_id = dataset['id']
                
                OperationLog.log(run_id, "process", f"Обработка набора: {identifier} - {title}")
                
                # Обрабатываем версии данных
                for version in meta_data.get('data_versions', []):
                    source_url = version['source']
                    
                    # Создаем запись о версии
                    DatasetVersion.create(
                        dataset_id=dataset_id,
                        source_url=source_url,
                        created_date=version.get('created', ''),
                        provenance=version.get('provenance', ''),
                        structure_version=version.get('structure', '')
                    )
                    
                    # Обрабатываем ZIP-архив
                    try:
                        xml_count = process_zip_archive(source_url, run_id)
                        
                        if xml_count > 0:
                            files_processed += 1
                            
                            # Загружаем XML-фрагменты
                            conn = get_connection()
                            cur = get_cursor(conn)
                            try:
                                cur.execute("SELECT id FROM zip_archives WHERE url = %s", (source_url,))
                                archive = cur.fetchone()
                                
                                if archive:
                                    archive_id = archive['id']
                                    cur.execute("""
                                        SELECT id FROM xml_fragments 
                                        WHERE zip_archive_id = %s AND status = 'pending'
                                    """, (archive_id,))
                                    
                                    fragments = cur.fetchall()
                                    
                                    for fragment in fragments:
                                        try:
                                            records = load_xml_to_db(fragment['id'], run_id)
                                            records_loaded += records
                                        except Exception as e:
                                            OperationLog.log(run_id, "xml_loader", 
                                                           f"Ошибка загрузки фрагмента {fragment['id']}: {str(e)}", 
                                                           level="ERROR")
                            finally:
                                cur.close()
                                conn.close()
                    except Exception as e:
                        OperationLog.log(run_id, "zip", 
                                     f"Ошибка обработки ZIP {source_url}: {str(e)}", 
                                     level="ERROR")
                        continue
                
            except Exception as e:
                print(f"Ошибка обработки {meta_file.name}: {e}")
                OperationLog.log(run_id, "process", 
                               f"Ошибка обработки {meta_file.name}: {str(e)}", 
                               level="ERROR")
                continue
        
        OperationLog.log(run_id, "process", 
                       f"Обработка завершена. Обработано файлов: {files_processed}, загружено записей: {records_loaded}")
        
        SyncRun.finish(run_id, 'completed', None, files_processed, records_loaded)
        
        print(f"\nОбработка завершена!")
        print(f"Обработано файлов: {files_processed}")
        print(f"Загружено записей: {records_loaded}")
        
    except Exception as e:
        SyncRun.finish(run_id, 'error', str(e), files_processed, records_loaded)
        print(f"Критическая ошибка: {e}")

if __name__ == '__main__':
    process_existing_meta_files()








