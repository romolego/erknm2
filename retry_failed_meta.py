"""Повторная обработка наборов данных с ошибками"""
from erknm.db.connection import get_connection, get_cursor
from erknm.db.models import Dataset, DatasetVersion, OperationLog, SyncRun
from erknm.parser.meta_parser import download_meta_xml, parse_meta_xml
from erknm.classifier.classifier import classify_dataset
from erknm.loader.zip_loader import process_zip_archive
from erknm.loader.xml_loader import load_xml_to_db
from erknm.config import DOWNLOAD_DIR
import time
import random

def retry_failed_datasets():
    """Повторная обработка наборов данных, которые не удалось обработать"""
    run = SyncRun.create(is_manual=True)
    run_id = run['id']
    
    files_processed = 0
    records_loaded = 0
    
    conn = get_connection()
    cur = get_cursor(conn)
    
    try:
        # Находим наборы данных без версий (не обработанные)
        cur.execute("""
            SELECT d.id, d.identifier, d.title, d.link, d.data_type
            FROM datasets d
            LEFT JOIN dataset_versions dv ON d.id = dv.dataset_id
            WHERE dv.id IS NULL
            ORDER BY d.id
        """)
        
        failed_datasets = cur.fetchall()
        
        print(f"Найдено {len(failed_datasets)} наборов данных без версий")
        
        for dataset in failed_datasets:
            dataset_id = dataset['id']
            identifier = dataset['identifier']
            title = dataset['title']
            link = dataset['link']
            
            print(f"\nОбработка: {identifier} - {title}")
            
            try:
                OperationLog.log(run_id, "retry", f"Повторная обработка: {identifier} - {title}")
                
                # Скачиваем мета-XML
                meta_xml_path = DOWNLOAD_DIR / "meta" / f"{identifier}.xml"
                
                if not meta_xml_path.exists():
                    print(f"  Скачивание мета-XML...")
                    download_meta_xml(link, meta_xml_path, max_retries=7, delay=3.0)
                    # Задержка между запросами
                    time.sleep(2.0 + random.uniform(0, 2))
                else:
                    print(f"  Используется существующий файл")
                
                # Парсим мета-XML
                meta_data = parse_meta_xml(meta_xml_path)
                
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
                            conn2 = get_connection()
                            cur2 = get_cursor(conn2)
                            try:
                                cur2.execute("SELECT id FROM zip_archives WHERE url = %s", (source_url,))
                                archive = cur2.fetchone()
                                
                                if archive:
                                    archive_id = archive['id']
                                    cur2.execute("""
                                        SELECT id FROM xml_fragments 
                                        WHERE zip_archive_id = %s AND status = 'pending'
                                    """, (archive_id,))
                                    
                                    fragments = cur2.fetchall()
                                    
                                    for fragment in fragments:
                                        try:
                                            records = load_xml_to_db(fragment['id'], run_id)
                                            records_loaded += records
                                        except Exception as e:
                                            OperationLog.log(run_id, "xml_loader", 
                                                           f"Ошибка загрузки фрагмента {fragment['id']}: {str(e)}", 
                                                           level="ERROR")
                            finally:
                                cur2.close()
                                conn2.close()
                    except Exception as e:
                        OperationLog.log(run_id, "zip", 
                                     f"Ошибка обработки ZIP {source_url}: {str(e)}", 
                                     level="ERROR")
                        continue
                
                print(f"  ✓ Обработан успешно")
                
            except Exception as e:
                print(f"  ✗ Ошибка: {e}")
                OperationLog.log(run_id, "retry", 
                               f"Ошибка обработки {identifier}: {str(e)}", 
                               level="ERROR")
                continue
        
        OperationLog.log(run_id, "retry", 
                       f"Повторная обработка завершена. Обработано файлов: {files_processed}, загружено записей: {records_loaded}")
        
        SyncRun.finish(run_id, 'completed', None, files_processed, records_loaded)
        
        print(f"\n{'='*60}")
        print(f"Обработка завершена!")
        print(f"Обработано файлов: {files_processed}")
        print(f"Загружено записей: {records_loaded}")
        
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    retry_failed_datasets()

