"""Загрузчик XML данных в БД"""
from pathlib import Path
from lxml import etree
from erknm.db.connection import get_connection, get_cursor
from erknm.db.models import XmlFragment, OperationLog
from erknm.classifier.classifier import classify_xml_file


def load_xml_to_db(xml_fragment_id: int, sync_run_id=None) -> int:
    """
    Загрузить XML-фрагмент в БД
    
    Returns:
        Количество загруженных записей
    """
    conn = get_connection()
    cur = get_cursor(conn)
    
    try:
        # Получаем информацию о фрагменте
        cur.execute("""
            SELECT xf.id, xf.file_path, xf.data_type, za.id as zip_id
            FROM xml_fragments xf
            JOIN zip_archives za ON xf.zip_archive_id = za.id
            WHERE xf.id = %s
        """, (xml_fragment_id,))
        
        fragment = cur.fetchone()
        if not fragment:
            raise Exception(f"XML-фрагмент {xml_fragment_id} не найден")
        
        file_path = Path(fragment['file_path'])
        data_type = fragment['data_type']
        
        if not file_path.exists():
            raise Exception(f"Файл не найден: {file_path}")
        
        # Классифицируем, если еще не классифицирован
        if not data_type:
            data_type = classify_xml_file(file_path)
            if data_type:
                XmlFragment.update_status(xml_fragment_id, 'pending', data_type=data_type)
        
        # Парсим XML
        try:
            tree = etree.parse(str(file_path))
            root = tree.getroot()
        except Exception as e:
            raise Exception(f"Ошибка парсинга XML: {str(e)}")
        
        # Определяем тип данных и загружаем
        records_count = 0
        
        if data_type == 'plan':
            # Загружаем планы проверок
            plans = root.xpath('.//PLAN | .//plan')
            for plan in plans:
                plan_xml = etree.tostring(plan, encoding='unicode')
                cur.execute("""
                    INSERT INTO plans_raw (xml_fragment_id, xml_content)
                    VALUES (%s, %s::xml)
                """, (xml_fragment_id, plan_xml))
                records_count += 1
            
            XmlFragment.update_status(xml_fragment_id, 'loaded', records_count=records_count)
            
        elif data_type == 'inspection':
            # Загружаем проверки
            inspections = root.xpath('.//INSPECTION | .//inspection')
            for inspection in inspections:
                inspection_xml = etree.tostring(inspection, encoding='unicode')
                cur.execute("""
                    INSERT INTO inspections_raw (xml_fragment_id, xml_content)
                    VALUES (%s, %s::xml)
                """, (xml_fragment_id, inspection_xml))
                records_count += 1
            
            XmlFragment.update_status(xml_fragment_id, 'loaded', records_count=records_count)
            
        else:
            # Неклассифицированные данные - помечаем как ошибку
            XmlFragment.update_status(xml_fragment_id, 'error', 
                                     error_message='Неклассифицированные данные',
                                     data_type='unknown')
            if sync_run_id:
                OperationLog.log(sync_run_id, "xml_loader", 
                               f"Неклассифицированный файл: {file_path}", 
                               level="WARNING")
            return 0
        
        conn.commit()
        
        if sync_run_id:
            OperationLog.log(sync_run_id, "data", 
                           f"Загружено {records_count} записей из {file_path.name}", stage='data')
        
        return records_count
        
    except Exception as e:
        conn.rollback()
        error_msg = str(e)
        XmlFragment.update_status(xml_fragment_id, 'error', error_message=error_msg)
        if sync_run_id:
            OperationLog.log(sync_run_id, "data", 
                           f"Ошибка загрузки XML {xml_fragment_id}: {error_msg}", 
                           level="ERROR", stage='data')
        raise
    finally:
        cur.close()
        conn.close()





