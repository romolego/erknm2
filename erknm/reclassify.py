"""Модуль переклассификации данных"""
from erknm.db.connection import get_connection, get_cursor
from erknm.db.models import Dataset, XmlFragment, OperationLog
from erknm.loader.xml_loader import load_xml_to_db


def reclassify_dataset(dataset_id: int, new_data_type: str, sync_run_id=None):
    """
    Переклассифицировать набор данных и перезагрузить его XML-фрагменты
    
    Args:
        dataset_id: ID набора данных
        new_data_type: Новый тип ('plan' или 'inspection')
    """
    if new_data_type not in ('plan', 'inspection'):
        raise ValueError("Тип данных должен быть 'plan' или 'inspection'")
    
    conn = get_connection()
    cur = get_cursor(conn)
    
    try:
        # Обновляем тип набора данных
        Dataset.update_type(dataset_id, new_data_type)
        
        if sync_run_id:
            OperationLog.log(sync_run_id, "reclassify", 
                           f"Набор данных {dataset_id} переклассифицирован как {new_data_type}")
        
        # Находим все необработанные XML-фрагменты этого набора
        # (через zip_archives -> dataset_versions)
        cur.execute("""
            SELECT DISTINCT xf.id, xf.file_path
            FROM xml_fragments xf
            JOIN zip_archives za ON xf.zip_archive_id = za.id
            JOIN dataset_versions dv ON za.url = dv.source_url
            WHERE dv.dataset_id = %s
            AND (xf.status = 'error' OR xf.data_type != %s)
        """, (dataset_id, new_data_type))
        
        fragments = cur.fetchall()
        
        records_loaded = 0
        
        for fragment in fragments:
            fragment_id = fragment['id']
            
            # Удаляем старые записи, если они были загружены
            cur.execute("""
                DELETE FROM plans_raw WHERE xml_fragment_id = %s
            """, (fragment_id,))
            cur.execute("""
                DELETE FROM inspections_raw WHERE xml_fragment_id = %s
            """, (fragment_id,))
            conn.commit()
            
            # Обновляем тип фрагмента
            XmlFragment.update_status(fragment_id, 'pending', data_type=new_data_type)
            
            # Перезагружаем XML
            try:
                records = load_xml_to_db(fragment_id, sync_run_id)
                records_loaded += records
            except Exception as e:
                if sync_run_id:
                    OperationLog.log(sync_run_id, "reclassify", 
                                   f"Ошибка перезагрузки фрагмента {fragment_id}: {str(e)}", 
                                   level="ERROR")
        
        if sync_run_id:
            OperationLog.log(sync_run_id, "reclassify", 
                           f"Переклассификация завершена. Загружено записей: {records_loaded}")
        
        return records_loaded
        
    finally:
        cur.close()
        conn.close()


def reclassify_xml_fragment(fragment_id: int, new_data_type: str, sync_run_id=None):
    """
    Переклассифицировать отдельный XML-фрагмент
    
    Args:
        fragment_id: ID XML-фрагмента
        new_data_type: Новый тип ('plan' или 'inspection')
    """
    if new_data_type not in ('plan', 'inspection'):
        raise ValueError("Тип данных должен быть 'plan' или 'inspection'")
    
    conn = get_connection()
    cur = get_cursor(conn)
    
    try:
        # Удаляем старые записи
        cur.execute("""
            DELETE FROM plans_raw WHERE xml_fragment_id = %s
        """, (fragment_id,))
        cur.execute("""
            DELETE FROM inspections_raw WHERE xml_fragment_id = %s
        """, (fragment_id,))
        conn.commit()
        
        # Обновляем тип и статус
        XmlFragment.update_status(fragment_id, 'pending', data_type=new_data_type)
        
        if sync_run_id:
            OperationLog.log(sync_run_id, "reclassify", 
                           f"XML-фрагмент {fragment_id} переклассифицирован как {new_data_type}")
        
        # Перезагружаем XML
        try:
            records = load_xml_to_db(fragment_id, sync_run_id)
            return records
        except Exception as e:
            if sync_run_id:
                OperationLog.log(sync_run_id, "reclassify", 
                               f"Ошибка перезагрузки фрагмента {fragment_id}: {str(e)}", 
                               level="ERROR")
            raise
    finally:
        cur.close()
        conn.close()

