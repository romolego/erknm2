"""Модели для работы с БД"""
from datetime import datetime
from erknm.db.connection import get_connection, get_cursor


class SyncRun:
    """Модель запуска синхронизации"""
    
    @staticmethod
    def create(is_manual=False):
        """Создать новый запуск синхронизации"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                INSERT INTO sync_runs (status, is_manual)
                VALUES ('running', %s)
                RETURNING id, started_at
            """, (is_manual,))
            result = cur.fetchone()
            conn.commit()
            return dict(result)
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def finish(run_id, status='completed', error_message=None, files_processed=0, records_loaded=0):
        """Завершить запуск синхронизации"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            # Если текущий статус stopping или был запрошен стоп, переводим в stopped
            cur.execute("SELECT status, stop_requested FROM sync_runs WHERE id = %s", (run_id,))
            result = cur.fetchone()
            if result:
                current_status = result['status']
                stop_requested = result['stop_requested']
                # Если статус stopping, всегда переводим в stopped
                if current_status == 'stopping':
                    status = 'stopped'
                    error_message = error_message or 'Остановлено пользователем'
                # Если был запрошен стоп, меняем статус на stopped вместо completed/paused
                elif stop_requested:
                    status = 'stopped'
                    error_message = error_message or 'Остановлено пользователем'
            
            cur.execute("""
                UPDATE sync_runs
                SET finished_at = CURRENT_TIMESTAMP,
                    status = %s,
                    error_message = %s,
                    files_processed = %s,
                    records_loaded = %s,
                    stop_requested = FALSE
                WHERE id = %s
            """, (status, error_message, files_processed, records_loaded, run_id))
            conn.commit()
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def set_status(run_id, status):
        """Установить статус запуска (для stopping и других промежуточных статусов)"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                UPDATE sync_runs
                SET status = %s
                WHERE id = %s
            """, (status, run_id))
            conn.commit()
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def request_stop(run_id):
        """Запросить остановку синхронизации (идемпотентно)"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            # Устанавливаем stop_requested для статусов 'running' или 'stopping' (идемпотентно)
            cur.execute("""
                UPDATE sync_runs
                SET stop_requested = TRUE
                WHERE id = %s AND status IN ('running', 'stopping')
            """, (run_id,))
            conn.commit()
            # Всегда возвращаем True если run существует (идемпотентность)
            # Проверяем существование run
            cur.execute("SELECT id FROM sync_runs WHERE id = %s", (run_id,))
            exists = cur.fetchone() is not None
            return exists
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def is_stop_requested(run_id):
        """Проверить, запрошена ли остановка"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                SELECT stop_requested FROM sync_runs WHERE id = %s
            """, (run_id,))
            result = cur.fetchone()
            return result['stop_requested'] if result else False
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def resume(run_id):
        """Возобновить приостановленную синхронизацию"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                UPDATE sync_runs
                SET status = 'running',
                    stop_requested = FALSE,
                    finished_at = NULL
                WHERE id = %s AND status = 'paused'
            """, (run_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def get_paused_run():
        """Получить последний приостановленный запуск"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                SELECT id, started_at, files_processed, records_loaded, is_manual
                FROM sync_runs
                WHERE status = 'paused'
                ORDER BY started_at DESC
                LIMIT 1
            """)
            result = cur.fetchone()
            return dict(result) if result else None
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def reconcile_stale_runs():
        """Исправить зависшие запуски (running/stopping без активного процесса)"""
        from datetime import datetime, timedelta
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            # Запуски в статусе running/stopping старше 1 часа считаем зависшими
            stale_threshold = datetime.now() - timedelta(hours=1)
            
            cur.execute("""
                UPDATE sync_runs
                SET status = CASE 
                    WHEN status = 'stopping' THEN 'stopped'
                    ELSE 'aborted'
                END,
                finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP),
                error_message = COALESCE(error_message, 
                    CASE 
                        WHEN status = 'stopping' THEN 'Запуск завис в состоянии остановки (reconcile)'
                        ELSE 'Запуск завис и был прерван (reconcile)'
                    END
                )
                WHERE status IN ('running', 'stopping')
                AND started_at < %s
                AND finished_at IS NULL
            """, (stale_threshold,))
            
            updated_count = cur.rowcount
            conn.commit()
            return updated_count
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def delete_run(run_id):
        """Удалить запуск и связанные данные (в транзакции)"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            # Получаем информацию о том, что будет удалено
            cur.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM zip_archives WHERE sync_run_id = %s) as archives_count,
                    (SELECT COUNT(*) FROM operation_log WHERE sync_run_id = %s) as logs_count,
                    (SELECT COUNT(*) FROM parsed_records pr
                     JOIN zip_archives za ON pr.zip_archive_id = za.id
                     WHERE za.sync_run_id = %s) as records_count
            """, (run_id, run_id, run_id))
            stats = cur.fetchone()
            
            archives_count = stats['archives_count'] if stats else 0
            logs_count = stats['logs_count'] if stats else 0
            records_count = stats['records_count'] if stats else 0
            
            # Удаляем в транзакции (каскадные удаления через ON DELETE CASCADE)
            # zip_archives имеет ON DELETE SET NULL, поэтому записи останутся, но sync_run_id станет NULL
            # operation_log имеет ON DELETE SET NULL
            # parsed_records связан через zip_archives, поэтому не удалится напрямую
            
            # Сначала обнуляем связи в zip_archives (чтобы сохранить данные, но отвязать от run)
            cur.execute("""
                UPDATE zip_archives
                SET sync_run_id = NULL
                WHERE sync_run_id = %s
            """, (run_id,))
            
            # Удаляем логи
            cur.execute("DELETE FROM operation_log WHERE sync_run_id = %s", (run_id,))
            
            # Удаляем сам запуск
            cur.execute("DELETE FROM sync_runs WHERE id = %s", (run_id,))
            
            conn.commit()
            
            return {
                'archives_affected': archives_count,
                'logs_deleted': logs_count,
                'records_affected': records_count
            }
        except Exception as e:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def get_run_stats(run_id):
        """Получить статистику запуска (агрегаты из связанных таблиц)"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            # Основная информация о запуске
            cur.execute("""
                SELECT 
                    id, started_at, finished_at, status, is_manual, error_message,
                    files_processed, records_loaded
                FROM sync_runs
                WHERE id = %s
            """, (run_id,))
            run = cur.fetchone()
            
            if not run:
                return None
            
            # Подсчитываем реальные значения из связанных таблиц
            cur.execute("""
                SELECT COUNT(*) as count
                FROM zip_archives
                WHERE sync_run_id = %s
            """, (run_id,))
            files_count = cur.fetchone()['count']
            
            cur.execute("""
                SELECT COUNT(*) as count
                FROM parsed_records pr
                JOIN zip_archives za ON pr.zip_archive_id = za.id
                WHERE za.sync_run_id = %s
            """, (run_id,))
            records_count = cur.fetchone()['count']
            
            cur.execute("""
                SELECT COUNT(*) as count
                FROM operation_log
                WHERE sync_run_id = %s AND level = 'ERROR'
            """, (run_id,))
            errors_count = cur.fetchone()['count']
            
            cur.execute("""
                SELECT COUNT(*) as count
                FROM operation_log
                WHERE sync_run_id = %s
            """, (run_id,))
            logs_count = cur.fetchone()['count']
            
            return {
                'id': run['id'],
                'started_at': run['started_at'],
                'finished_at': run['finished_at'],
                'status': run['status'],
                'is_manual': run['is_manual'],
                'error_message': run['error_message'],
                'files_count': files_count,
                'records_count': records_count,
                'errors_count': errors_count,
                'logs_count': logs_count,
                'files_processed': run['files_processed'] or 0,
                'records_loaded': run['records_loaded'] or 0,
                'duration_seconds': (
                    (run['finished_at'] - run['started_at']).total_seconds() 
                    if run['finished_at'] and run['started_at'] else None
                )
            }
        finally:
            cur.close()
            conn.close()


class Dataset:
    """Модель набора данных"""
    
    @staticmethod
    def get_or_create(identifier, title, link, data_type=None):
        """Получить или создать набор данных"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                INSERT INTO datasets (identifier, title, link, data_type)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (identifier) 
                DO UPDATE SET 
                    title = EXCLUDED.title,
                    link = EXCLUDED.link,
                    data_type = COALESCE(EXCLUDED.data_type, datasets.data_type),
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id, identifier, title, link, data_type
            """, (identifier, title, link, data_type))
            result = cur.fetchone()
            conn.commit()
            return dict(result)
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def update_type(dataset_id, data_type):
        """Обновить тип набора данных"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                UPDATE datasets
                SET data_type = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (data_type, dataset_id))
            conn.commit()
        finally:
            cur.close()
            conn.close()


class DatasetVersion:
    """Модель версии набора данных"""
    
    @staticmethod
    def create(dataset_id, source_url, created_date, provenance, structure_version):
        """Создать версию набора данных"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                INSERT INTO dataset_versions (dataset_id, source_url, created_date, provenance, structure_version)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (dataset_id, source_url, created_date, provenance, structure_version))
            result = cur.fetchone()
            conn.commit()
            return dict(result) if result else None
        finally:
            cur.close()
            conn.close()


class ZipArchive:
    """Модель ZIP-архива"""
    
    @staticmethod
    def exists(url, sha256_hash=None):
        """Проверить существование архива"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            if sha256_hash:
                cur.execute("""
                    SELECT id FROM zip_archives
                    WHERE url = %s OR sha256_hash = %s
                """, (url, sha256_hash))
            else:
                cur.execute("""
                    SELECT id FROM zip_archives
                    WHERE url = %s
                """, (url,))
            result = cur.fetchone()
            return dict(result) if result else None
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def create(url, file_path=None, file_size=None, sha256_hash=None, status='pending', sync_run_id=None):
        """Создать запись об архиве"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                INSERT INTO zip_archives (url, file_path, file_size, sha256_hash, status, sync_run_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                RETURNING id, url, status
            """, (url, file_path, file_size, sha256_hash, status, sync_run_id))
            result = cur.fetchone()
            conn.commit()
            return dict(result) if result else None
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def update_status(archive_id, status, file_path=None, file_size=None, sha256_hash=None, error_message=None):
        """Обновить статус архива"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            update_fields = ["status = %s"]
            values = [status]
            
            if file_path:
                update_fields.append("file_path = %s")
                values.append(file_path)
            if file_size:
                update_fields.append("file_size = %s")
                values.append(file_size)
            if sha256_hash:
                update_fields.append("sha256_hash = %s")
                values.append(sha256_hash)
            if error_message:
                update_fields.append("error_message = %s")
                values.append(error_message)
            
            if status == 'downloaded':
                update_fields.append("downloaded_at = CURRENT_TIMESTAMP")
            elif status == 'processed':
                update_fields.append("processed_at = CURRENT_TIMESTAMP")
            
            values.append(archive_id)
            
            cur.execute(f"""
                UPDATE zip_archives
                SET {', '.join(update_fields)}
                WHERE id = %s
            """, values)
            conn.commit()
        finally:
            cur.close()
            conn.close()


class XmlFragment:
    """Модель XML-фрагмента"""
    
    @staticmethod
    def create(zip_archive_id, file_name, file_path=None, data_type=None, status='pending'):
        """Создать запись о XML-фрагменте"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                INSERT INTO xml_fragments (zip_archive_id, file_name, file_path, data_type, status)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (zip_archive_id, file_name, file_path, data_type, status))
            result = cur.fetchone()
            conn.commit()
            return dict(result)
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def update_status(fragment_id, status, records_count=None, error_message=None, data_type=None):
        """Обновить статус фрагмента"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            update_fields = ["status = %s"]
            values = [status]
            
            if records_count is not None:
                update_fields.append("records_count = %s")
                values.append(records_count)
            if error_message:
                update_fields.append("error_message = %s")
                values.append(error_message)
            if data_type:
                update_fields.append("data_type = %s")
                values.append(data_type)
            
            if status in ('parsed', 'loaded'):
                update_fields.append("processed_at = CURRENT_TIMESTAMP")
            
            values.append(fragment_id)
            
            cur.execute(f"""
                UPDATE xml_fragments
                SET {', '.join(update_fields)}
                WHERE id = %s
            """, values)
            conn.commit()
        finally:
            cur.close()
            conn.close()


class OperationLog:
    """Модель журнала операций"""
    
    # Кэш для проверки наличия столбца stage (чтобы не проверять каждый раз)
    _has_stage_column = None
    
    @staticmethod
    def _check_stage_column(cur):
        """Проверить наличие столбца stage (кэшируем результат)"""
        if OperationLog._has_stage_column is None:
            try:
                cur.execute("""
                    SELECT COUNT(*) as cnt 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'operation_log' 
                    AND column_name = 'stage'
                """)
                OperationLog._has_stage_column = cur.fetchone()['cnt'] > 0
            except:
                OperationLog._has_stage_column = False
        return OperationLog._has_stage_column
    
    @staticmethod
    def log(sync_run_id, operation_type, message, level='INFO', stage='general'):
        """
        Записать в журнал
        
        Args:
            sync_run_id: ID запуска синхронизации
            operation_type: Тип операции (sync, browser, meta, zip, xml_loader и т.д.)
            message: Текст сообщения
            level: Уровень (INFO, WARNING, ERROR)
            stage: Этап работы ('general', 'list', 'dataset', 'data')
        """
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            # Проверяем наличие столбца stage для обратной совместимости
            has_stage = OperationLog._check_stage_column(cur)
            
            if has_stage:
                cur.execute("""
                    INSERT INTO operation_log (sync_run_id, operation_type, message, level, stage)
                    VALUES (%s, %s, %s, %s, %s)
                """, (sync_run_id, operation_type, message, level, stage))
            else:
                # Если столбца нет, вставляем без stage
                cur.execute("""
                    INSERT INTO operation_log (sync_run_id, operation_type, message, level)
                    VALUES (%s, %s, %s, %s)
                """, (sync_run_id, operation_type, message, level))
            conn.commit()
        except Exception as e:
            # Если произошла ошибка, пробуем без stage (для обратной совместимости)
            try:
                conn.rollback()
                cur.execute("""
                    INSERT INTO operation_log (sync_run_id, operation_type, message, level)
                    VALUES (%s, %s, %s, %s)
                """, (sync_run_id, operation_type, message, level))
                conn.commit()
                # Сбрасываем кэш, чтобы проверить при следующем вызове
                OperationLog._has_stage_column = None
            except:
                # Если и это не сработало, просто игнорируем ошибку логирования
                # (чтобы сбой логирования не убивал синхронизацию)
                conn.rollback()
                pass
        finally:
            cur.close()
            conn.close()


class Settings:
    """Модель настроек робота"""
    
    @staticmethod
    def get(key, default=None):
        """Получить значение настройки"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                SELECT value FROM robot_settings WHERE key = %s
            """, (key,))
            result = cur.fetchone()
            return result['value'] if result and result['value'] else default
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def set(key, value):
        """Установить значение настройки"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                INSERT INTO robot_settings (key, value, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (key) 
                DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
            """, (key, str(value) if value is not None else None))
            conn.commit()
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def get_all():
        """Получить все настройки"""
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                SELECT key, value, updated_at FROM robot_settings ORDER BY key
            """)
            results = cur.fetchall()
            return {row['key']: row['value'] for row in results}
        finally:
            cur.close()
            conn.close()
    
    @staticmethod
    def set_defaults():
        """Установить настройки по умолчанию (если их еще нет)"""
        defaults = {
            'schedule_enabled': 'true',
            'schedule_mode': 'daily',
            'schedule_time': '02:00',
            'schedule_day_of_week': '1',
            'schedule_day_of_month': '1',
            'on_error': 'pause',
            'retry_policy': 'fixed',
            'retry_count': '3',
            'retry_delay_seconds': '60',
            'throttle_seconds': '10',
            'process_only_zip': 'true',
            'unknown_policy': 'skip',
            'operational_log_enabled': 'true',
            'sync_order': 'old_to_new',  # Порядок обработки: 'old_to_new' или 'new_to_old'
            'stop_on_repeats_enabled': 'false',  # Остановка на повторах: 'true' или 'false'
            'stop_on_repeats_count': '3'  # Количество подряд идущих повторов для остановки
        }
        
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            for key, value in defaults.items():
                cur.execute("""
                    INSERT INTO robot_settings (key, value, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (key) DO NOTHING
                """, (key, value))
            conn.commit()
        finally:
            cur.close()
            conn.close()


