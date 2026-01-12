"""Схема базы данных"""
from erknm.db.connection import get_connection, get_cursor


def init_schema():
    """Инициализировать схему БД"""
    conn = get_connection()
    
    # Устанавливаем кодировку UTF-8 для соединения
    try:
        conn.set_client_encoding('UTF8')
    except:
        pass
    
    cur = get_cursor(conn)
    
    try:
        # Устанавливаем кодировку через SQL
        try:
            cur.execute("SET client_encoding TO 'UTF8'")
        except:
            pass
        
        # Проверяем, не инициализирована ли уже БД
        cur.execute("""
            SELECT COUNT(*) as cnt 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('sync_runs', 'datasets', 'zip_archives', 'xml_fragments', 
                               'plans_raw', 'inspections_raw', 'operation_log', 'dataset_versions', 
                               'robot_settings', 'parsed_records')
        """)
        existing_tables = cur.fetchone()['cnt']
        
        if existing_tables >= 10:
            # БД уже инициализирована, но нужно проверить и добавить недостающие колонки
            # Добавляем колонку stop_requested если её нет
            try:
                cur.execute("ALTER TABLE sync_runs ADD COLUMN IF NOT EXISTS stop_requested BOOLEAN NOT NULL DEFAULT FALSE")
                conn.commit()
            except Exception as e:
                # Игнорируем ошибки, если колонка уже существует или другая проблема
                conn.rollback()
                pass
            
            # Миграция: добавляем sync_run_id в zip_archives если отсутствует
            try:
                # Проверяем наличие колонки
                cur.execute("""
                    SELECT COUNT(*) as cnt 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'zip_archives' 
                    AND column_name = 'sync_run_id'
                """)
                col_exists = cur.fetchone()['cnt'] > 0
                
                if not col_exists:
                    # Добавляем колонку без FK
                    cur.execute("ALTER TABLE zip_archives ADD COLUMN sync_run_id INTEGER")
                    conn.commit()
                    
                    # Пытаемся добавить FK constraint (может не получиться если есть данные с несуществующими run_id)
                    try:
                        cur.execute("""
                            ALTER TABLE zip_archives 
                            ADD CONSTRAINT zip_archives_sync_run_id_fkey 
                            FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL
                        """)
                        conn.commit()
                    except:
                        conn.rollback()
                        # Если не получилось добавить FK (например, из-за существующих данных), продолжаем без него
                        pass
                    
                    # Добавляем индекс для производительности
                    try:
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_zip_archives_sync_run_id ON zip_archives(sync_run_id)")
                        conn.commit()
                    except:
                        conn.rollback()
                        pass
            except Exception as e:
                conn.rollback()
                # Игнорируем ошибки миграции
                pass
            
            # Миграция: добавляем поле stage в operation_log если оно отсутствует
            try:
                cur.execute("""
                    ALTER TABLE operation_log 
                    ADD COLUMN IF NOT EXISTS stage VARCHAR(20) DEFAULT 'general'
                """)
                conn.commit()
                
                # Бэкфил существующих записей без stage
                cur.execute("""
                    UPDATE operation_log 
                    SET stage = 'general' 
                    WHERE stage IS NULL
                """)
                conn.commit()
                
                # Добавляем индекс по (stage, created_at) для быстрых выборок
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_operation_log_stage_created 
                    ON operation_log(stage, created_at DESC)
                """)
                conn.commit()
            except Exception as e:
                conn.rollback()
                # Игнорируем ошибки миграции
                pass
            
            return True
        
        # Выдаем права на схему public (если нужно)
        try:
            cur.execute("GRANT ALL ON SCHEMA public TO CURRENT_USER")
            cur.execute("GRANT CREATE ON SCHEMA public TO CURRENT_USER")
        except:
            pass  # Игнорируем, если права уже есть или нет доступа
        
        # Таблица запусков синхронизации
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_runs (
                id SERIAL PRIMARY KEY,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP,
                status VARCHAR(50) NOT NULL,
                is_manual BOOLEAN NOT NULL DEFAULT FALSE,
                error_message TEXT,
                files_processed INTEGER DEFAULT 0,
                records_loaded INTEGER DEFAULT 0,
                stop_requested BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)
        
        # Добавляем колонку stop_requested если таблица уже существует
        try:
            cur.execute("ALTER TABLE sync_runs ADD COLUMN IF NOT EXISTS stop_requested BOOLEAN NOT NULL DEFAULT FALSE")
        except:
            pass  # Колонка уже существует
        
        # Миграция: обновляем счетчики для существующих запусков (backfill)
        try:
            # Обновляем files_processed из zip_archives
            cur.execute("""
                UPDATE sync_runs sr
                SET files_processed = COALESCE((
                    SELECT COUNT(*) 
                    FROM zip_archives za 
                    WHERE za.sync_run_id = sr.id
                ), 0)
                WHERE files_processed = 0 OR files_processed IS NULL
            """)
            
            # Обновляем records_loaded из parsed_records через zip_archives
            cur.execute("""
                UPDATE sync_runs sr
                SET records_loaded = COALESCE((
                    SELECT COUNT(*) 
                    FROM parsed_records pr
                    JOIN zip_archives za ON pr.zip_archive_id = za.id
                    WHERE za.sync_run_id = sr.id
                ), 0)
                WHERE records_loaded = 0 OR records_loaded IS NULL
            """)
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            # Игнорируем ошибки миграции (может быть таблицы еще не созданы)
            pass
        
        # Таблица наборов данных
        cur.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id SERIAL PRIMARY KEY,
                identifier VARCHAR(255) UNIQUE NOT NULL,
                title TEXT,
                link TEXT NOT NULL,
                data_type VARCHAR(50), -- 'plan', 'inspection', 'unknown'
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица версий наборов данных
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dataset_versions (
                id SERIAL PRIMARY KEY,
                dataset_id INTEGER REFERENCES datasets(id) ON DELETE CASCADE,
                source_url TEXT NOT NULL,
                created_date VARCHAR(50),
                provenance TEXT,
                structure_version VARCHAR(50),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица ZIP-архивов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS zip_archives (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                file_path TEXT,
                file_size BIGINT,
                sha256_hash VARCHAR(64),
                status VARCHAR(50) NOT NULL, -- 'pending', 'downloaded', 'processed', 'error'
                error_message TEXT,
                downloaded_at TIMESTAMP,
                processed_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                sync_run_id INTEGER REFERENCES sync_runs(id) ON DELETE SET NULL
            )
        """)
        
        # Таблица XML-фрагментов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS xml_fragments (
                id SERIAL PRIMARY KEY,
                zip_archive_id INTEGER REFERENCES zip_archives(id) ON DELETE CASCADE,
                file_name VARCHAR(255) NOT NULL,
                file_path TEXT,
                data_type VARCHAR(50), -- 'plan', 'inspection', 'unknown'
                status VARCHAR(50) NOT NULL, -- 'pending', 'parsed', 'loaded', 'error'
                error_message TEXT,
                records_count INTEGER DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP
            )
        """)
        
        # Таблица планов проверок (сырой XML)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS plans_raw (
                id SERIAL PRIMARY KEY,
                xml_fragment_id INTEGER REFERENCES xml_fragments(id) ON DELETE CASCADE,
                xml_content XML NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица проверок (сырой XML)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inspections_raw (
                id SERIAL PRIMARY KEY,
                xml_fragment_id INTEGER REFERENCES xml_fragments(id) ON DELETE CASCADE,
                xml_content XML NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица журнала операций
        cur.execute("""
            CREATE TABLE IF NOT EXISTS operation_log (
                id SERIAL PRIMARY KEY,
                sync_run_id INTEGER REFERENCES sync_runs(id) ON DELETE SET NULL,
                operation_type VARCHAR(100) NOT NULL,
                message TEXT NOT NULL,
                level VARCHAR(20) NOT NULL, -- 'INFO', 'WARNING', 'ERROR'
                stage VARCHAR(20) DEFAULT 'general', -- 'general', 'list', 'dataset', 'data'
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Миграция: добавляем поле stage если оно отсутствует (идемпотентно)
        try:
            cur.execute("""
                ALTER TABLE operation_log 
                ADD COLUMN IF NOT EXISTS stage VARCHAR(20) DEFAULT 'general'
            """)
            conn.commit()
            
            # Бэкфил существующих записей без stage (устанавливаем 'general')
            cur.execute("""
                UPDATE operation_log 
                SET stage = 'general' 
                WHERE stage IS NULL
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            # Игнорируем ошибки миграции (колонка может уже существовать)
            pass
        
        # Добавляем индекс по (stage, created_at) для быстрых выборок
        try:
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_operation_log_stage_created 
                ON operation_log(stage, created_at DESC)
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            pass
        
        # Таблица настроек робота
        cur.execute("""
            CREATE TABLE IF NOT EXISTS robot_settings (
                id SERIAL PRIMARY KEY,
                key VARCHAR(100) UNIQUE NOT NULL,
                value TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица распознанных данных (витрина результата парсинга)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS parsed_records (
                id SERIAL PRIMARY KEY,
                zip_archive_id INTEGER REFERENCES zip_archives(id) ON DELETE CASCADE,
                xml_fragment_id INTEGER REFERENCES xml_fragments(id) ON DELETE SET NULL,
                record_type VARCHAR(50) NOT NULL, -- 'plan', 'inspection'
                record_key VARCHAR(255), -- GUID/номер записи если извлекается
                record_date DATE, -- Дата записи если извлекается
                payload_json JSONB, -- Облегченный JSON с важными полями
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Индексы
        cur.execute("CREATE INDEX IF NOT EXISTS idx_datasets_identifier ON datasets(identifier)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_zip_archives_url ON zip_archives(url)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_zip_archives_hash ON zip_archives(sha256_hash)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_zip_archives_status ON zip_archives(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_zip_archives_sync_run_id ON zip_archives(sync_run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_xml_fragments_zip ON xml_fragments(zip_archive_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_xml_fragments_status ON xml_fragments(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_operation_log_run ON operation_log(sync_run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_operation_log_created ON operation_log(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_operation_log_level ON operation_log(level)")  # Для быстрого поиска ошибок
        
        # Индексы для parsed_records
        cur.execute("CREATE INDEX IF NOT EXISTS idx_parsed_records_archive ON parsed_records(zip_archive_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_parsed_records_type ON parsed_records(record_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_parsed_records_date ON parsed_records(record_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_parsed_records_created ON parsed_records(created_at)")
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        error_msg = str(e)
        error_type = type(e).__name__
        
        # Проверяем на ошибку прав доступа
        if 'permission' in error_msg.lower() or 'privilege' in error_msg.lower() or 'права' in error_msg.lower() or 'InsufficientPrivilege' in error_type:
            raise Exception(
                "Ошибка прав доступа к базе данных. "
                "Выполните в PostgreSQL:\n"
                f"GRANT ALL PRIVILEGES ON DATABASE erknm TO erknm_user;\n"
                f"GRANT ALL ON SCHEMA public TO erknm_user;\n"
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO erknm_user;"
            )
        
        # Обработка ошибок кодировки - пробуем переподключиться
        if 'codec' in error_msg.lower() or 'encoding' in error_msg.lower() or 'utf-8' in error_msg.lower():
            try:
                # Закрываем текущее соединение
                cur.close()
                conn.close()
                
                # Создаем новое соединение с явной установкой кодировки
                conn = get_connection()
                # Пробуем установить кодировку через переменную окружения
                import os
                os.environ['PGCLIENTENCODING'] = 'UTF8'
                
                cur = conn.cursor()
                cur.execute("SET client_encoding TO 'UTF8'")
                
                # Повторяем создание таблиц
                # (код создания таблиц здесь не дублируем, просто возвращаем успех если таблицы уже есть)
                cur.execute("""
                    SELECT COUNT(*) as cnt 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                result = cur.fetchone()
                if result and result[0] >= 8:
                    conn.commit()
                    cur.close()
                    conn.close()
                    return True
                
                raise Exception(f"Ошибка кодировки при инициализации. Попробуйте установить переменную окружения PGCLIENTENCODING=UTF8")
            except Exception as e2:
                raise Exception(f"Ошибка кодировки: {error_msg}. Дополнительная ошибка: {str(e2)}")
        
        raise Exception(f"Ошибка инициализации схемы: {error_msg}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    init_schema()


