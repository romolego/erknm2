"""Веб-приложение Flask для управления роботом"""
from flask import Flask, render_template, jsonify, request, redirect, url_for
import threading
from pathlib import Path
import os
from erknm.db.connection import get_connection, get_cursor
from erknm.db.schema import init_schema
from erknm.db.models import SyncRun, OperationLog, Settings, ZipArchive, XmlFragment
from erknm.sync.synchronizer import sync, process_manual_file

# Определяем путь к шаблонам относительно этого файла
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=template_dir)
app.config['SECRET_KEY'] = 'erknm-secret-key-change-in-production'
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max file size

# Глобальная переменная для отслеживания запущенной синхронизации
sync_thread = None
sync_status = {
    'state': 'idle',  # idle, running, stopping, stopped, error
    'running': False,
    'message': '',
    'current_operation': '',
    'current_file': '',
    'progress': None  # {'files_processed': 0, 'records_loaded': 0, 'current_step': ''}
}


@app.route('/')
def index():
    """Главная страница"""
    return render_template('index.html')


@app.route('/favicon.ico')
def favicon():
    """Обработка favicon - возвращаем 204 No Content чтобы не было 404"""
    return '', 204


@app.route('/api/status')
def api_status():
    """Статус системы"""
    # Выполняем reconcile перед проверкой статуса
    SyncRun.reconcile_stale_runs()
    
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Ошибка подключения к БД: {str(e)}'
        }), 500
    
    try:
        # Статистика по запускам
        cur.execute("""
            SELECT 
                COUNT(*) as total_runs,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'running') as running,
                COUNT(*) FILTER (WHERE status = 'error') as errors,
                SUM(files_processed) as total_files,
                SUM(records_loaded) as total_records
            FROM sync_runs
        """)
        stats = cur.fetchone()
        
        # Последний запуск
        cur.execute("""
            SELECT id, started_at, finished_at, status, is_manual,
                   files_processed, records_loaded, error_message
            FROM sync_runs
            ORDER BY started_at DESC
            LIMIT 1
        """)
        last_run = cur.fetchone()
        
        # Статистика по данным (с проверкой существования таблиц)
        try:
            cur.execute("SELECT COUNT(*) FROM datasets")
            datasets_count = cur.fetchone()['count']
        except:
            datasets_count = 0
        
        try:
            cur.execute("SELECT COUNT(*) FROM zip_archives WHERE status = 'processed'")
            processed_zips = cur.fetchone()['count']
        except:
            processed_zips = 0
        
        try:
            cur.execute("SELECT COUNT(*) FROM xml_fragments WHERE status = 'loaded'")
            loaded_fragments = cur.fetchone()['count']
        except:
            loaded_fragments = 0
        
        try:
            cur.execute("SELECT COUNT(*) FROM plans_raw")
            plans_count = cur.fetchone()['count']
        except:
            plans_count = 0
        
        try:
            cur.execute("SELECT COUNT(*) FROM inspections_raw")
            inspections_count = cur.fetchone()['count']
        except:
            inspections_count = 0
        
        # Проверяем, есть ли приостановленная синхронизация
        paused_run = SyncRun.get_paused_run()
        
        # Проверяем наличие реально выполняющихся запусков в БД
        cur.execute("""
            SELECT id, status FROM sync_runs 
            WHERE status IN ('running', 'stopping')
            ORDER BY started_at DESC
            LIMIT 1
        """)
        db_running_run = cur.fetchone()
        
        # Синхронизация считается запущенной, если:
        # 1. Глобальная переменная sync_status['running'] = True, ИЛИ
        # 2. В БД есть запуск со статусом 'running' или 'stopping'
        is_running = sync_status['running'] or (db_running_run is not None)
        
        # Если глобальная переменная не соответствует БД, синхронизируем
        if db_running_run and not sync_status['running']:
            # В БД есть запуск, но глобальная переменная говорит что нет - обновляем
            sync_status['running'] = True
            sync_status['state'] = 'running'
            if db_running_run['status'] == 'stopping':
                sync_status['state'] = 'stopping'
        elif not db_running_run and sync_status['running']:
            # Глобальная переменная говорит что запущено, но в БД нет - сбрасываем
            sync_status['running'] = False
            sync_status['state'] = 'idle'
        
        return jsonify({
            'success': True,
            'sync_running': is_running,
            'sync_message': sync_status['message'],
            'sync_paused': paused_run is not None,
            'paused_run': dict(paused_run) if paused_run else None,
            'stats': {
                'total_runs': stats['total_runs'] or 0,
                'completed': stats['completed'] or 0,
                'running': stats['running'] or 0,
                'errors': stats['errors'] or 0,
                'total_files': stats['total_files'] or 0,
                'total_records': stats['total_records'] or 0,
                'datasets': datasets_count,
                'processed_zips': processed_zips,
                'loaded_fragments': loaded_fragments,
                'plans': plans_count,
                'inspections': inspections_count
            },
            'last_run': dict(last_run) if last_run else None
        })
    finally:
        cur.close()
        conn.close()


@app.route('/api/runs')
def api_runs():
    """Список запусков с корректировкой статуса на основе runtime status"""
    # Выполняем reconcile перед загрузкой списка
    SyncRun.reconcile_stale_runs()
    
    limit = request.args.get('limit', 20, type=int)
    
    # Получаем реальный статус из runtime status (источник истины)
    # Используем прямой доступ к sync_status и БД вместо HTTP запроса
    active_run_id = None
    active_state = 'idle'
    is_running = sync_status['running']
    
    if is_running:
        active_state = sync_status['state']
        # Получаем active_run_id из БД
        try:
            conn_temp = get_connection()
            cur_temp = get_cursor(conn_temp)
            try:
                cur_temp.execute("""
                    SELECT id FROM sync_runs 
                    WHERE status IN ('running', 'stopping')
                    ORDER BY started_at DESC 
                    LIMIT 1
                """)
                current_run = cur_temp.fetchone()
                if current_run:
                    active_run_id = current_run['id']
            finally:
                cur_temp.close()
                conn_temp.close()
        except:
            pass
    
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Ошибка подключения к БД: {str(e)}'
        }), 500
    
    try:
        # Подсчитываем реальные значения из связанных таблиц
        cur.execute("""
            SELECT 
                sr.id,
                sr.started_at,
                sr.finished_at,
                sr.status,
                sr.is_manual,
                sr.error_message,
                COALESCE((
                    SELECT COUNT(*) 
                    FROM zip_archives za 
                    WHERE za.sync_run_id = sr.id
                ), 0) as files_count,
                COALESCE((
                    SELECT COUNT(*) 
                    FROM parsed_records pr
                    JOIN zip_archives za ON pr.zip_archive_id = za.id
                    WHERE za.sync_run_id = sr.id
                ), 0) as records_count,
                COALESCE((
                    SELECT COUNT(*) 
                    FROM operation_log ol
                    WHERE ol.sync_run_id = sr.id AND ol.level = 'ERROR'
                ), 0) as errors_count,
                sr.files_processed,
                sr.records_loaded
            FROM sync_runs sr
            ORDER BY sr.started_at DESC
            LIMIT %s
        """, (limit,))
        
        runs = cur.fetchall()
        result = []
        for run in runs:
            # Корректируем статус: если это активный run и воркер работает,
            # показываем реальный статус из runtime, а не из БД
            display_status = run['status']
            if active_run_id and run['id'] == active_run_id and is_running:
                # Воркер реально работает - показываем реальный статус
                if active_state == 'running':
                    display_status = 'running'
                elif active_state == 'stopping':
                    display_status = 'stopping'
            
            result.append({
                'id': run['id'],
                'started_at': run['started_at'].isoformat() if run['started_at'] else None,
                'finished_at': run['finished_at'].isoformat() if run['finished_at'] else None,
                'status': display_status,  # Используем скорректированный статус
                'is_manual': run['is_manual'],
                'error_message': run['error_message'],
                'files_count': run['files_count'] or 0,
                'records_count': run['records_count'] or 0,
                'errors_count': run['errors_count'] or 0,
                'files_processed': run['files_processed'] or 0,
                'records_loaded': run['records_loaded'] or 0
            })
        
        return jsonify({
            'success': True,
            'runs': result
        })
    finally:
        cur.close()
        conn.close()


@app.route('/api/logs')
def api_logs():
    """Журнал операций"""
    limit = request.args.get('limit', 100, type=int)
    level = request.args.get('level', 'ALL')
    stage = request.args.get('stage', 'all')  # 'all', 'general', 'list', 'dataset', 'data'
    
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Ошибка подключения к БД: {str(e)}'
        }), 500
    
    try:
        # Проверяем наличие столбца stage для обратной совместимости
        cur.execute("""
            SELECT COUNT(*) as cnt 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'operation_log' 
            AND column_name = 'stage'
        """)
        has_stage_column = cur.fetchone()['cnt'] > 0
        
        # Формируем SELECT с учетом наличия столбца stage
        if has_stage_column:
            query = """
                SELECT ol.created_at, ol.operation_type, ol.message, ol.level, ol.stage, sr.id as run_id
                FROM operation_log ol
                LEFT JOIN sync_runs sr ON ol.sync_run_id = sr.id
                WHERE 1=1
            """
        else:
            # Если столбца нет, используем NULL как значение по умолчанию
            query = """
                SELECT ol.created_at, ol.operation_type, ol.message, ol.level, NULL as stage, sr.id as run_id
                FROM operation_log ol
                LEFT JOIN sync_runs sr ON ol.sync_run_id = sr.id
                WHERE 1=1
            """
        
        params = []
        
        if level != 'ALL':
            query += " AND ol.level = %s"
            params.append(level)
        
        # Фильтрация по stage/operation_type
        # Для вкладок "Наборы данных" и "Данные" фильтруем по operation_type
        # Для остальных вкладок - по stage
        if stage != 'all' and has_stage_column:
            if stage == 'general':
                # general: показываем записи с stage='general' или NULL
                query += " AND (ol.stage = 'general' OR ol.stage IS NULL)"
            elif stage == 'dataset':
                # Наборы данных: фильтруем по operation_type='dataset'
                query += " AND ol.operation_type = 'dataset'"
            elif stage == 'data':
                # Данные: фильтруем по operation_type='data'
                query += " AND ol.operation_type = 'data'"
            else:
                # Для других стадий (list) используем stage
                query += " AND ol.stage = %s"
                params.append(stage)
        elif stage != 'all' and not has_stage_column:
            # Если запрошен фильтр по stage, но столбца нет
            if stage == 'general':
                # Для general показываем все (так как старые записи считаются general)
                pass
            elif stage == 'dataset':
                # Наборы данных: фильтруем по operation_type='dataset'
                query += " AND ol.operation_type = 'dataset'"
            elif stage == 'data':
                # Данные: фильтруем по operation_type='data'
                query += " AND ol.operation_type = 'data'"
            else:
                # Для других стадий возвращаем пустой результат
                query += " AND 1=0"  # Ничего не возвращаем
        
        query += " ORDER BY ol.created_at DESC LIMIT %s"
        params.append(limit)
        
        cur.execute(query, params)
        logs = cur.fetchall()
        
        result = []
        for log in logs:
            result.append({
                'created_at': log['created_at'].isoformat() if log['created_at'] else None,
                'operation_type': log['operation_type'],
                'message': log['message'],
                'level': log['level'],
                'stage': log['stage'] or 'general',  # NULL трактуем как general
                'run_id': log['run_id']
            })
        
        return jsonify({
            'success': True,
            'logs': result
        })
    except Exception as e:
        # Логируем ошибку, но не падаем полностью
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Ошибка получения журнала: {str(e)}'
        }), 500
    finally:
        cur.close()
        conn.close()


@app.route('/api/sync/start', methods=['POST'])
def api_sync_start():
    """Запустить синхронизацию"""
    global sync_thread, sync_status
    
    # Проверяем как глобальную переменную, так и БД
    try:
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                SELECT id FROM sync_runs 
                WHERE status IN ('running', 'stopping')
                ORDER BY started_at DESC 
                LIMIT 1
            """)
            db_running = cur.fetchone() is not None
        finally:
            cur.close()
            conn.close()
    except:
        db_running = False
    
    if sync_status['running'] or db_running:
        return jsonify({
            'success': False,
            'message': 'Синхронизация уже выполняется'
        }), 400
    
    is_manual = request.json.get('manual', True)
    
    # Логируем команду старта (без run_id, он будет создан в sync())
    try:
        OperationLog.log(None, 'command', f'Команда: Запустить синхронизацию ({"ручной" if is_manual else "автоматический"} режим) — ПРИНЯТА', level='INFO', stage='general')
    except:
        pass  # Игнорируем ошибки логирования
    
    def run_sync():
        global sync_status
        sync_status['running'] = True
        sync_status['state'] = 'running'
        sync_status['message'] = 'Синхронизация запущена...'
        sync_status['current_operation'] = 'Инициализация синхронизации'
        sync_status['progress'] = {'files_processed': 0, 'records_loaded': 0, 'current_step': 'start'}
        try:
            sync(is_manual=is_manual)
            sync_status['state'] = 'idle'
            sync_status['message'] = 'Синхронизация завершена успешно'
            sync_status['current_operation'] = ''
        except Exception as e:
            # sync() должна сама вызывать SyncRun.finish() при ошибках, но на всякий случай проверяем
            # Если есть незавершенные запуски, reconcile их обработает
            error_msg = str(e)
            sync_status['state'] = 'error'
            sync_status['message'] = f'Ошибка синхронизации: {error_msg}'
            sync_status['current_operation'] = f'Ошибка: {error_msg[:60]}'
            # Логируем ошибку в журнал (без run_id, так как он может быть не создан)
            try:
                OperationLog.log(None, 'sync', f'Ошибка в потоке синхронизации: {error_msg}', level='ERROR', stage='general')
            except:
                pass
        finally:
            sync_status['running'] = False
            sync_status['progress'] = None
            # Выполняем reconcile для обработки зависших запусков (если есть)
            try:
                SyncRun.reconcile_stale_runs()
            except:
                pass
    
    sync_thread = threading.Thread(target=run_sync, daemon=True)
    sync_thread.start()
    
    return jsonify({
        'success': True,
        'message': 'Синхронизация запущена'
    })


@app.route('/api/sync/stop', methods=['POST'])
def api_sync_stop():
    """Остановить синхронизацию - использует active_run_id из /api/runtime/status"""
    global sync_status
    
    # Получаем активный run_id из runtime status (источник истины)
    # Используем прямой доступ к sync_status и БД вместо HTTP запроса
    is_running = sync_status['running']
    active_run_id = None
    
    if is_running:
        # Получаем active_run_id из БД
        try:
            conn_temp = get_connection()
            cur_temp = get_cursor(conn_temp)
            try:
                cur_temp.execute("""
                    SELECT id FROM sync_runs 
                    WHERE status IN ('running', 'stopping')
                    ORDER BY started_at DESC 
                    LIMIT 1
                """)
                current_run = cur_temp.fetchone()
                if current_run:
                    active_run_id = current_run['id']
            finally:
                cur_temp.close()
                conn_temp.close()
        except:
            pass
    
    if not is_running and not sync_status['running']:
        return jsonify({
            'success': False,
            'message': 'Синхронизация не выполняется'
        }), 400
    
    # Если нет active_run_id из runtime, пробуем найти в БД
    if not active_run_id:
        try:
            conn = get_connection()
            cur = get_cursor(conn)
            try:
                cur.execute("""
                    SELECT id FROM sync_runs 
                    WHERE status IN ('running', 'stopping')
                    ORDER BY started_at DESC 
                    LIMIT 1
                """)
                current_run = cur.fetchone()
                if current_run:
                    active_run_id = current_run['id']
            finally:
                cur.close()
                conn.close()
        except:
            pass
    
    if not active_run_id:
        return jsonify({
            'success': False,
            'message': 'Не найден активный запуск синхронизации'
        }), 404
    
    try:
        # Выставляем stop_requested для активного run
        success = SyncRun.request_stop(active_run_id)
        if success:
            SyncRun.set_status(active_run_id, 'stopping')
            # Логируем событие остановки
            OperationLog.log(active_run_id, 'command', 'Стоп запрошен. Завершаю текущие операции...', level='INFO', stage='general')
            sync_status['state'] = 'stopping'
            sync_status['message'] = 'Остановка всех процессов запрошена...'
            sync_status['current_operation'] = 'Стоп запрошен. Завершаю текущий файл...'
            return jsonify({
                'success': True,
                'message': 'Остановка всех процессов обработки запрошена',
                'run_id': active_run_id
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Не удалось установить флаг остановки (возможно, run уже завершен)'
            }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Ошибка при запросе остановки: {str(e)}'
        }), 500


@app.route('/api/sync/force-stop', methods=['POST'])
def api_sync_force_stop():
    """Принудительная остановка синхронизации - прерывает текущую операцию"""
    global sync_status
    
    try:
        # Находим активный run
        active_run_id = None
        try:
            conn = get_connection()
            cur = get_cursor(conn)
            try:
                cur.execute("""
                    SELECT id FROM sync_runs 
                    WHERE status IN ('running', 'stopping')
                    ORDER BY started_at DESC 
                    LIMIT 1
                """)
                current_run = cur.fetchone()
                if current_run:
                    active_run_id = current_run['id']
            finally:
                cur.close()
                conn.close()
        except:
            pass
        
        if not active_run_id:
            return jsonify({
                'success': False,
                'error': 'Не найден активный запуск для принудительной остановки'
            }), 404
        
        # Принудительно завершаем run
        SyncRun.finish(active_run_id, status='stopped', error_message='Принудительно остановлено пользователем')
        
        # Сбрасываем состояние синхронизации
        sync_status['running'] = False
        sync_status['state'] = 'idle'
        sync_status['message'] = 'Синхронизация принудительно остановлена'
        sync_status['current_operation'] = ''
        sync_status['progress'] = None
        
        # Логируем событие
        OperationLog.log(active_run_id, 'command', 'Принудительная остановка выполнена', level='WARNING', stage='general')
        
        return jsonify({
            'success': True,
            'message': 'Принудительная остановка выполнена',
            'run_id': active_run_id
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Ошибка принудительной остановки: {str(e)}'
        }), 500


@app.route('/api/sync/resume', methods=['POST'])
def api_sync_resume():
    """Продолжить приостановленную синхронизацию"""
    global sync_thread, sync_status
    
    if sync_status['running']:
        return jsonify({
            'success': False,
            'message': 'Синхронизация уже выполняется'
        }), 400
    
    try:
        paused_run = SyncRun.get_paused_run()
        if not paused_run:
            return jsonify({
                'success': False,
                'message': 'Нет приостановленной синхронизации'
            }), 404
        
        # Запускаем новую синхронизацию (она автоматически пропустит уже обработанные файлы)
        # Старый paused run остается в БД для истории
        is_manual = paused_run.get('is_manual', True)
        
        def run_sync():
            global sync_status
            sync_status['running'] = True
            sync_status['state'] = 'running'
            sync_status['message'] = 'Синхронизация возобновлена...'
            sync_status['current_operation'] = 'Возобновление синхронизации'
            sync_status['progress'] = {'files_processed': 0, 'records_loaded': 0, 'current_step': 'resume'}
            try:
                sync(is_manual=is_manual)
                sync_status['state'] = 'idle'
                sync_status['message'] = 'Синхронизация завершена успешно'
                sync_status['current_operation'] = ''
            except Exception as e:
                sync_status['state'] = 'error'
                sync_status['message'] = f'Ошибка синхронизации: {str(e)}'
                sync_status['current_operation'] = f'Ошибка: {str(e)[:60]}'
            finally:
                sync_status['running'] = False
                sync_status['progress'] = None
        
        sync_thread = threading.Thread(target=run_sync, daemon=True)
        sync_thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Синхронизация возобновлена'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Ошибка при возобновлении: {str(e)}'
        }), 500


@app.route('/api/init', methods=['POST'])
def api_init():
    """Инициализировать БД"""
    try:
        result = init_schema()
        if result:
            # Проверяем, были ли созданы новые таблицы или они уже существовали
            from erknm.db.connection import get_connection, get_cursor
            conn = get_connection()
            cur = get_cursor(conn)
            try:
                cur.execute("""
                    SELECT COUNT(*) as cnt 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                table_count = cur.fetchone()['cnt']
                if table_count >= 8:
                    message = 'База данных успешно инициализирована' if table_count == 8 else 'База данных уже была инициализирована'
                else:
                    message = f'База данных частично инициализирована ({table_count} таблиц)'
            finally:
                cur.close()
                conn.close()
            
            return jsonify({
                'success': True,
                'message': message
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Не удалось инициализировать базу данных'
            }), 500
    except Exception as e:
        error_msg = str(e)
        # Очищаем сообщение об ошибке от проблем с кодировкой
        if 'codec' in error_msg.lower() or 'utf-8' in error_msg.lower():
            error_msg = 'Ошибка кодировки. База данных может быть уже инициализирована. Проверьте через веб-интерфейс.'
        return jsonify({
            'success': False,
            'message': f'Ошибка инициализации: {error_msg}'
        }), 500


@app.route('/api/upload', methods=['POST'])
def api_upload():
    """Загрузить файл"""
    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'message': 'Файл не выбран'
        }), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({
            'success': False,
            'message': 'Файл не выбран'
        }), 400
    
    try:
        # Сохраняем файл временно
        from erknm.config import DOWNLOAD_DIR
        temp_path = DOWNLOAD_DIR / file.filename
        file.save(str(temp_path))
        
        # Определяем тип файла
        is_zip = temp_path.suffix.lower() == '.zip'
        
        # Обрабатываем в отдельном потоке
        def process_file():
            try:
                process_manual_file(temp_path, is_zip=is_zip)
            except Exception as e:
                print(f"Ошибка обработки файла: {e}")
        
        thread = threading.Thread(target=process_file, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'Файл {file.filename} принят к обработке'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Ошибка загрузки файла: {str(e)}'
        }), 500


# ==================== API для вкладки "База данных" ====================

@app.route('/api/db/archives')
def api_db_archives():
    """Список архивов с фильтрами"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        # Параметры фильтров
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        data_type = request.args.get('data_type', 'all')
        status = request.args.get('status', 'all')
        search = request.args.get('search', '')
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        sort_by = request.args.get('sort_by', 'created_at')
        sort_order = request.args.get('sort_order', 'desc')
        
        query = """
            SELECT 
                za.id,
                za.url,
                za.file_path,
                za.file_size,
                za.sha256_hash,
                za.status,
                za.error_message,
                za.downloaded_at,
                za.processed_at,
                za.created_at,
                za.sync_run_id,
                COALESCE(MAX(xf.data_type), 'unknown') as data_type,
                COALESCE(SUM(xf.records_count), 0) as total_records,
                COUNT(xf.id) as fragments_count,
                MAX(xf.file_name) as xml_file_name
            FROM zip_archives za
            LEFT JOIN xml_fragments xf ON za.id = xf.zip_archive_id
            WHERE 1=1
        """
        params = []
        
        if date_from:
            query += " AND za.downloaded_at >= %s"
            params.append(date_from)
        if date_to:
            query += " AND za.downloaded_at <= %s"
            params.append(date_to)
        if data_type != 'all':
            query += " AND xf.data_type = %s"
            params.append(data_type)
        if status != 'all':
            query += " AND za.status = %s"
            params.append(status)
        if search:
            query += " AND (za.url ILIKE %s OR za.file_path ILIKE %s OR za.sha256_hash ILIKE %s)"
            search_pattern = f"%{search}%"
            params.extend([search_pattern, search_pattern, search_pattern])
        
        query += " GROUP BY za.id"
        
        # Сортировка
        valid_sort_fields = ['created_at', 'downloaded_at', 'processed_at', 'file_size']
        if sort_by in valid_sort_fields:
            query += f" ORDER BY za.{sort_by} {sort_order.upper()}"
        else:
            query += " ORDER BY za.created_at DESC"
        
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cur.execute(query, params)
        archives = cur.fetchall()
        
        # Подсчет общего количества для пагинации
        count_query = "SELECT COUNT(DISTINCT za.id) as total FROM zip_archives za LEFT JOIN xml_fragments xf ON za.id = xf.zip_archive_id WHERE 1=1"
        count_params = []
        if date_from:
            count_query += " AND za.downloaded_at >= %s"
            count_params.append(date_from)
        if date_to:
            count_query += " AND za.downloaded_at <= %s"
            count_params.append(date_to)
        if data_type != 'all':
            count_query += " AND xf.data_type = %s"
            count_params.append(data_type)
        if status != 'all':
            count_query += " AND za.status = %s"
            count_params.append(status)
        if search:
            count_query += " AND (za.url ILIKE %s OR za.file_path ILIKE %s OR za.sha256_hash ILIKE %s)"
            search_pattern = f"%{search}%"
            count_params.extend([search_pattern, search_pattern, search_pattern])
        
        cur.execute(count_query, count_params)
        total = cur.fetchone()['total']
        
        # Форматируем данные
        result = []
        for arch in archives:
            sha256_short = arch['sha256_hash'][:16] + '...' if arch['sha256_hash'] and len(arch['sha256_hash']) > 16 else arch['sha256_hash']
            result.append({
                'id': arch['id'],
                'url': arch['url'],
                'file_path': arch['file_path'],
                'file_size': arch['file_size'],
                'sha256': sha256_short,
                'status': arch['status'],
                'error_message': arch['error_message'][:100] + '...' if arch['error_message'] and len(arch['error_message']) > 100 else arch['error_message'],
                'downloaded_at': arch['downloaded_at'].isoformat() if arch['downloaded_at'] else None,
                'processed_at': arch['processed_at'].isoformat() if arch['processed_at'] else None,
                'created_at': arch['created_at'].isoformat() if arch['created_at'] else None,
                'data_type': arch['data_type'],
                'total_records': arch['total_records'],
                'fragments_count': arch['fragments_count'],
                'xml_file_name': arch['xml_file_name'],
                'sync_run_id': arch['sync_run_id']
            })
        
        return jsonify({'success': True, 'archives': result, 'total': total})
    finally:
        cur.close()
        conn.close()


@app.route('/api/db/archive/<int:archive_id>')
def api_db_archive_detail(archive_id):
    """Детали архива"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        # Информация об архиве
        cur.execute("""
            SELECT * FROM zip_archives WHERE id = %s
        """, (archive_id,))
        archive = cur.fetchone()
        
        if not archive:
            return jsonify({'success': False, 'error': 'Архив не найден'}), 404
        
        # Фрагменты архива
        cur.execute("""
            SELECT id, file_name, data_type, status, records_count, error_message, created_at, processed_at
            FROM xml_fragments
            WHERE zip_archive_id = %s
            ORDER BY created_at
        """, (archive_id,))
        fragments = cur.fetchall()
        
        return jsonify({
            'success': True,
            'archive': dict(archive),
            'fragments': [dict(f) for f in fragments]
        })
    finally:
        cur.close()
        conn.close()


@app.route('/api/db/archive/<int:archive_id>/retry', methods=['POST'])
def api_db_archive_retry(archive_id):
    """Повторить обработку архива"""
    try:
        # Сбрасываем статус на pending
        conn = get_connection()
        cur = get_cursor(conn)
        try:
            cur.execute("""
                UPDATE zip_archives 
                SET status = 'pending', error_message = NULL
                WHERE id = %s
            """, (archive_id,))
            conn.commit()
            
            # Запускаем обработку в отдельном потоке
            from erknm.loader.zip_loader import process_zip_archive
            def retry_process():
                try:
                    cur.execute("SELECT url FROM zip_archives WHERE id = %s", (archive_id,))
                    url_row = cur.fetchone()
                    if url_row:
                        process_zip_archive(url_row['url'], None)
                except Exception as e:
                    print(f"Ошибка повторной обработки: {e}")
            
            thread = threading.Thread(target=retry_process, daemon=True)
            thread.start()
            
            return jsonify({'success': True, 'message': 'Обработка перезапущена'})
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/db/runs')
def api_db_runs():
    """Список запусков с фильтрами (расширенная версия)"""
    # Выполняем reconcile перед загрузкой списка
    SyncRun.reconcile_stale_runs()
    
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        status = request.args.get('status', 'all')
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        query = """
            SELECT 
                sr.id,
                sr.started_at,
                sr.finished_at,
                sr.status,
                CASE WHEN sr.is_manual THEN 'manual' ELSE 'scheduled' END as mode,
                sr.error_message,
                COALESCE((
                    SELECT COUNT(*) 
                    FROM zip_archives za 
                    WHERE za.sync_run_id = sr.id
                ), 0) as files_count,
                COALESCE((
                    SELECT COUNT(*) 
                    FROM parsed_records pr
                    JOIN zip_archives za ON pr.zip_archive_id = za.id
                    WHERE za.sync_run_id = sr.id
                ), 0) as records_count,
                COALESCE((
                    SELECT COUNT(*) 
                    FROM operation_log ol
                    WHERE ol.sync_run_id = sr.id AND ol.level = 'ERROR'
                ), 0) as errors_count,
                sr.files_processed,
                sr.records_loaded
            FROM sync_runs sr
            WHERE 1=1
        """
        params = []
        
        if date_from:
            query += " AND sr.started_at >= %s"
            params.append(date_from)
        if date_to:
            query += " AND sr.started_at <= %s"
            params.append(date_to)
        if status != 'all':
            query += " AND sr.status = %s"
            params.append(status)
        
        query += " ORDER BY sr.started_at DESC"
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cur.execute(query, params)
        runs = cur.fetchall()
        
        result = []
        for run in runs:
            result.append({
                'id': run['id'],
                'started_at': run['started_at'].isoformat() if run['started_at'] else None,
                'finished_at': run['finished_at'].isoformat() if run['finished_at'] else None,
                'status': run['status'],
                'mode': run['mode'],
                'files_count': run['files_count'] or 0,
                'records_count': run['records_count'] or 0,
                'errors_count': run['errors_count'] or 0,
                'files_processed': run['files_processed'] or 0,
                'records_loaded': run['records_loaded'] or 0,
                'error_message': run['error_message']
            })
        
        return jsonify({'success': True, 'runs': result})
    finally:
        cur.close()
        conn.close()


@app.route('/api/runs/<int:run_id>', methods=['DELETE'])
def api_run_delete(run_id):
    """Удалить запуск и связанные данные"""
    try:
        stats = SyncRun.delete_run(run_id)
        return jsonify({
            'success': True,
            'message': 'Запуск успешно удален',
            'stats': stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/runs/<int:run_id>/details')
def api_run_details(run_id):
    """Получить детальную информацию о запуске"""
    try:
        stats = SyncRun.get_run_stats(run_id)
        if not stats:
            return jsonify({
                'success': False,
                'error': 'Запуск не найден'
            }), 404
        
        return jsonify({
            'success': True,
            'run': stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/runs/<int:run_id>/files')
def api_run_files(run_id):
    """Получить список файлов запуска"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        # Проверяем существование запуска
        cur.execute("SELECT id FROM sync_runs WHERE id = %s", (run_id,))
        if not cur.fetchone():
            return jsonify({'success': False, 'error': 'Запуск не найден'}), 404
        
        cur.execute("""
            SELECT COUNT(*) as total
            FROM zip_archives
            WHERE sync_run_id = %s
        """, (run_id,))
        total = cur.fetchone()['total']
        
        cur.execute("""
            SELECT 
                id, url, file_path, file_size, sha256_hash, status,
                downloaded_at, processed_at, created_at, error_message
            FROM zip_archives
            WHERE sync_run_id = %s
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, (run_id, limit, offset))
        
        files = cur.fetchall()
        result = []
        for file in files:
            result.append({
                'id': file['id'],
                'url': file['url'],
                'file_path': file['file_path'],
                'file_size': file['file_size'],
                'sha256_hash': file['sha256_hash'],
                'status': file['status'],
                'downloaded_at': file['downloaded_at'].isoformat() if file['downloaded_at'] else None,
                'processed_at': file['processed_at'].isoformat() if file['processed_at'] else None,
                'created_at': file['created_at'].isoformat() if file['created_at'] else None,
                'error_message': file['error_message']
            })
        
        return jsonify({
            'success': True,
            'files': result,
            'total': total
        })
    finally:
        cur.close()
        conn.close()


@app.route('/api/runs/<int:run_id>/errors')
def api_run_errors(run_id):
    """Получить список ошибок запуска"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        limit = request.args.get('limit', 20, type=int)
        
        # Проверяем существование запуска
        cur.execute("SELECT id FROM sync_runs WHERE id = %s", (run_id,))
        if not cur.fetchone():
            return jsonify({'success': False, 'error': 'Запуск не найден'}), 404
        
        cur.execute("""
            SELECT 
                id, operation_type, message, level, created_at
            FROM operation_log
            WHERE sync_run_id = %s AND level = 'ERROR'
            ORDER BY created_at DESC
            LIMIT %s
        """, (run_id, limit))
        
        errors = cur.fetchall()
        result = []
        for error in errors:
            result.append({
                'id': error['id'],
                'operation_type': error['operation_type'],
                'message': error['message'],
                'level': error['level'],
                'created_at': error['created_at'].isoformat() if error['created_at'] else None
            })
        
        return jsonify({
            'success': True,
            'errors': result
        })
    finally:
        cur.close()
        conn.close()


@app.route('/api/db/parsed-records')
def api_db_parsed_records():
    """Список распознанных данных (результат парсинга) с пагинацией"""
    import logging
    
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        logging.error(f'API /api/db/parsed-records: ошибка подключения к БД: {e}')
        return jsonify({
            'success': False, 
            'error': f'Ошибка подключения к БД: {str(e)}',
            'items': [],
            'records': [],
            'total': 0
        }), 500
    
    try:
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        record_type = request.args.get('record_type', 'all')
        search = request.args.get('search', '')
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        sort_by = request.args.get('sort_by', 'created_at')
        sort_order = request.args.get('sort_order', 'desc')
        
        # Проверяем существование таблицы
        try:
            cur.execute("""
                SELECT COUNT(*) as cnt 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'parsed_records'
            """)
            table_exists = cur.fetchone()['cnt'] > 0
        except Exception as table_check_err:
            logging.warning(f'API /api/db/parsed-records: ошибка проверки таблицы: {table_check_err}')
            table_exists = False
        
        if not table_exists:
            logging.info('API /api/db/parsed-records: таблица parsed_records не существует')
            return jsonify({
                'success': True, 
                'items': [], 
                'records': [], 
                'total': 0,
                'limit': limit,
                'offset': offset,
                'message': 'Таблица parsed_records не существует'
            })
        
        # Базовые условия фильтрации
        where_conditions = ["1=1"]
        params = []
        
        if date_from:
            where_conditions.append("pr.created_at >= %s")
            params.append(date_from)
        if date_to:
            where_conditions.append("pr.created_at <= %s")
            params.append(date_to)
        if record_type != 'all':
            where_conditions.append("pr.record_type = %s")
            params.append(record_type)
        if search:
            where_conditions.append("(pr.record_key ILIKE %s OR pr.payload_json::text ILIKE %s)")
            search_pattern = f"%{search}%"
            params.extend([search_pattern, search_pattern])
        
        where_clause = " AND ".join(where_conditions)
        
        # Подсчет общего количества для пагинации (до limit/offset)
        count_query = f"SELECT COUNT(*) as total FROM parsed_records pr WHERE {where_clause}"
        cur.execute(count_query, params)
        total = cur.fetchone()['total']
        
        logging.info(f'API /api/db/parsed-records: COUNT(*) = {total}, params = {params}')
        
        # Валидация сортировки
        allowed_sort = ['created_at', 'record_date', 'record_type', 'record_key', 'id']
        if sort_by not in allowed_sort:
            sort_by = 'created_at'
        if sort_order.lower() not in ['asc', 'desc']:
            sort_order = 'desc'
        
        # Основной запрос
        query = f"""
            SELECT 
                pr.id,
                pr.zip_archive_id,
                pr.xml_fragment_id,
                pr.record_type,
                pr.record_key,
                pr.record_date,
                pr.payload_json,
                pr.created_at,
                za.url as archive_url,
                za.file_path as archive_file_path
            FROM parsed_records pr
            LEFT JOIN zip_archives za ON pr.zip_archive_id = za.id
            WHERE {where_clause}
            ORDER BY pr.{sort_by} {sort_order.upper()}
            LIMIT %s OFFSET %s
        """
        query_params = params + [limit, offset]
        
        cur.execute(query, query_params)
        records = cur.fetchall()
        
        logging.info(f'API /api/db/parsed-records: найдено {len(records)} записей из {total}, limit={limit}, offset={offset}')
        
        items = []
        for rec in records:
            items.append({
                'id': rec['id'],
                'zip_archive_id': rec['zip_archive_id'],
                'xml_fragment_id': rec['xml_fragment_id'],
                'record_type': rec['record_type'],
                'record_key': rec['record_key'],
                'record_date': rec['record_date'].isoformat() if rec['record_date'] else None,
                'payload_json': rec['payload_json'],
                'created_at': rec['created_at'].isoformat() if rec['created_at'] else None,
                'archive_url': rec['archive_url'],
                'archive_file_path': rec['archive_file_path']
            })
        
        # Унифицированный формат ответа
        return jsonify({
            'success': True, 
            'items': items,           # унифицированное имя массива
            'records': items,         # для обратной совместимости
            'total': total,
            'limit': limit,
            'offset': offset,
            'sort_by': sort_by,
            'sort_order': sort_order
        })
    except Exception as e:
        logging.error(f'API /api/db/parsed-records: исключение: {e}', exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Ошибка получения распознанных данных: {str(e)}',
            'items': [],
            'records': [],
            'total': 0
        }), 500
    finally:
        cur.close()
        conn.close()


@app.route('/api/db/errors')
def api_db_errors():
    """Список ошибок из логов с пагинацией"""
    import logging
    
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        logging.error(f'API /api/db/errors: ошибка подключения к БД: {e}')
        return jsonify({
            'success': False, 
            'error': f'Ошибка подключения к БД: {str(e)}',
            'items': [],
            'total': 0
        }), 500
    
    try:
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        search = request.args.get('search', '')
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        sort_by = request.args.get('sort_by', 'created_at')
        sort_order = request.args.get('sort_order', 'desc')
        
        # Базовые условия фильтрации
        where_conditions = ["ol.level = 'ERROR'"]
        params = []
        
        if date_from:
            where_conditions.append("ol.created_at >= %s")
            params.append(date_from)
        if date_to:
            where_conditions.append("ol.created_at <= %s")
            params.append(date_to)
        if search:
            where_conditions.append("(ol.message ILIKE %s OR ol.operation_type ILIKE %s)")
            search_pattern = f"%{search}%"
            params.extend([search_pattern, search_pattern])
        
        where_clause = " AND ".join(where_conditions)
        
        # Подсчёт общего количества записей (до применения limit/offset)
        count_query = f"SELECT COUNT(*) as total FROM operation_log ol WHERE {where_clause}"
        cur.execute(count_query, params)
        total = cur.fetchone()['total']
        
        logging.info(f'API /api/db/errors: COUNT(*) = {total}, params = {params}')
        
        # Валидация сортировки
        allowed_sort = ['created_at', 'operation_type', 'message', 'level']
        if sort_by not in allowed_sort:
            sort_by = 'created_at'
        if sort_order.lower() not in ['asc', 'desc']:
            sort_order = 'desc'
        
        # Основной запрос с пагинацией
        query = f"""
            SELECT 
                ol.id,
                ol.created_at,
                ol.operation_type,
                ol.message,
                ol.level,
                ol.sync_run_id,
                NULL as archive_id,
                NULL as archive_url
            FROM operation_log ol
            WHERE {where_clause}
            ORDER BY ol.{sort_by} {sort_order.upper()}
            LIMIT %s OFFSET %s
        """
        query_params = params + [limit, offset]
        
        cur.execute(query, query_params)
        errors = cur.fetchall()
        
        logging.info(f'API /api/db/errors: найдено {len(errors)} ошибок из {total}, limit={limit}, offset={offset}')
        
        # Формируем результат в едином формате
        items = []
        for err in errors:
            items.append({
                'id': err['id'],
                'created_at': err['created_at'].isoformat() if err['created_at'] else None,
                'operation_type': err['operation_type'],
                'message': err['message'],
                'level': err['level'],
                'run_id': err['sync_run_id'],
                'archive_id': err['archive_id'],
                'archive_url': err['archive_url']
            })
        
        # Унифицированный формат ответа
        return jsonify({
            'success': True,
            'items': items,           # унифицированное имя массива
            'errors': items,          # для обратной совместимости
            'total': total,           # общее количество записей
            'limit': limit,
            'offset': offset,
            'sort_by': sort_by,
            'sort_order': sort_order
        })
    except Exception as e:
        logging.error(f'API /api/db/errors: исключение: {e}', exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Ошибка получения ошибок: {str(e)}',
            'items': [],
            'errors': [],
            'total': 0
        }), 500
    finally:
        cur.close()
        conn.close()


# ==================== API для вкладки "Настройки робота" ====================

@app.route('/api/settings/get')
def api_settings_get():
    """Получить настройки"""
    try:
        # Инициализируем настройки по умолчанию если их нет
        Settings.set_defaults()
        settings = Settings.get_all()
        
        # Заполняем значения по умолчанию если их нет
        defaults = {
            'schedule_enabled': 'false',
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
            'sync_order': 'old_to_new',
            'stop_on_repeats_enabled': 'false',
            'stop_on_repeats_count': '3'
        }
        
        for key, default_value in defaults.items():
            if key not in settings:
                settings[key] = default_value
        
        return jsonify({'success': True, 'settings': settings})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/save', methods=['POST'])
def api_settings_save():
    """Сохранить настройки"""
    try:
        data = request.json
        if not data or 'settings' not in data:
            return jsonify({'success': False, 'error': 'Нет данных для сохранения'}), 400
        
        settings = data['settings']
        for key, value in settings.items():
            Settings.set(key, value)
        
        return jsonify({
            'success': True,
            'message': 'Настройки сохранены',
            'updated_at': 'now'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== API для просмотра содержимого данных ====================

@app.route('/api/db/archive/<int:archive_id>/records')
def api_db_archive_records(archive_id):
    """Получить записи конкретного архива с пагинацией"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        search = request.args.get('search', '')
        sort_by = request.args.get('sort_by', 'id')
        sort_order = request.args.get('sort_order', 'asc')
        
        # Получаем информацию об архиве
        cur.execute("""
            SELECT za.*, xf.data_type, xf.file_name as xml_file_name
            FROM zip_archives za
            LEFT JOIN xml_fragments xf ON za.id = xf.zip_archive_id
            WHERE za.id = %s
            LIMIT 1
        """, (archive_id,))
        archive = cur.fetchone()
        
        if not archive:
            return jsonify({'success': False, 'error': 'Archive not found'}), 404
        
        # Строим запрос для записей
        query = """
            SELECT 
                pr.id,
                pr.record_type,
                pr.record_key,
                pr.record_date,
                pr.payload_json,
                pr.created_at
            FROM parsed_records pr
            WHERE pr.zip_archive_id = %s
        """
        params = [archive_id]
        
        if search:
            query += " AND (pr.record_key ILIKE %s OR pr.payload_json::text ILIKE %s)"
            search_pattern = f"%{search}%"
            params.extend([search_pattern, search_pattern])
        
        # Валидация сортировки
        allowed_sort = ['id', 'record_type', 'record_key', 'record_date', 'created_at']
        if sort_by not in allowed_sort:
            sort_by = 'id'
        if sort_order not in ['asc', 'desc']:
            sort_order = 'asc'
        
        query += f" ORDER BY pr.{sort_by} {sort_order.upper()}"
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cur.execute(query, params)
        records = cur.fetchall()
        
        # Подсчет общего количества
        count_query = "SELECT COUNT(*) as total FROM parsed_records WHERE zip_archive_id = %s"
        count_params = [archive_id]
        if search:
            count_query += " AND (record_key ILIKE %s OR payload_json::text ILIKE %s)"
            count_params.extend([f"%{search}%", f"%{search}%"])
        
        cur.execute(count_query, count_params)
        total = cur.fetchone()['total']
        
        # Получаем список всех уникальных ключей из payload_json для формирования колонок
        columns = set(['id', 'record_type', 'record_key', 'record_date', 'created_at'])
        for rec in records:
            if rec['payload_json']:
                columns.update(rec['payload_json'].keys())
        
        result = []
        for rec in records:
            row = {
                'id': rec['id'],
                'record_type': rec['record_type'],
                'record_key': rec['record_key'],
                'record_date': rec['record_date'].isoformat() if rec['record_date'] else None,
                'created_at': rec['created_at'].isoformat() if rec['created_at'] else None,
            }
            # Добавляем поля из payload_json
            if rec['payload_json']:
                for key, value in rec['payload_json'].items():
                    row[f'payload_{key}'] = value
            result.append(row)
        
        return jsonify({
            'success': True,
            'archive': {
                'id': archive['id'],
                'url': archive['url'],
                'file_path': archive['file_path'],
                'data_type': archive['data_type'],
                'xml_file_name': archive['xml_file_name'],
                'status': archive['status']
            },
            'records': result,
            'columns': sorted(list(columns)),
            'total': total,
            'limit': limit,
            'offset': offset
        })
    finally:
        cur.close()
        conn.close()


@app.route('/api/db/record/<int:record_id>')
def api_db_record_detail(record_id):
    """Получить детали одной записи"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        cur.execute("""
            SELECT 
                pr.*,
                za.url as archive_url,
                za.file_path as archive_file_path,
                xf.file_name as xml_file_name,
                xf.data_type
            FROM parsed_records pr
            LEFT JOIN zip_archives za ON pr.zip_archive_id = za.id
            LEFT JOIN xml_fragments xf ON pr.xml_fragment_id = xf.id
            WHERE pr.id = %s
        """, (record_id,))
        record = cur.fetchone()
        
        if not record:
            return jsonify({'success': False, 'error': 'Record not found'}), 404
        
        # Получаем сырой XML если есть
        raw_xml = None
        if record['record_type'] == 'plan' and record['xml_fragment_id']:
            cur.execute("""
                SELECT xml_content::text FROM plans_raw 
                WHERE xml_fragment_id = %s LIMIT 1
            """, (record['xml_fragment_id'],))
            raw = cur.fetchone()
            if raw:
                raw_xml = raw['xml_content']
        elif record['record_type'] == 'inspection' and record['xml_fragment_id']:
            cur.execute("""
                SELECT xml_content::text FROM inspections_raw 
                WHERE xml_fragment_id = %s LIMIT 1
            """, (record['xml_fragment_id'],))
            raw = cur.fetchone()
            if raw:
                raw_xml = raw['xml_content']
        
        return jsonify({
            'success': True,
            'record': {
                'id': record['id'],
                'zip_archive_id': record['zip_archive_id'],
                'xml_fragment_id': record['xml_fragment_id'],
                'record_type': record['record_type'],
                'record_key': record['record_key'],
                'record_date': record['record_date'].isoformat() if record['record_date'] else None,
                'payload_json': record['payload_json'],
                'created_at': record['created_at'].isoformat() if record['created_at'] else None,
                'archive_url': record['archive_url'],
                'archive_file_path': record['archive_file_path'],
                'xml_file_name': record['xml_file_name'],
                'data_type': record['data_type']
            },
            'raw_xml': raw_xml
        })
    finally:
        cur.close()
        conn.close()


# ==================== API для очистки данных по периоду ====================

@app.route('/api/cleanup/preview', methods=['POST'])
def api_cleanup_preview():
    """Предварительный просмотр того, что будет удалено"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        data = request.json or {}
        period = data.get('period', 'today')  # 'hour', 'today', 'week', 'custom'
        date_from = data.get('date_from')
        date_to = data.get('date_to')
        target = data.get('target', 'runs')  # 'runs', 'archives', 'logs', 'all'
        
        # Вычисляем даты
        from datetime import datetime, timedelta
        now = datetime.now()
        
        if period == 'hour':
            start_date = now - timedelta(hours=1)
            end_date = now
        elif period == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = now
        elif period == 'week':
            start_date = now - timedelta(days=7)
            end_date = now
        elif period == 'custom' and date_from and date_to:
            start_date = datetime.fromisoformat(date_from)
            end_date = datetime.fromisoformat(date_to)
        else:
            return jsonify({'success': False, 'error': 'Invalid period'}), 400
        
        preview = {
            'period': {'from': start_date.isoformat(), 'to': end_date.isoformat()},
            'counts': {}
        }
        
        # Подсчитываем что будет удалено
        if target in ['runs', 'all']:
            cur.execute("""
                SELECT COUNT(*) as cnt FROM sync_runs 
                WHERE started_at >= %s AND started_at <= %s
            """, (start_date, end_date))
            preview['counts']['sync_runs'] = cur.fetchone()['cnt']
        
        if target in ['logs', 'all']:
            cur.execute("""
                SELECT COUNT(*) as cnt FROM operation_log 
                WHERE created_at >= %s AND created_at <= %s
            """, (start_date, end_date))
            preview['counts']['operation_log'] = cur.fetchone()['cnt']
        
        if target in ['archives', 'all']:
            cur.execute("""
                SELECT COUNT(*) as cnt FROM zip_archives 
                WHERE created_at >= %s AND created_at <= %s
            """, (start_date, end_date))
            preview['counts']['zip_archives'] = cur.fetchone()['cnt']
            
            cur.execute("""
                SELECT COUNT(*) as cnt FROM xml_fragments xf
                JOIN zip_archives za ON xf.zip_archive_id = za.id
                WHERE za.created_at >= %s AND za.created_at <= %s
            """, (start_date, end_date))
            preview['counts']['xml_fragments'] = cur.fetchone()['cnt']
            
            cur.execute("""
                SELECT COUNT(*) as cnt FROM parsed_records pr
                WHERE pr.created_at >= %s AND pr.created_at <= %s
            """, (start_date, end_date))
            preview['counts']['parsed_records'] = cur.fetchone()['cnt']
            
            cur.execute("""
                SELECT COUNT(*) as cnt FROM plans_raw pl
                JOIN xml_fragments xf ON pl.xml_fragment_id = xf.id
                JOIN zip_archives za ON xf.zip_archive_id = za.id
                WHERE za.created_at >= %s AND za.created_at <= %s
            """, (start_date, end_date))
            preview['counts']['plans_raw'] = cur.fetchone()['cnt']
            
            cur.execute("""
                SELECT COUNT(*) as cnt FROM inspections_raw ir
                JOIN xml_fragments xf ON ir.xml_fragment_id = xf.id
                JOIN zip_archives za ON xf.zip_archive_id = za.id
                WHERE za.created_at >= %s AND za.created_at <= %s
            """, (start_date, end_date))
            preview['counts']['inspections_raw'] = cur.fetchone()['cnt']
        
        return jsonify({'success': True, 'preview': preview})
    finally:
        cur.close()
        conn.close()


@app.route('/api/cleanup/execute', methods=['POST'])
def api_cleanup_execute():
    """Выполнить очистку данных"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        data = request.json or {}
        period = data.get('period', 'today')
        date_from = data.get('date_from')
        date_to = data.get('date_to')
        target = data.get('target', 'runs')
        confirm = data.get('confirm', False)
        
        if not confirm:
            return jsonify({'success': False, 'error': 'Confirmation required'}), 400
        
        # Вычисляем даты
        from datetime import datetime, timedelta
        now = datetime.now()
        
        if period == 'hour':
            start_date = now - timedelta(hours=1)
            end_date = now
        elif period == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = now
        elif period == 'week':
            start_date = now - timedelta(days=7)
            end_date = now
        elif period == 'custom' and date_from and date_to:
            start_date = datetime.fromisoformat(date_from)
            end_date = datetime.fromisoformat(date_to)
        else:
            return jsonify({'success': False, 'error': 'Invalid period'}), 400
        
        deleted = {}
        
        # Удаляем данные (порядок важен из-за foreign keys)
        if target in ['archives', 'all']:
            # Сначала удаляем parsed_records
            cur.execute("""
                DELETE FROM parsed_records 
                WHERE created_at >= %s AND created_at <= %s
            """, (start_date, end_date))
            deleted['parsed_records'] = cur.rowcount
            
            # Удаляем plans_raw и inspections_raw через каскад от xml_fragments
            # Удаляем zip_archives (каскадно удалит xml_fragments, plans_raw, inspections_raw)
            cur.execute("""
                DELETE FROM zip_archives 
                WHERE created_at >= %s AND created_at <= %s
            """, (start_date, end_date))
            deleted['zip_archives'] = cur.rowcount
        
        if target in ['logs', 'all']:
            cur.execute("""
                DELETE FROM operation_log 
                WHERE created_at >= %s AND created_at <= %s
            """, (start_date, end_date))
            deleted['operation_log'] = cur.rowcount
        
        if target in ['runs', 'all']:
            # Сначала очищаем ссылки на sync_run_id в operation_log
            cur.execute("""
                UPDATE operation_log 
                SET sync_run_id = NULL
                WHERE sync_run_id IN (
                    SELECT id FROM sync_runs 
                    WHERE started_at >= %s AND started_at <= %s
                )
            """, (start_date, end_date))
            
            cur.execute("""
                DELETE FROM sync_runs 
                WHERE started_at >= %s AND started_at <= %s
            """, (start_date, end_date))
            deleted['sync_runs'] = cur.rowcount
        
        conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'Cleanup completed',
            'deleted': deleted
        })
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cur.close()
        conn.close()


# ==================== API для просмотра содержимого XML ====================

@app.route('/api/xml-contents')
def api_xml_contents():
    """Список XML контента с фильтрами и пагинацией"""
    import logging
    
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        logging.error(f'API /api/xml-contents: ошибка подключения к БД: {e}')
        return jsonify({
            'success': False, 
            'error': f'Ошибка подключения к БД: {str(e)}',
            'items': [],
            'contents': [],
            'total': 0
        }), 500
    
    try:
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        data_type = request.args.get('data_type', 'all')  # 'all', 'plan', 'inspection'
        search = request.args.get('search', '')
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        sort_by = request.args.get('sort_by', 'created_at')
        sort_order = request.args.get('sort_order', 'desc')
        
        logging.info(f'API /api/xml-contents: запрос с params: data_type={data_type}, limit={limit}, offset={offset}')
        
        # Проверяем существование таблиц plans_raw и inspections_raw
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name IN ('plans_raw', 'inspections_raw')
        """)
        existing_tables = [row['table_name'] for row in cur.fetchall()]
        logging.info(f'API /api/xml-contents: существующие таблицы: {existing_tables}')
        
        has_plans = 'plans_raw' in existing_tables
        has_inspections = 'inspections_raw' in existing_tables
        
        if not has_plans and not has_inspections:
            logging.info('API /api/xml-contents: таблицы plans_raw и inspections_raw не существуют')
            return jsonify({
                'success': True, 
                'items': [], 
                'contents': [], 
                'total': 0,
                'limit': limit,
                'offset': offset,
                'message': 'Таблицы XML контента не существуют'
            })
        
        # Объединяем планы и проверки в один список
        # Используем UNION для объединения результатов
        query_parts = []
        params = []
        param_counter = 0
        
        # Запрос для планов
        if data_type in ('all', 'plan') and has_plans:
            plan_query = """
                SELECT 
                    pr.id,
                    'plan' as data_type,
                    pr.xml_fragment_id,
                    xf.file_name,
                    xf.zip_archive_id,
                    za.url as archive_url,
                    za.file_path as archive_file_path,
                    xf.processed_at,
                    xf.status as fragment_status,
                    xf.error_message,
                    pr.created_at,
                    LENGTH(pr.xml_content::text) as xml_size,
                    xf.data_type as dataset_type
                FROM plans_raw pr
                JOIN xml_fragments xf ON pr.xml_fragment_id = xf.id
                LEFT JOIN zip_archives za ON xf.zip_archive_id = za.id
                WHERE 1=1
            """
            
            if search:
                plan_query += " AND (xf.file_name ILIKE %s OR za.url ILIKE %s OR pr.xml_content::text ILIKE %s)"
                search_pattern = f"%{search}%"
                params.extend([search_pattern, search_pattern, search_pattern])
                param_counter += 3
            
            if date_from:
                plan_query += " AND pr.created_at >= %s"
                params.append(date_from)
                param_counter += 1
            
            if date_to:
                plan_query += " AND pr.created_at <= %s"
                params.append(date_to)
                param_counter += 1
            
            query_parts.append(plan_query)
        
        # Запрос для проверок
        if data_type in ('all', 'inspection') and has_inspections:
            inspection_query = """
                SELECT 
                    ir.id,
                    'inspection' as data_type,
                    ir.xml_fragment_id,
                    xf.file_name,
                    xf.zip_archive_id,
                    za.url as archive_url,
                    za.file_path as archive_file_path,
                    xf.processed_at,
                    xf.status as fragment_status,
                    xf.error_message,
                    ir.created_at,
                    LENGTH(ir.xml_content::text) as xml_size,
                    xf.data_type as dataset_type
                FROM inspections_raw ir
                JOIN xml_fragments xf ON ir.xml_fragment_id = xf.id
                LEFT JOIN zip_archives za ON xf.zip_archive_id = za.id
                WHERE 1=1
            """
            
            # Для UNION ALL параметры добавляются отдельно для каждой части запроса
            inspection_params = []
            if search:
                inspection_query += " AND (xf.file_name ILIKE %s OR za.url ILIKE %s OR ir.xml_content::text ILIKE %s)"
                search_pattern = f"%{search}%"
                inspection_params.extend([search_pattern, search_pattern, search_pattern])
            
            if date_from:
                inspection_query += " AND ir.created_at >= %s"
                inspection_params.append(date_from)
            
            if date_to:
                inspection_query += " AND ir.created_at <= %s"
                inspection_params.append(date_to)
            
            if query_parts:
                query_parts.append(" UNION ALL ")
                params.extend(inspection_params)
            else:
                params = inspection_params
            query_parts.append(inspection_query)
        
        if not query_parts:
            logging.info('API /api/xml-contents: нет данных для запрошенного типа')
            return jsonify({
                'success': True, 
                'items': [], 
                'contents': [], 
                'total': 0,
                'limit': limit,
                'offset': offset
            })
        
        # Объединяем запросы
        full_query = "".join(query_parts)
        
        # Валидация сортировки
        allowed_sort = ['created_at', 'processed_at', 'data_type', 'xml_size']
        if sort_by not in allowed_sort:
            sort_by = 'created_at'
        if sort_order.lower() not in ['asc', 'desc']:
            sort_order = 'desc'
        
        # Подсчет общего количества (до limit/offset)
        count_query = f"SELECT COUNT(*) as total FROM ({full_query}) as combined"
        cur.execute(count_query, params)
        total = cur.fetchone()['total']
        
        logging.info(f'API /api/xml-contents: COUNT(*) = {total}')
        
        # Добавляем сортировку и пагинацию
        full_query_paginated = f"SELECT * FROM ({full_query}) as combined ORDER BY {sort_by} {sort_order.upper()} LIMIT %s OFFSET %s"
        params_paginated = params + [limit, offset]
        
        cur.execute(full_query_paginated, params_paginated)
        rows = cur.fetchall()
        
        logging.info(f'API /api/xml-contents: найдено {len(rows)} записей из {total}')
        
        items = []
        for row in rows:
            # Формируем уникальный ID: тип + id
            unique_id = f"{row['data_type']}_{row['id']}"
            
            items.append({
                'id': unique_id,
                'raw_id': row['id'],
                'data_type': row['data_type'],
                'xml_fragment_id': row['xml_fragment_id'],
                'file_name': row['file_name'],
                'zip_archive_id': row['zip_archive_id'],
                'archive_url': row['archive_url'],
                'archive_file_path': row['archive_file_path'],
                'processed_at': row['processed_at'].isoformat() if row['processed_at'] else None,
                'status': row['fragment_status'],
                'error_message': row['error_message'],
                'created_at': row['created_at'].isoformat() if row['created_at'] else None,
                'xml_size': row['xml_size'],
                'dataset_type': row['dataset_type']
            })
        
        # Унифицированный формат ответа
        return jsonify({
            'success': True,
            'items': items,           # унифицированное имя массива
            'contents': items,        # для обратной совместимости
            'total': total,
            'limit': limit,
            'offset': offset,
            'sort_by': sort_by,
            'sort_order': sort_order
        })
    except Exception as e:
        logging.error(f'API /api/xml-contents: исключение: {e}', exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Ошибка получения XML контента: {str(e)}',
            'items': [],
            'contents': [],
            'total': 0
        }), 500
    finally:
        cur.close()
        conn.close()


@app.route('/api/xml-contents/<path:content_id>')
def api_xml_content_detail(content_id):
    """Получить конкретный XML контент"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        # Парсим ID: тип_id
        parts = content_id.split('_', 1)
        if len(parts) != 2:
            return jsonify({'success': False, 'error': 'Invalid content ID format'}), 400
        
        data_type, raw_id = parts
        raw_id = int(raw_id)
        
        # Выбираем из соответствующей таблицы
        if data_type == 'plan':
            query = """
                SELECT 
                    pr.id,
                    pr.xml_content::text as xml_content,
                    pr.created_at,
                    xf.file_name,
                    xf.zip_archive_id,
                    xf.data_type as dataset_type,
                    xf.status,
                    xf.error_message,
                    xf.processed_at,
                    za.url as archive_url,
                    za.file_path as archive_file_path
                FROM plans_raw pr
                JOIN xml_fragments xf ON pr.xml_fragment_id = xf.id
                LEFT JOIN zip_archives za ON xf.zip_archive_id = za.id
                WHERE pr.id = %s
            """
        elif data_type == 'inspection':
            query = """
                SELECT 
                    ir.id,
                    ir.xml_content::text as xml_content,
                    ir.created_at,
                    xf.file_name,
                    xf.zip_archive_id,
                    xf.data_type as dataset_type,
                    xf.status,
                    xf.error_message,
                    xf.processed_at,
                    za.url as archive_url,
                    za.file_path as archive_file_path
                FROM inspections_raw ir
                JOIN xml_fragments xf ON ir.xml_fragment_id = xf.id
                LEFT JOIN zip_archives za ON xf.zip_archive_id = za.id
                WHERE ir.id = %s
            """
        else:
            return jsonify({'success': False, 'error': 'Invalid data type'}), 400
        
        cur.execute(query, (raw_id,))
        row = cur.fetchone()
        
        if not row:
            return jsonify({'success': False, 'error': 'Content not found'}), 404
        
        return jsonify({
            'success': True,
            'content': {
                'id': f"{data_type}_{row['id']}",
                'raw_id': row['id'],
                'data_type': data_type,
                'xml_content': row['xml_content'],
                'file_name': row['file_name'],
                'zip_archive_id': row['zip_archive_id'],
                'archive_url': row['archive_url'],
                'archive_file_path': row['archive_file_path'],
                'dataset_type': row['dataset_type'],
                'status': row['status'],
                'error_message': row['error_message'],
                'processed_at': row['processed_at'].isoformat() if row['processed_at'] else None,
                'created_at': row['created_at'].isoformat() if row['created_at'] else None,
                'xml_size': len(row['xml_content']) if row['xml_content'] else 0
            }
        })
    finally:
        cur.close()
        conn.close()


@app.route('/api/xml-contents/<path:content_id>/raw')
def api_xml_content_raw(content_id):
    """Получить raw XML текст"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        parts = content_id.split('_', 1)
        if len(parts) != 2:
            return '', 400
        
        data_type, raw_id = parts
        raw_id = int(raw_id)
        
        if data_type == 'plan':
            query = "SELECT xml_content::text as xml_content FROM plans_raw WHERE id = %s"
        elif data_type == 'inspection':
            query = "SELECT xml_content::text as xml_content FROM inspections_raw WHERE id = %s"
        else:
            return '', 400
        
        cur.execute(query, (raw_id,))
        row = cur.fetchone()
        
        if not row or not row['xml_content']:
            return '', 404
        
        from flask import Response
        response = Response(row['xml_content'], mimetype='application/xml; charset=utf-8')
        response.headers['Content-Disposition'] = f'inline; filename="content_{content_id}.xml"'
        return response
    finally:
        cur.close()
        conn.close()


# ==================== API для оперативного лога действий ====================

@app.route('/api/runtime/status')
def api_runtime_status():
    """Текущий статус выполнения операций - источник истины для статуса запусков"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        # Получаем текущий активный запуск синхронизации (running или stopping)
        cur.execute("""
            SELECT id, started_at, status, files_processed, records_loaded, is_manual
            FROM sync_runs
            WHERE status IN ('running', 'stopping')
            ORDER BY started_at DESC
            LIMIT 1
        """)
        current_run = cur.fetchone()
        
        # Определяем реальный статус: если воркер работает (sync_status['running'] = True),
        # то статус должен быть 'running', даже если в БД стоит 'stopped' или 'aborted'
        active_run_id = None
        active_state = 'idle'
        
        if sync_status['running']:
            # Воркер реально работает - это источник истины
            active_state = sync_status['state']  # 'running' или 'stopping'
            if current_run:
                active_run_id = current_run['id']
            # Если в БД нет текущего run, но воркер работает, создаем временный ID
            # (это может быть в момент создания нового run)
        elif current_run:
            # В БД есть run, но воркер не работает - используем статус из БД
            active_run_id = current_run['id']
            active_state = current_run['status']
        
        # Получаем последнюю операцию из лога
        cur.execute("""
            SELECT operation_type, message, level, created_at
            FROM operation_log
            ORDER BY created_at DESC
            LIMIT 1
        """)
        last_event = cur.fetchone()
        
        # Определяем текущую операцию на основе последнего события
        current_operation = sync_status.get('current_operation', '')
        if last_event:
            op_type = last_event['operation_type']
            msg = last_event['message']
            if op_type == 'sync':
                if 'скачивание' in msg.lower() or 'download' in msg.lower():
                    current_operation = 'Скачивание list.xml'
                elif 'парсинг' in msg.lower() or 'parse' in msg.lower():
                    current_operation = 'Парсинг list.xml'
                elif 'обработка порции' in msg.lower() or 'batch' in msg.lower():
                    current_operation = msg
                elif 'обработка набора' in msg.lower():
                    current_operation = msg
                elif 'пауза' in msg.lower():
                    current_operation = 'Пауза между порциями'
                else:
                    current_operation = msg
            elif op_type == 'zip':
                current_operation = f'Обработка ZIP: {msg[:60]}...' if len(msg) > 60 else f'Обработка ZIP: {msg}'
            elif op_type == 'meta':
                current_operation = f'Обработка метаданных: {msg[:50]}...' if len(msg) > 50 else f'Обработка метаданных: {msg}'
            else:
                current_operation = msg[:80] if len(msg) > 80 else msg
        
        return jsonify({
            'success': True,
            'state': active_state,  # Реальный статус: 'idle', 'running', 'stopping'
            'running': sync_status['running'],  # Воркер реально работает
            'active_run_id': active_run_id,  # ID активного запуска (если есть)
            'message': sync_status['message'],
            'current_operation': current_operation or sync_status.get('current_operation', 'Нет активных операций'),
            'current_file': sync_status.get('current_file', ''),
            'progress': sync_status.get('progress'),
            'current_run': dict(current_run) if current_run else None,
            'last_event': dict(last_event) if last_event else None
        })
    finally:
        cur.close()
        conn.close()


@app.route('/api/runtime/events')
def api_runtime_events():
    """Последние события для оперативного лога"""
    try:
        conn = get_connection()
        cur = get_cursor(conn)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
    try:
        limit = request.args.get('limit', 50, type=int)
        level = request.args.get('level', 'all')  # all, INFO, WARNING, ERROR
        operation_type = request.args.get('operation_type', 'all')  # all, sync, zip, meta, etc.
        
        query = """
            SELECT 
                ol.id,
                ol.created_at,
                ol.operation_type,
                ol.message,
                ol.level,
                ol.sync_run_id,
                sr.status as run_status
            FROM operation_log ol
            LEFT JOIN sync_runs sr ON ol.sync_run_id = sr.id
            WHERE 1=1
        """
        params = []
        
        if level != 'all':
            query += " AND ol.level = %s"
            params.append(level)
        
        if operation_type != 'all':
            query += " AND ol.operation_type = %s"
            params.append(operation_type)
        
        query += " ORDER BY ol.created_at DESC LIMIT %s"
        params.append(limit)
        
        cur.execute(query, params)
        events = cur.fetchall()
        
        result = []
        for event in events:
            result.append({
                'id': event['id'],
                'timestamp': event['created_at'].isoformat() if event['created_at'] else None,
                'operation_type': event['operation_type'],
                'message': event['message'],
                'level': event['level'],
                'run_id': event['sync_run_id'],
                'run_status': event['run_status']
            })
        
        return jsonify({
            'success': True,
            'events': result
        })
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

