"""CLI интерфейс"""
import click
from pathlib import Path
from erknm.sync.synchronizer import sync, process_manual_file
from erknm.db.schema import init_schema
from erknm.db.connection import get_connection, get_cursor


@click.group()
def cli():
    """Робот сбора и инкрементальной загрузки открытых данных ФГИС ЕРКНМ"""
    pass


@cli.command()
def init():
    """Инициализировать схему базы данных"""
    click.echo("Инициализация схемы базы данных...")
    try:
        init_schema()
        click.echo("✓ Схема базы данных успешно инициализирована")
    except Exception as e:
        click.echo(f"✗ Ошибка: {e}", err=True)
        raise click.Abort()


@cli.command()
def sync_cmd():
    """Запустить автоматическую синхронизацию"""
    click.echo("Запуск синхронизации...")
    try:
        sync(is_manual=False)
        click.echo("✓ Синхронизация завершена успешно")
    except Exception as e:
        click.echo(f"✗ Ошибка синхронизации: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--zip/--xml', default=None, help='Тип файла (определяется автоматически, если не указан)')
def load_file(file_path, zip):
    """Загрузить файл вручную"""
    click.echo(f"Загрузка файла: {file_path}")
    try:
        is_zip = zip if zip is not None else None
        process_manual_file(Path(file_path), is_zip=is_zip)
        click.echo("✓ Файл успешно загружен")
    except Exception as e:
        click.echo(f"✗ Ошибка загрузки: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--limit', default=50, help='Количество записей для отображения')
@click.option('--level', type=click.Choice(['INFO', 'WARNING', 'ERROR', 'ALL']), default='ALL')
def show_logs(limit, level):
    """Показать журнал операций"""
    conn = get_connection()
    cur = get_cursor(conn)
    
    try:
        query = """
            SELECT ol.created_at, ol.operation_type, ol.message, ol.level, sr.id as run_id
            FROM operation_log ol
            LEFT JOIN sync_runs sr ON ol.sync_run_id = sr.id
        """
        params = []
        
        if level != 'ALL':
            query += " WHERE ol.level = %s"
            params.append(level)
        
        query += " ORDER BY ol.created_at DESC LIMIT %s"
        params.append(limit)
        
        cur.execute(query, params)
        logs = cur.fetchall()
        
        if not logs:
            click.echo("Журнал пуст")
            return
        
        click.echo(f"\nПоследние {len(logs)} записей журнала:\n")
        click.echo(f"{'Дата/Время':<20} {'Тип':<20} {'Уровень':<10} {'Сообщение'}")
        click.echo("-" * 100)
        
        for log in logs:
            created_at = log['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            op_type = log['operation_type'][:18]
            level_str = log['level']
            message = log['message'][:50] + '...' if len(log['message']) > 50 else log['message']
            
            click.echo(f"{created_at:<20} {op_type:<20} {level_str:<10} {message}")
        
    finally:
        cur.close()
        conn.close()


@cli.command()
@click.option('--limit', default=10, help='Количество запусков для отображения')
def show_runs(limit):
    """Показать последние запуски синхронизации"""
    conn = get_connection()
    cur = get_cursor(conn)
    
    try:
        cur.execute("""
            SELECT id, started_at, finished_at, status, is_manual, 
                   files_processed, records_loaded, error_message
            FROM sync_runs
            ORDER BY started_at DESC
            LIMIT %s
        """, (limit,))
        
        runs = cur.fetchall()
        
        if not runs:
            click.echo("Запуски не найдены")
            return
        
        click.echo(f"\nПоследние {len(runs)} запусков:\n")
        click.echo(f"{'ID':<5} {'Начало':<20} {'Завершение':<20} {'Статус':<15} {'Тип':<10} {'Файлов':<10} {'Записей':<10}")
        click.echo("-" * 100)
        
        for run in runs:
            run_id = run['id']
            started = run['started_at'].strftime('%Y-%m-%d %H:%M:%S')
            finished = run['finished_at'].strftime('%Y-%m-%d %H:%M:%S') if run['finished_at'] else 'N/A'
            status = run['status']
            is_manual = 'Ручной' if run['is_manual'] else 'Авто'
            files = run['files_processed'] or 0
            records = run['records_loaded'] or 0
            
            click.echo(f"{run_id:<5} {started:<20} {finished:<20} {status:<15} {is_manual:<10} {files:<10} {records:<10}")
        
    finally:
        cur.close()
        conn.close()


@cli.command()
@click.argument('dataset_id', type=int)
@click.argument('data_type', type=click.Choice(['plan', 'inspection']))
def reclassify_dataset_cmd(dataset_id, data_type):
    """Переклассифицировать набор данных"""
    from erknm.reclassify import reclassify_dataset
    from erknm.db.models import SyncRun
    
    run = SyncRun.create(is_manual=True)
    run_id = run['id']
    
    try:
        click.echo(f"Переклассификация набора данных {dataset_id} как {data_type}...")
        records = reclassify_dataset(dataset_id, data_type, run_id)
        SyncRun.finish(run_id, 'completed', None, 0, records)
        click.echo(f"✓ Переклассификация завершена. Загружено записей: {records}")
    except Exception as e:
        SyncRun.finish(run_id, 'error', str(e), 0, 0)
        click.echo(f"✗ Ошибка: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.argument('fragment_id', type=int)
@click.argument('data_type', type=click.Choice(['plan', 'inspection']))
def reclassify_fragment_cmd(fragment_id, data_type):
    """Переклассифицировать XML-фрагмент"""
    from erknm.reclassify import reclassify_xml_fragment
    from erknm.db.models import SyncRun
    
    run = SyncRun.create(is_manual=True)
    run_id = run['id']
    
    try:
        click.echo(f"Переклассификация XML-фрагмента {fragment_id} как {data_type}...")
        records = reclassify_xml_fragment(fragment_id, data_type, run_id)
        SyncRun.finish(run_id, 'completed', None, 0, records)
        click.echo(f"✓ Переклассификация завершена. Загружено записей: {records}")
    except Exception as e:
        SyncRun.finish(run_id, 'error', str(e), 0, 0)
        click.echo(f"✗ Ошибка: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()

