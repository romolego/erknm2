"""Проверка состояния БД"""
from erknm.db.connection import get_connection, get_cursor

try:
    conn = get_connection()
    cur = get_cursor(conn)
    
    # Проверяем количество таблиц
    cur.execute("""
        SELECT COUNT(*) as cnt 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    result = cur.fetchone()
    table_count = result['cnt']
    
    print("=" * 60)
    print("Проверка базы данных")
    print("=" * 60)
    print(f"Найдено таблиц: {table_count}")
    
    if table_count >= 8:
        print("[OK] База данных инициализирована корректно!")
    else:
        print("[WARNING] База данных не полностью инициализирована")
        print("  Выполните инициализацию через веб-интерфейс")
    
    # Проверяем наличие основных таблиц
    required_tables = [
        'sync_runs', 'datasets', 'zip_archives', 
        'xml_fragments', 'plans_raw', 'inspections_raw', 
        'operation_log'
    ]
    
    print("\nПроверка таблиц:")
    for table in required_tables:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table,))
        exists = cur.fetchone()['exists']
        status = "[OK]" if exists else "[MISSING]"
        print(f"  {status} {table}")
    
    cur.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print("Проверка завершена!")
    print("=" * 60)
    
except Exception as e:
    print(f"Ошибка: {e}")

