"""Проверка наличия колонки stop_requested"""
from erknm.db.connection import get_connection, get_cursor

conn = get_connection()
cur = get_cursor(conn)

try:
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='sync_runs' AND column_name='stop_requested'
    """)
    result = cur.fetchone()
    if result:
        print("OK: Column 'stop_requested' exists")
    else:
        print("ERROR: Column 'stop_requested' does not exist")
        print("Adding column...")
        cur.execute("ALTER TABLE sync_runs ADD COLUMN stop_requested BOOLEAN NOT NULL DEFAULT FALSE")
        conn.commit()
        print("OK: Column added successfully")
finally:
    cur.close()
    conn.close()








