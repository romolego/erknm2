"""Вспомогательный скрипт для инициализации БД из bat-файла"""
import sys
import io

# Настройка кодировки для Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

try:
    from erknm.db.schema import init_schema
    
    result = init_schema()
    if result:
        print("[OK] База данных инициализирована")
        sys.exit(0)
    else:
        print("[WARNING] Проблема с инициализацией БД")
        sys.exit(1)
except Exception as e:
    print(f"[ERROR] Ошибка при инициализации БД: {str(e)}")
    sys.exit(1)








