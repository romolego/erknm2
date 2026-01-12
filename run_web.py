"""Запуск веб-интерфейса"""
import sys
import io

# Настройка кодировки для Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from erknm.web.app import app

if __name__ == '__main__':
    print("=" * 60)
    print("Запуск веб-интерфейса ERKNM")
    print("=" * 60)
    print("\nВеб-интерфейс будет доступен по адресу:")
    print("   http://localhost:5000")
    print("\nДля остановки нажмите Ctrl+C")
    print("=" * 60)
    print()
    
    try:
        app.run(host='0.0.0.0', port=5000, debug=False)
    except Exception as e:
        print(f"Ошибка запуска сервера: {e}")
        import traceback
        traceback.print_exc()

