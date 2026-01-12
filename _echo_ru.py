"""Вспомогательный скрипт для вывода русского текста в batch файлах"""
import sys
import io

# Установка кодировки для консоли Windows
if sys.platform == 'win32':
    try:
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    except:
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
        except:
            pass

# Если есть аргументы командной строки, используем их
if len(sys.argv) > 1:
    output = ' '.join(sys.argv[1:])
    # Убираем кавычки если они есть с обеих сторон
    if output.startswith('"') and output.endswith('"'):
        output = output[1:-1]
    print(output)
    sys.stdout.flush()
# Иначе читаем из stdin
elif not sys.stdin.isatty():
    try:
        output = sys.stdin.read()
        print(output.rstrip('\n\r'))
        sys.stdout.flush()
    except:
        pass
