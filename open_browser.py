"""Скрипт для открытия браузера после запуска сервера"""
import time
import sys
import urllib.request
import urllib.error
import webbrowser

# Ждем пока сервер станет доступен
max_attempts = 30
url = "http://localhost:5000"

for i in range(max_attempts):
    try:
        response = urllib.request.urlopen(url, timeout=1)
        if response.status == 200:
            # Сервер доступен, открываем браузер
            webbrowser.open(url)
            sys.exit(0)
    except (urllib.error.URLError, OSError):
        # Сервер еще не доступен, ждем
        time.sleep(1)

# Если не удалось подключиться, все равно открываем браузер
webbrowser.open(url)
sys.exit(0)


