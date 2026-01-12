# Инструкция по перезапуску сервисов ERKNM

## Если возникают ошибки, выполните следующие шаги:

### 1. Остановите все процессы Python
```powershell
Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force
```

### 2. Очистите кэш Python
```powershell
Get-ChildItem -Path . -Filter __pycache__ -Recurse -Directory | Remove-Item -Recurse -Force
```

### 3. Проверьте, что модули загружаются
```powershell
python -c "from erknm.browser.meta_downloader import download_meta_xml_browser; print('OK')"
```

### 4. Запустите веб-сервер заново
```powershell
python run_web.py
```

## Быстрый перезапуск (одной командой):

```powershell
Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force; Get-ChildItem -Path . -Filter __pycache__ -Recurse -Directory | Remove-Item -Recurse -Force; python run_web.py
```

## Текущие настройки:

- Задержка между файлами: 10-17 секунд
- Порции: по 3 файла
- Пауза между порциями: 30-60 секунд
- Retry: до 5 попыток







