# Скрипт для перезапуска сервисов ERKNM

Write-Host "========================================"
Write-Host "Перезапуск сервисов ERKNM"
Write-Host "========================================"

# Останавливаем все процессы Python
Write-Host "`n1. Остановка процессов Python..."
Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2
Write-Host "   [OK] Процессы остановлены"

# Очищаем кэш Python
Write-Host "`n2. Очистка кэша Python..."
Get-ChildItem -Path . -Filter __pycache__ -Recurse -Directory | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "   [OK] Кэш очищен"

# Проверяем модули
Write-Host "`n3. Проверка модулей..."
python -c "from erknm.browser.meta_downloader import download_meta_xml_browser; from erknm.sync.synchronizer import sync; print('   [OK] Модули загружены')" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "   [ERROR] Ошибка загрузки модулей"
    exit 1
}

Write-Host "`n========================================"
Write-Host "Готово! Теперь запустите веб-сервер:"
Write-Host "   python run_web.py"
Write-Host "========================================"








