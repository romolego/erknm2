@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

REM ============================================================
REM ERKNM - Starting Services (with PostgreSQL autostart)
REM ============================================================

REM --- Auto-elevate: открыть НОВОЕ админ-окно и продолжить там (чтобы окно не "пропадало")
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Requesting Administrator privileges...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath 'cmd.exe' -Verb RunAs -ArgumentList '/k',('""%~f0""')"
    exit /b 0
)

echo ========================================
echo ERKNM - Starting Services
echo ========================================
echo.

echo [0/8] Starting PostgreSQL...

REM 0.1) Start all PostgreSQL services (if any)
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='SilentlyContinue';" ^
  "$svcs = Get-Service | Where-Object { $_.Name -match 'postgres' -or $_.DisplayName -match 'PostgreSQL' };" ^
  "foreach($s in $svcs){" ^
  "  try { Set-Service -Name $s.Name -StartupType Manual } catch {}" ^
  "  if($s.Status -ne 'Running'){" ^
  "    Write-Host ('  - starting service: ' + $s.Name);" ^
  "    try { Start-Service -Name $s.Name } catch {}" ^
  "  }" ^
  "}" ^
  "exit 0"

REM 0.2) Start portable instance D:\programms\PosgreSQL (if exists)
set "PGCTL_D=D:\programms\PosgreSQL\bin\pg_ctl.exe"
set "PGDATA_D=D:\programms\PosgreSQL\data"
if exist "%PGCTL_D%" (
    if exist "%PGDATA_D%\PG_VERSION" (
        "%PGCTL_D%" status -D "%PGDATA_D%" >nul 2>&1
        if errorlevel 1 (
            echo   - starting portable cluster: %PGDATA_D%
            "%PGCTL_D%" start -D "%PGDATA_D%" -w -t 15 -l "%TEMP%\erknm_pg_d.log"
            if errorlevel 1 (
                echo [ERROR] Failed to start portable PostgreSQL (see %TEMP%\erknm_pg_d.log)
                goto :FAIL
            )
        ) else (
            echo   - portable cluster already running: %PGDATA_D%
        )
    )
)

REM 0.3) Verify postgres.exe exists
powershell -NoProfile -Command "if (Get-Process -Name postgres -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] PostgreSQL is not running after start attempt.
    goto :FAIL
)
echo [OK] PostgreSQL is running
echo.

echo [1/8] Checking Python...
python --version
if errorlevel 1 (
    echo [ERROR] Python not found in PATH
    goto :FAIL
)
echo.

echo [2/8] Checking virtual environment...
if exist "venv\Scripts\activate.bat" (
    echo [INFO] Found venv, activating...
    call venv\Scripts\activate.bat
    if errorlevel 1 goto :FAIL
    echo [OK] Virtual environment activated
) else if exist ".venv\Scripts\activate.bat" (
    echo [INFO] Found .venv, activating...
    call .venv\Scripts\activate.bat
    if errorlevel 1 goto :FAIL
    echo [OK] Virtual environment activated
) else (
    echo [INFO] No virtual environment found, using system Python
)
echo.

echo [3/8] Checking dependencies...
python -c "import flask, psycopg2, playwright, lxml, requests, click, schedule, dotenv"
if errorlevel 1 (
    echo [WARNING] Some dependencies missing, installing...
    python -m pip install --upgrade pip
    if errorlevel 1 goto :FAIL
    python -m pip install -r requirements.txt
    if errorlevel 1 goto :FAIL
    python -m playwright install chromium
    if errorlevel 1 goto :FAIL
    echo [OK] Dependencies installed
) else (
    echo [OK] Dependencies OK
)
echo.

echo [4/8] Stopping previous web server on :5000...
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr :5000 ^| findstr LISTENING') do (
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 1 /nobreak >nul
echo [OK] Done
echo.

echo [5/8] Cleaning Python cache...
for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d" >nul 2>&1
echo [OK] Cache cleaned
echo.

echo [6/8] Checking project modules...
python -c "from erknm.browser.meta_downloader import download_meta_xml_browser; from erknm.sync.synchronizer import sync; from erknm.db.schema import init_schema"
if errorlevel 1 (
    echo [ERROR] Failed to load modules
    goto :FAIL
)
echo [OK] Modules loaded
echo.

echo [7/8] Initializing database...
python init_db.py
if errorlevel 1 (
    echo [WARNING] Database init failed (see output above)
) else (
    echo [OK] Database ready
)
echo.

echo ========================================
echo Starting web server...
echo ========================================
echo Web interface: http://localhost:5000
echo Press Ctrl+C to stop
echo ========================================
echo.

start "" /B python open_browser.py

python run_web.py
set "RC=%ERRORLEVEL%"
echo.
echo [INFO] Web server exited with code: %RC%
pause
exit /b %RC%

:FAIL
echo.
echo [FAIL] Startup failed. Window will stay open.
pause
exit /b 1
