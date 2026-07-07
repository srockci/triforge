@echo off
REM ============================================================
REM Start TriForge server on Windows
REM ============================================================
REM Usage:
REM   start.bat [port]            default port: 8000
REM   set PORT=9000 && start.bat   custom port via env
REM   set TRIFORGE_VENV=C:\path    custom venv location
REM
REM Env vars:
REM   PORT             bind port        (default 8000)
REM   TRIFORGE_VENV    path to venv     (default %~dp0.venv)
REM   TRIFORGE_HOST    bind host        (default 127.0.0.1)
REM ============================================================

setlocal
set "PORT=%1"
if "%PORT%"=="" set "PORT=%PORT%"
if "%PORT%"=="" set "PORT=8000"
if not defined TRIFORGE_VENV set "TRIFORGE_VENV=%~dp0.venv"
if not defined TRIFORGE_HOST set "TRIFORGE_HOST=127.0.0.1"

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

if not exist "%TRIFORGE_VENV%\Scripts\python.exe" (
    echo [ERROR] venv not found at: %TRIFORGE_VENV%\Scripts\python.exe
    echo         Set TRIFORGE_VENV to your venv path, or run:  python -m venv .venv
    exit /b 1
)

echo Starting TriForge server on %TRIFORGE_HOST%:%PORT%...
echo Dashboard: http://%TRIFORGE_HOST%:%PORT%
echo venv:       %TRIFORGE_VENV%

echo Checking venv python... "%TRIFORGE_VENV%\Scripts\python.exe"
if not exist "%TRIFORGE_VENV%\Scripts\python.exe" (
    echo [ERROR] python.exe not found at "%TRIFORGE_VENV%\Scripts\python.exe"
    echo         Run: python -m venv .venv
    pause
    exit /b 1
)

"%TRIFORGE_VENV%\Scripts\python.exe" -c "import qrcode" 2>nul
if errorlevel 1 (
    echo [WARNING] qrcode package not found. Run: .venv\Scripts\python -m pip install -r requirements.txt
)

echo Server starting... open http://%TRIFORGE_HOST%:%PORT% in your browser.
echo Press Ctrl+C to stop the server. Close this window = server dies.
echo.

:: Log stdout+stderr to a file so we can see what happened after a crash
set "LOGFILE=%~dp0logs\server.log"
if not exist "%~dp0logs" mkdir "%~dp0logs"
echo [%DATE% %TIME%] Starting server... >> "%LOGFILE%"

:: ⚠️  Personal WeChat 通知强制 workers=1（ILinkGateway 长连线程）
:: 多 worker 下多进程抢占同一 bot_token 的 getupdates，iLink 只保留一个。
"%TRIFORGE_VENV%\Scripts\python.exe" -X utf8 -m uvicorn triforge_server.server:app --host %TRIFORGE_HOST% --port %PORT% --log-level info >> "%LOGFILE%" 2>&1
set EXIT_CODE=%ERRORLEVEL%

echo [%DATE% %TIME%] Server exited with code %EXIT_CODE% >> "%LOGFILE%"

if %EXIT_CODE% NEQ 0 (
    echo [ERROR] Server exited with code %EXIT_CODE%. See logs\server.log for details.
    type "%LOGFILE%"
    echo.
    echo Make sure .venv has all dependencies: .venv\Scripts\python -m pip install -r requirements.txt
    pause
    exit /b 1
)
echo [INFO] Server stopped normally.
pause
endlocal