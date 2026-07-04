@echo off
REM ============================================================
REM Stop the TriForge FastAPI server on Windows.
REM ============================================================
REM Usage:
REM   stop.bat [port]                default port: 8000
REM   set PORT=9000 && stop.bat      custom port
REM
REM Kills any process listening on the given port (default 8000).
REM Also pkill-falls-back to tasklist for orphan python processes.
REM ============================================================

setlocal
set "PORT=%1"
if "%PORT%"=="" set "PORT=%PORT%"
if "%PORT%"=="" set "PORT=8000"

echo Stopping TriForge server on port %PORT%...

REM Kill anything bound to that port
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":%PORT%" ^| findstr "LISTENING"') do (
    echo   Killing PID %%a ...
    taskkill /F /PID %%a >nul 2>&1
)

echo Done.
endlocal