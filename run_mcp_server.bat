@echo off
REM ============================================================
REM Run the TriForge MCP server (stdio transport) on Windows.
REM ============================================================
REM Usage:
REM   run_mcp_server.bat [args...]
REM   set TRIFORGE_VENV=C:\path    custom venv location
REM ============================================================

setlocal
if not defined TRIFORGE_VENV set "TRIFORGE_VENV=%~dp0.venv"
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

if not exist "%TRIFORGE_VENV%\Scripts\python.exe" (
    echo [ERROR] venv not found at: %TRIFORGE_VENV%\Scripts\python.exe
    exit /b 1
)

"%TRIFORGE_VENV%\Scripts\python.exe" -X utf8 -m triforge_server.mcp_server %*
endlocal