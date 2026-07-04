#!/usr/bin/env bash
# ============================================================
# Stop the TriForge FastAPI server.
# ============================================================
# Usage:
#   ./stop.sh                     uses logs/server.pid
#   PORT=9000 ./stop.sh           match by port if no pidfile
#   ./stop.sh 9000                 same as PORT=9000 ./stop.sh
# ============================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$SCRIPT_DIR/logs/server.pid"
PORT="${PORT:-${1:-8000}}"

stopped=0

# Prefer the pidfile if present
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID" && echo "Stopped TriForge server (PID $PID)"
        stopped=1
    else
        echo "PID $PID not running"
    fi
    rm -f "$PID_FILE"
fi

# Fallback: kill anything bound to the port
if [ "$stopped" = "0" ]; then
    PIDS=$(lsof -nP -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print $2}' | sort -u)
    if [ -n "$PIDS" ]; then
        for PID in $PIDS; do
            kill "$PID" && echo "Killed PID $PID on port $PORT" && stopped=1
        done
    fi
fi

# Last resort: pkill
if [ "$stopped" = "0" ]; then
    pkill -f "uvicorn.*triforge_server" 2>/dev/null && echo "Killed via pkill" && stopped=1
fi

if [ "$stopped" = "0" ]; then
    echo "No server found"
fi