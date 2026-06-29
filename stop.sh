#!/bin/bash
# Stop the OpenManus FastAPI server.
PID_FILE=/root/openmanus-integration/logs/server.pid
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "Stopped OpenManus server (PID $PID)"
    else
        echo "PID $PID not running"
    fi
    rm -f "$PID_FILE"
else
    pkill -f "uvicorn.*openmanus_server" 2>/dev/null && echo "Killed via pkill" || echo "No server found"
fi