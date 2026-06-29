#!/bin/bash
# Start the OpenManus FastAPI server in the background.
# Logs go to logs/server.log, PID written to logs/server.pid.
cd /root/openmanus-integration
mkdir -p logs
source .venv/bin/activate
nohup uvicorn openmanus_server.server:app --host 127.0.0.1 --port 8000 > logs/server.log 2>&1 &
echo $! > logs/server.pid
echo "OpenManus API server started, PID $(cat logs/server.pid)"
echo "Logs: tail -f logs/server.log"
sleep 1
curl -s http://127.0.0.1:8000/health