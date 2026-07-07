#!/usr/bin/env bash
# ============================================================
# Start the TriForge FastAPI server in the background.
# ============================================================
# Usage:
#   ./start.sh [port]            default port: 8000
#   PORT=9000 ./start.sh         custom port via env
#   TRIFORGE_VENV=/path ./start.sh   custom venv location
#
# Env vars:
#   PORT             bind port        (default 8000)
#   TRIFORGE_VENV    path to venv     (default <script_dir>/.venv)
#   TRIFORGE_HOST    bind host        (default 127.0.0.1)
# ============================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${PORT:-${1:-8000}}"
TRIFORGE_VENV="${TRIFORGE_VENV:-$SCRIPT_DIR/.venv}"
TRIFORGE_HOST="${TRIFORGE_HOST:-127.0.0.1}"

mkdir -p logs

# Activate the venv if it exists; otherwise expect python to already be on PATH.
if [ -f "$TRIFORGE_VENV/bin/activate" ]; then
    # shellcheck disable=SC1091
    source "$TRIFORGE_VENV/bin/activate"
    PYTHON_BIN="$TRIFORGE_VENV/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
else
    echo "[ERROR] No venv at $TRIFORGE_VENV and no python3 on PATH" >&2
    exit 1
fi

export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8

echo "Starting TriForge server on $TRIFORGE_HOST:$PORT..."
echo "Dashboard:  http://$TRIFORGE_HOST:$PORT"
echo "venv:        $TRIFORGE_VENV"

# ⚠️  Personal WeChat 通知强制 workers=1（ILinkGateway 长连线程）
# 多 worker 下多进程抢占同一 bot_token 的 getupdates，iLink 只保留一个。
# 改 workers 之前请阅读 docs/ilink_protocol.md。
nohup "$PYTHON_BIN" -X utf8 -m uvicorn triforge_server.server:app \
    --host "$TRIFORGE_HOST" --port "$PORT" \
    > logs/server.log 2>&1 &

echo $! > logs/server.pid
PID=$(cat logs/server.pid)
echo "PID:         $PID"
echo "Logs:        tail -f logs/server.log"
sleep 1
curl -s "http://$TRIFORGE_HOST:$PORT/health" || true
echo