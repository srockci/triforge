#!/usr/bin/env bash
# ============================================================
# Run the TriForge MCP server (stdio transport).
# ============================================================
# Usage:
#   ./run_mcp_server.sh [args...]
#   TRIFORGE_VENV=/path ./run_mcp_server.sh
# ============================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
TRIFORGE_VENV="${TRIFORGE_VENV:-$SCRIPT_DIR/.venv}"

if [ -x "$TRIFORGE_VENV/bin/python" ]; then
    PYTHON_BIN="$TRIFORGE_VENV/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
else
    echo "[ERROR] No venv at $TRIFORGE_VENV and no python3 on PATH" >&2
    exit 1
fi

export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8
exec "$PYTHON_BIN" -X utf8 -m triforge_server.mcp_server "$@"