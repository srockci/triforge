"""P4: Persistence + restart-recovery test.

Strategy:
  1. Start mock + server
  2. Create a run, drive it to phase=implement awaiting_approval
  3. Kill server #1 (hard kill, simulating crash)
  4. Start server #2 with the same DB
  5. Verify:
     - GET /board/runs lists the run (now status="interrupted")
     - GET /board/runs/{id} returns detail with error message
     - GET /board/runs/{id}/events replays all historical events
     - POST /approve restarts the pipeline (returns "pipeline_restarted")
  6. Drive restarted pipeline to completion
"""
from __future__ import annotations

import json
import os
import shutil
import socket as _socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
WORKSPACE = ROOT / "workspace"
DB_PATH = ROOT / "data" / "board.db"
MOCK_PORT = int(os.environ.get("MOCK_PORT", 11520))
PORT = int(os.environ.get("PORT", 8800))


def cleanup_db_and_workspace():
    # Kill any leftover processes first
    kill_all()
    time.sleep(0.5)
    if DB_PATH.exists():
        try:
            DB_PATH.unlink()
        except PermissionError:
            # File locked on Windows — try harder
            kill_all()
            time.sleep(1)
            try:
                DB_PATH.unlink()
            except PermissionError:
                pass  # Will be overwritten by the server anyway
    if WORKSPACE.exists():
        shutil.rmtree(WORKSPACE, ignore_errors=True)
    WORKSPACE.mkdir(parents=True, exist_ok=True)
    for sub in ("design", "src", "tests"):
        (WORKSPACE / sub).mkdir(parents=True, exist_ok=True)


def kill_all():
    """Kill leftover server processes by name (cross-platform, best-effort)."""
    if sys.platform == "win32":
        # On Windows, use taskkill with window title filter (best-effort)
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq *uvicorn*"],
                capture_output=True, timeout=5,
            )
        except Exception:
            pass
    else:
        for proc_name in ("uvicorn", "mock_llm_server"):
            subprocess.run(["pkill", "-9", "-f", proc_name], capture_output=True)


def start_mock() -> subprocess.Popen:
    p = subprocess.Popen(
        [sys.executable, "-m", "triforge_tests.mock_llm_server", str(MOCK_PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=str(ROOT),
    )
    for _ in range(50):
        try:
            with _socket.create_connection(("127.0.0.1", MOCK_PORT), timeout=0.1):
                return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    raise RuntimeError("mock LLM did not start")


def start_server() -> subprocess.Popen:
    env = os.environ.copy()
    env["TRIFORGE_MINIMAX_BASE_URL"] = f"http://127.0.0.1:{MOCK_PORT}/v1"
    env["TRIFORGE_DEEPSEEK_BASE_URL"] = f"http://127.0.0.1:{MOCK_PORT}/v1"
    env["TRIFORGE_WORKSPACE"] = str(WORKSPACE)
    env["TRIFORGE_DB_PATH"] = str(DB_PATH)
    env["PYTHONPATH"] = str(ROOT)
    env["MINIMAX_CN_API_KEY"] = "mock-key-not-used"
    env["DEEPSEEK_API_KEY"] = "mock-key-not-used"
    p = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "triforge_server.server:app",
         "--host", "127.0.0.1", "--port", str(PORT),
         "--log-level", "warning", "--no-access-log"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        env=env, cwd=str(ROOT),
    )
    for _ in range(80):
        try:
            with _socket.create_connection(("127.0.0.1", PORT), timeout=0.1):
                return p
        except OSError:
            time.sleep(0.1)
    p.kill()
    out, _ = p.communicate(timeout=2)
    raise RuntimeError(f"server did not start. log:\n{out.decode()[:1500]}")


def http_json(method: str, path: str, body: dict = None) -> dict:
    url = f"http://127.0.0.1:{PORT}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if body else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def read_sse_events_replay_only(run_id: str, max_wait_s: float = 3.0) -> List[dict]:
    """Read SSE events; return after we've seen nothing new for 1s.

    Uses `requests` because stdlib http.client has chunked-encoding issues.
    """
    import requests
    url = f"http://127.0.0.1:{PORT}/board/runs/{run_id}/events"
    events: List[dict] = []
    buf = ""
    last_data_at = time.time()
    try:
        resp = requests.get(url, stream=True, timeout=2)
        for raw_line in resp.iter_lines(decode_unicode=True, chunk_size=1):
            if raw_line is None:
                if events and (time.time() - last_data_at) > 1.0:
                    break
                continue
            line = raw_line + "\n" if not raw_line.endswith("\n") else raw_line
            last_data_at = time.time()
            if line == "\n":
                if not buf.strip():
                    continue
                kind, data_str = "message", ""
                for ln in buf.split("\n"):
                    if ln.startswith("event: "):
                        kind = ln[7:].strip()
                    elif ln.startswith("data: "):
                        data_str += ln[6:]
                buf = ""
                if data_str:
                    try:
                        payload = json.loads(data_str)
                        events.append({"event": kind, "data": payload})
                        if payload.get("kind") == "run_end":
                            break
                    except json.JSONDecodeError:
                        pass
            elif line.startswith(":"):
                continue
            else:
                buf += line
            if time.time() - last_data_at > max_wait_s:
                break
        resp.close()
    except Exception as e:
        print(f"  ! SSE error: {e}", flush=True)
    return events


def wait_for_status(run_id: str, want_status: str, timeout_s: float = 30.0) -> dict:
    start = time.time()
    last: dict = {}
    while time.time() - start < timeout_s:
        last = http_json("GET", f"/board/runs/{run_id}")
        if last["status"] == want_status:
            return last
        if last["status"] == "failed":
            return last
        time.sleep(0.4)
    return last


def main() -> int:
    print("=" * 60, flush=True)
    print("P4: Persistence + restart-recovery test", flush=True)
    print("=" * 60, flush=True)

    cleanup_db_and_workspace()
    time.sleep(0.5)

    mock: subprocess.Popen = None
    server1: subprocess.Popen = None
    server2: subprocess.Popen = None

    try:
        # ===== Phase A =====
        print("\n[A] starting mock + server #1 ...", flush=True)
        mock = start_mock()
        server1 = start_server()
        print(f"  ✓ mock :{MOCK_PORT}, server :{PORT}", flush=True)

        created = http_json("POST", "/board/runs",
                            {"requirement": "P4 test: build a hello world API"})
        run_id = created["run_id"]
        print(f"  ✓ run created: {run_id}", flush=True)

        d = wait_for_status(run_id, "awaiting_approval", timeout_s=15)
        assert d["status"] == "awaiting_approval"
        assert d["phase"] == "design"
        print(f"  ✓ phase=design awaiting", flush=True)

        http_json("POST", f"/board/runs/{run_id}/approve",
                  {"decision": "approve", "comment": ""})

        d = wait_for_status(run_id, "awaiting_approval", timeout_s=15)
        if d["status"] != "awaiting_approval":
            print(f"  ! run ended early: {d['status']}", flush=True)
            return 1
        assert d["phase"] == "implement"
        print(f"  ✓ phase=implement awaiting", flush=True)

        print("\n[A] killing server #1 (hard kill) ...", flush=True)
        server1.kill()
        server1.wait(timeout=3)
        server1 = None

        # Verify DB state
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        n_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        snap = conn.execute(
            "SELECT status, phase FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        conn.close()
        print(f"  ✓ DB has {n_events} event(s) persisted", flush=True)
        assert n_events > 0
        assert snap[0] == "awaiting_approval"
        assert snap[1] == "implement"

        # ===== Phase B =====
        print("\n[B] starting server #2 (fresh process, same DB) ...", flush=True)
        server2 = start_server()
        time.sleep(2)
        try:
            with _socket.create_connection(("127.0.0.1", PORT), timeout=2):
                print(f"  ✓ server #2 up", flush=True)
        except Exception as e:
            print(f"  ✗ server #2 not up: {e}", flush=True)
            return 1

        # 1. Kanban lists restored run as "interrupted"
        runs_data = http_json("GET", "/board/runs")
        target = next((r for r in runs_data["runs"] if r["run_id"] == run_id), None)
        assert target is not None, "restored run not in /board/runs"
        print(f"  ✓ /board/runs lists restored run: status={target['status']}, "
              f"phase={target['phase']}", flush=True)
        assert target["status"] == "interrupted", \
            f"expected 'interrupted', got {target['status']!r}"
        assert target["phase"] == "implement"

        # 2. Detail endpoint shows error message
        detail = http_json("GET", f"/board/runs/{run_id}")
        assert detail["status"] == "interrupted"
        assert detail.get("error"), "interrupted run should have error message"
        print(f"  ✓ /board/runs/{{id}} shows interrupted: "
              f"error={detail['error'][:60]}...", flush=True)

        # 3. SSE replay — historical events from DB
        events = read_sse_events_replay_only(run_id, max_wait_s=3)
        kinds = [e["event"] for e in events]
        print(f"  ✓ /events replayed {len(events)} events: "
              f"{sorted(set(kinds))}", flush=True)
        assert "run_start" in kinds
        assert "phase_start" in kinds
        assert "approval_requested" in kinds
        assert "approval_resolved" in kinds

        # 4. Re-approve → pipeline restarts
        print("\n[B] re-approving interrupted run ...", flush=True)
        result = http_json("POST", f"/board/runs/{run_id}/approve",
                           {"decision": "approve", "comment": "retry"})
        assert result["status"] == "pipeline_restarted", \
            f"expected pipeline_restarted, got {result}"
        print(f"  ✓ pipeline restarted: {result}", flush=True)

        # 5. Drive restarted pipeline to completion
        completed_via_restart = False
        for n in range(4):
            d = wait_for_status(run_id, "awaiting_approval", timeout_s=20)
            if d["status"] == "completed":
                completed_via_restart = True
                print(f"  ✓ run completed (after {n+1} more approvals)",
                      flush=True)
                break
            if d["status"] == "failed":
                print(f"  ! run failed: {d.get('error')}", flush=True)
                return 1
            if d["status"] != "awaiting_approval":
                print(f"  ! unexpected status: {d['status']}", flush=True)
                return 1
            print(f"  ✓ next awaiting phase={d['phase']}", flush=True)
            http_json("POST", f"/board/runs/{run_id}/approve",
                      {"decision": "approve", "comment": ""})

        if not completed_via_restart:
            d = wait_for_status(run_id, "completed", timeout_s=10)
            assert d["status"] == "completed", f"final status is {d['status']}"
            print(f"  ✓ final status: completed", flush=True)

        # 6. DB state reflects completion
        conn = sqlite3.connect(DB_PATH)
        n_events_final = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        snap = conn.execute(
            "SELECT status FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        conn.close()
        print(f"  ✓ DB final: {n_events_final} events, status={snap[0]}",
              flush=True)
        assert snap[0] == "completed"

        print("\n" + "=" * 60, flush=True)
        print("RESULT: ✅ PASS — P4 persistence + restart recovery works",
              flush=True)
        print("=" * 60, flush=True)
        return 0

    except AssertionError as e:
        print(f"\n✗ ASSERTION FAILED: {e}", flush=True)
        return 1
    except Exception as e:
        print(f"\n✗ FAIL: {type(e).__name__}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return 1
    finally:
        for p in (server2, server1, mock):
            if p is None:
                continue
            try:
                p.kill()
                p.wait(timeout=3)
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())