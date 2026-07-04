"""End-to-end test for the board API (P1).

Spawns mock_llm_server + TriForge FastAPI server, then drives a real
pipeline through the /board/* endpoints including:
  - POST /board/runs
  - GET  /board/runs (kanban list)
  - GET  /board/runs/{id} (detail)
  - GET  /board/runs/{id}/events (SSE)
  - POST /board/runs/{id}/approve (3x for 3 phases)
  - GET  /board/runs/{id}/files + /files/{path}
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path
from threading import Thread
from typing import List

ROOT = Path(__file__).resolve().parent.parent
WORKSPACE = ROOT / "workspace"
MOCK_PORT = int(os.environ.get("MOCK_PORT", 11450))
PORT = int(os.environ.get("PORT", 8770))


def start_mock() -> subprocess.Popen:
    p = subprocess.Popen(
        [sys.executable, "-m", "triforge_tests.mock_llm_server", str(MOCK_PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=str(ROOT),
    )
    for _ in range(50):
        try:
            with socket.create_connection(("127.0.0.1", MOCK_PORT), timeout=0.1):
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
    env["PYTHONPATH"] = str(ROOT)
    # Mock LLM doesn't check keys but Agent.__init__ requires non-empty
    env["MINIMAX_CN_API_KEY"] = "mock-key-not-used"
    env["DEEPSEEK_API_KEY"] = "mock-key-not-used"
    p = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "triforge_server.server:app",
         "--host", "127.0.0.1", "--port", str(PORT), "--log-level", "warning",
         "--no-access-log"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        env=env, cwd=str(ROOT),
    )
    for _ in range(80):
        try:
            with socket.create_connection(("127.0.0.1", PORT), timeout=0.1):
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


def read_sse_events(run_id: str, deadline_s: float) -> List[dict]:
    """Connect to SSE and yield events until run_end or deadline.

    Uses readline() rather than read(1) — Python's http.client has
    known issues with chunked transfer encoding + 1-byte reads.
    """
    url = f"http://127.0.0.1:{PORT}/board/runs/{run_id}/events"
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, timeout=2)
    events: List[dict] = []
    buf = ""
    start = time.time()
    while time.time() - start < deadline_s:
        try:
            line_bytes = resp.readline()
        except (socket.timeout, TimeoutError):
            line_bytes = b""
        if not line_bytes:
            time.sleep(0.1)
            if any(e["data"].get("kind") == "run_end" for e in events):
                return events
            continue
        line = line_bytes.decode("utf-8", errors="replace")
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
                        return events
                except json.JSONDecodeError:
                    pass
        elif line.startswith(":"):
            continue
        else:
            buf += line
    return events


def wait_for_status(run_id: str, want_status: str, timeout_s: float = 30.0) -> dict:
    """Poll /board/runs/{id} until status matches. Returns last detail dict."""
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
    print("Board API e2e test (P1)", flush=True)
    print("=" * 60, flush=True)

    if WORKSPACE.exists():
        shutil.rmtree(WORKSPACE)
    WORKSPACE.mkdir(parents=True, exist_ok=True)
    for sub in ("design", "src", "tests"):
        (WORKSPACE / sub).mkdir(parents=True, exist_ok=True)

    print("\n[setup] starting mock LLM ...", flush=True)
    mock = start_mock()
    print(f"  ✓ mock on :{MOCK_PORT}", flush=True)

    print("\n[setup] starting FastAPI server ...", flush=True)
    server = start_server()
    print(f"  ✓ server on :{PORT}", flush=True)

    sse_events: List[dict] = []

    try:
        # ---- 1. Health ----
        h = http_json("GET", "/health")
        assert h["status"] == "ok"
        print(f"\n[1] ✓ /health: status={h['status']}", flush=True)

        # ---- 2. Create run ----
        created = http_json("POST", "/board/runs",
                            {"requirement": "Board e2e test: hello world API"})
        run_id = created["run_id"]
        assert created["status"] == "started"
        print(f"\n[2] ✓ created run via /board/runs: {run_id}", flush=True)

        # ---- 3. Start SSE collector in background ----
        def sse_worker():
            sse_events.extend(read_sse_events(run_id, deadline_s=60))
        Thread(target=sse_worker, daemon=True).start()
        print("\n[3] ✓ SSE collector started", flush=True)

        # ---- 4. Wait for first approval (design phase) ----
        d = wait_for_status(run_id, "awaiting_approval", timeout_s=20)
        if d["status"] != "awaiting_approval":
            print(f"  ✗ never hit awaiting_approval: status={d['status']} "
                  f"error={d.get('error')}", flush=True)
            return 1
        print(f"\n[4] ✓ awaiting_approval: phase={d['phase']}, "
              f"pending={d.get('pending', {}).get('args', {}).get('path')}",
              flush=True)

        # ---- 5. Verify list and detail ----
        lst = http_json("GET", "/board/runs")
        assert any(r["run_id"] == run_id for r in lst["runs"])
        target = next(r for r in lst["runs"] if r["run_id"] == run_id)
        print(f"\n[5] ✓ run visible in /board/runs: "
              f"status={target['status']}, phase={target['phase']}",
              flush=True)

        detail = http_json("GET", f"/board/runs/{run_id}")
        assert detail["status"] == "awaiting_approval"
        assert detail.get("pending") and detail["pending"].get("args")
        print(f"  ✓ detail: pending preview={len(detail['pending']['preview'])} chars, "
              f"files={len(detail.get('files', []))}", flush=True)

        # ---- 6. Approve 3 times (design -> implement -> review) ----
        for n in range(3):
            print(f"\n[6.{n+1}] approving ...", flush=True)
            result = http_json("POST", f"/board/runs/{run_id}/approve",
                               {"decision": "approve", "comment": ""})
            assert result["status"] == "decision_submitted"
            print(f"  ✓ approved: {result}", flush=True)
            if n < 2:
                d = wait_for_status(run_id, "awaiting_approval", timeout_s=25)
                if d["status"] != "awaiting_approval":
                    print(f"  ! run ended early: {d['status']}", flush=True)
                    if d["status"] == "failed":
                        return 1
                    break
                print(f"  next awaiting: phase={d['phase']}", flush=True)

        # ---- 7. Wait for completion ----
        d = wait_for_status(run_id, "completed", timeout_s=25)
        if d["status"] != "completed":
            print(f"\n[7] ✗ run did not complete: status={d['status']} "
                  f"error={d.get('error')}", flush=True)
            return 1
        print(f"\n[7] ✓ run completed: phase={d['phase']}, "
              f"outputs={list(d.get('outputs', {}).keys())}", flush=True)

        # ---- 8. Verify SSE captured events ----
        time.sleep(1.0)
        kinds_seen = [e["event"] for e in sse_events]
        ctr = Counter(kinds_seen)
        print(f"\n[8] SSE event counts: {dict(ctr)}", flush=True)
        required = ("run_start", "phase_start", "tool_call",
                    "approval_requested", "approval_resolved", "run_end")
        missing = [k for k in required if k not in ctr]
        if missing:
            print(f"  ✗ missing events: {missing}", flush=True)
            print(f"  last events: {kinds_seen[-10:]}", flush=True)
            return 1
        print(f"  ✓ all {len(required)} event types present "
              f"(total events: {len(sse_events)})", flush=True)

        # Verify run_end payload carries outputs
        run_end = next(e for e in sse_events if e["event"] == "run_end")
        end_data = run_end["data"].get("data", {})
        assert end_data.get("status") == "completed"
        assert "outputs" in end_data
        print(f"  ✓ run_end payload: status={end_data['status']}, "
              f"outputs keys={list(end_data['outputs'].keys())}", flush=True)

        # ---- 9. Verify file endpoints ----
        fl = http_json("GET", f"/board/runs/{run_id}/files")
        paths = [f["path"] for f in fl["files"]]
        print(f"\n[9] ✓ /board/runs/{run_id}/files: {len(paths)} files", flush=True)
        for required_path in ("design/architecture.md", "src/hello.py",
                              "design/review_report.md"):
            assert any(required_path in p for p in paths), \
                f"missing: {required_path}"
        print(f"  ✓ all required files present", flush=True)

        # Read architecture content
        arch = http_json("GET", f"/board/runs/{run_id}/files/design/architecture.md")
        assert "Architecture" in arch["content"]
        print(f"  ✓ architecture.md: {arch['size']} bytes, content verified",
              flush=True)

        # ---- 10. Path-traversal protection ----
        print(f"\n[10] testing path traversal protection ...", flush=True)
        try:
            http_json("GET", f"/board/runs/{run_id}/files/../../../etc/passwd")
            print(f"  ✗ path traversal NOT blocked!", flush=True)
            return 1
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f"  ✓ path traversal blocked (404)", flush=True)
            else:
                print(f"  ? path traversal returned {e.code}", flush=True)

        # ---- 11. Detail endpoint with completed state ----
        print(f"\n[11] verifying detail in completed state ...", flush=True)
        d = http_json("GET", f"/board/runs/{run_id}")
        assert d["status"] == "completed"
        assert d["phase"] == "done"
        assert d["outputs"].get("design_doc")
        assert d["outputs"].get("code_files")
        assert d["outputs"].get("review_report")
        print(f"  ✓ outputs: design_doc + "
              f"{len(d['outputs']['code_files'])} code files + review_report",
              flush=True)

        print("\n" + "=" * 60, flush=True)
        print("RESULT: ✅ PASS — Board API end-to-end works", flush=True)
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
        try:
            server.terminate()
            server.wait(timeout=5)
        except Exception:
            server.kill()
        try:
            mock.terminate()
            mock.wait(timeout=3)
        except Exception:
            mock.kill()


if __name__ == "__main__":
    sys.exit(main())