"""End-to-end pipeline test using a mock LLM server.

Steps:
  1. Start mock LLM server
  2. Set TRIFORGE_LLM_BASE_URL/MODEL so the server uses it
  3. Start the TriForge FastAPI server
  4. POST /workflow/start — should pause at architecture.md approval
  5. POST /approve approve — should pause at src/hello.py approval
  6. POST /approve approve — should pause at review_report.md approval
  7. POST /approve approve — should be completed
  8. Verify all 3 files exist in workspace/<run_id>/
  9. Stop everything

Run:
    cd <project_root>
    .venv/Scripts/python -m triforge_tests.test_pipeline_e2e   # Windows
    .venv/bin/python -m triforge_tests.test_pipeline_e2e       # Linux/macOS
"""
import asyncio
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
WORKSPACE = ROOT / "workspace"
SERVER_PORT = int(os.environ.get("PORT", 8000))
MOCK_PORT = int(os.environ.get("MOCK_PORT", 11435))
SERVER_URL = f"http://127.0.0.1:{SERVER_PORT}"
MOCK_LLM_URL = f"http://127.0.0.1:{MOCK_PORT}"


def cleanup_workspace():
    """Remove any files from previous runs."""
    if WORKSPACE.exists():
        shutil.rmtree(WORKSPACE)
    WORKSPACE.mkdir(parents=True, exist_ok=True)


def wait_for(url: str, timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


async def drive():
    cleanup_workspace()
    print(f"✓ workspace cleaned: {WORKSPACE}")

    # 1. Start mock LLM
    mock = subprocess.Popen(
        [sys.executable, "-m", "triforge_tests.mock_llm_server", str(MOCK_PORT)],
        cwd=str(ROOT),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    # Wait for mock to be ready
    for _ in range(50):
        try:
            requests.post(f"{MOCK_LLM_URL}/v1/chat/completions",
                          json={"model": "x", "messages": [
                              {"role": "system", "content": "test"},
                          ]}, timeout=1)
            break
        except Exception:
            time.sleep(0.2)
    else:
        print("✗ mock LLM didn't come up")
        mock.terminate()
        return False

    # Verify mock
    try:
        r = requests.post(f"{MOCK_LLM_URL}/v1/chat/completions",
                          json={"model": "x", "messages": [
                              {"role": "system", "content": "you are architect. design phase."},
                              {"role": "user", "content": "do it"},
                          ]}, timeout=5)
        assert r.status_code == 200
        print(f"✓ mock LLM up")
    except Exception as e:
        print(f"✗ mock LLM test failed: {e}")
        mock.terminate()
        return False

    # 2. Start the TriForge server
    env = os.environ.copy()
    env["TRIFORGE_MINIMAX_BASE_URL"] = MOCK_LLM_URL + "/v1"
    env["TRIFORGE_DEEPSEEK_BASE_URL"] = MOCK_LLM_URL + "/v1"
    env["DEEPSEEK_API_KEY"] = "mock-key"
    env["MINIMAX_CN_API_KEY"] = "mock-key"
    env["TRIFORGE_WORKSPACE"] = str(WORKSPACE)

    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn",
         "triforge_server.server:app",
         "--host", "127.0.0.1", "--port", str(SERVER_PORT),
         "--log-level", "warning"],
        cwd=str(ROOT),
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )

    if not wait_for(f"{SERVER_URL}/health"):
        print("✗ FastAPI server didn't come up")
        server.terminate()
        mock.terminate()
        return False
    print("✓ TriForge FastAPI server up")

    try:
        # 3. Start workflow
        r = requests.post(f"{SERVER_URL}/workflow/start",
                          json={"requirement": "build a hello-world demo"}, timeout=10)
        r.raise_for_status()
        run_id = r.json()["run_id"]
        print(f"✓ workflow started: {run_id}")

        # Per-run workspace
        run_ws = WORKSPACE / run_id

        # 4-7. Loop: poll until awaiting_approval, then approve
        approval_count = 0
        just_approved = False
        for step in range(40):
            if just_approved:
                time.sleep(2.5)
                just_approved = False
            else:
                time.sleep(0.5)
            sr = requests.get(f"{SERVER_URL}/workflow/{run_id}/status", timeout=5)
            sr.raise_for_status()
            snap = sr.json()
            status = snap["status"]
            phase = snap["phase"]
            print(f"  step {step}: status={status} phase={phase}")

            if status == "awaiting_approval":
                pending = snap.get("pending_tool", "?")
                args = snap.get("pending_args", {})
                path = args.get("path", "?")
                approval_count += 1
                print(f"  → approval {approval_count}: {pending}({path})")
                ar = requests.post(f"{SERVER_URL}/workflow/{run_id}/approve",
                                   json={"decision": "approve"}, timeout=5)
                ar.raise_for_status()
                just_approved = True
                continue

            if status == "completed":
                print(f"  ✓ completed after {approval_count} approvals")
                break

            if status == "failed":
                print(f"  ✗ FAILED: {snap.get('error', 'unknown')}")
                return False

            # still running, keep polling
        else:
            print(f"  ✗ hit loop limit, last status: {status}")
            return False

        # 8. Verify files in per-run workspace
        print(f"\n--- workspace contents ({run_ws}) ---")
        for sub in ("design", "src", "tests"):
            sub_dir = run_ws / sub
            if sub_dir.exists():
                for p in sorted(sub_dir.rglob("*")):
                    if p.is_file():
                        size = p.stat().st_size
                        print(f"  {p.relative_to(run_ws)}  ({size} bytes)")

        expected = [
            run_ws / "design/architecture.md",
            run_ws / "design/review_report.md",
            run_ws / "src/hello.py",
        ]
        missing = [str(p) for p in expected if not p.exists()]
        if missing:
            print(f"\n✗ MISSING FILES: {missing}")
            return False
        print(f"\n✓ all 3 expected files present")

        arch = (run_ws / "design/architecture.md").read_text(encoding="utf-8")
        assert "Architecture" in arch
        print("✓ architecture.md content verified")

        code = (run_ws / "src/hello.py").read_text(encoding="utf-8")
        assert "hello" in code or "print" in code
        print("✓ hello.py content verified")

        review = (run_ws / "design/review_report.md").read_text(encoding="utf-8")
        assert "Review" in review or "PASS" in review
        print("✓ review_report.md content verified")

        return True

    finally:
        try:
            server.terminate()
            server.wait(timeout=5)
        except Exception:
            try: server.kill()
            except: pass
        try:
            mock.terminate()
            mock.wait(timeout=3)
        except Exception:
            try: mock.kill()
            except: pass


def main():
    ok = asyncio.run(drive())
    print("\n" + ("=" * 50))
    print("RESULT:", "✅ PASS" if ok else "❌ FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()