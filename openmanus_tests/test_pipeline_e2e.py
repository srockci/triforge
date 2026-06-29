"""End-to-end pipeline test using a mock LLM server.

Steps:
  1. Start mock LLM server on :11435
  2. Set OPENMANUS_LLM_BASE_URL/MODEL so the server uses it
  3. Start the OpenManus FastAPI server on :8000
  4. POST /workflow/start — should pause at architecture.md approval
  5. POST /approve approve — should pause at src/hello.py approval
  6. POST /approve approve — should pause at review_report.md approval
  7. POST /approve approve — should be completed
  8. Verify all 3 files exist in workspace/
  9. Stop everything

Run:
    cd /root/openmanus-integration
    source .venv/bin/activate
    python -m tests.test_pipeline_e2e
"""
import asyncio
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import requests

WORKSPACE = Path("/root/openmanus-integration/workspace")
SERVER_URL = "http://127.0.0.1:8000"
MOCK_LLM_URL = "http://127.0.0.1:11435"


def cleanup_workspace():
    """Remove any files from previous runs."""
    for sub in ("design", "src", "tests"):
        d = WORKSPACE / sub
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)


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
        [sys.executable, "-m", "openmanus_tests.mock_llm_server"],
        cwd="/root/openmanus-integration",
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    if not wait_for(f"{MOCK_LLM_URL}/v1/chat/completions", timeout=10):
        # 404 is fine — we only need server to be listening
        try:
            requests.post(f"{MOCK_LLM_URL}/v1/chat/completions",
                          json={"messages": [{"role": "system", "content": ""}]},
                          timeout=2)
        except Exception:
            pass
    if not wait_for(MOCK_LLM_URL, timeout=5):
        # Try ping any path
        try:
            requests.get(MOCK_LLM_URL, timeout=2)
        except Exception:
            pass
    # Test it really responds
    try:
        r = requests.post(f"{MOCK_LLM_URL}/v1/chat/completions",
                          json={"model": "x", "messages": [
                              {"role": "system", "content": "you are architect. design phase."},
                              {"role": "user", "content": "do it"},
                          ]}, timeout=5)
        assert r.status_code == 200, f"mock LLM bad status: {r.status_code}"
        body = r.json()
        assert body["choices"][0]["message"].get("tool_calls"), "mock LLM didn't return tool_calls"
        print(f"✓ mock LLM up (test call returned {len(body['choices'][0]['message']['tool_calls'])} tool_calls)")
    except Exception as e:
        print(f"✗ mock LLM test failed: {e}")
        mock.terminate()
        return False

    # 2. Start the OpenManus server, with env pointing at mock LLM
    env = os.environ.copy()
    # Point both providers at mock LLM
    env["OPENMANUS_MINIMAX_BASE_URL"] = MOCK_LLM_URL + "/v1"
    env["OPENMANUS_DEEPSEEK_BASE_URL"] = MOCK_LLM_URL + "/v1"
    env["DEEPSEEK_API_KEY"] = "mock-key"
    env["MINIMAX_CN_API_KEY"] = "mock-key"  # any non-empty value
    # workspace
    env["OPENMANUS_WORKSPACE"] = str(WORKSPACE)

    server = subprocess.Popen(
        ["/root/openmanus-integration/.venv/bin/uvicorn",
         "openmanus_server.server:app",
         "--host", "127.0.0.1", "--port", "8000",
         "--log-level", "warning"],
        cwd="/root/openmanus-integration",
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        start_new_session=True,
    )

    if not wait_for(f"{SERVER_URL}/health"):
        print("✗ FastAPI server didn't come up")
        server.terminate()
        mock.terminate()
        return False
    print("✓ OpenManus FastAPI server up")

    try:
        # 3. Start workflow
        r = requests.post(f"{SERVER_URL}/workflow/start",
                          json={"requirement": "build a hello-world demo"}, timeout=10)
        r.raise_for_status()
        run_id = r.json()["run_id"]
        print(f"✓ workflow started: {run_id}")

        # 4-7. Loop: poll until awaiting_approval, then approve
        approval_count = 0
        # After approve, the agent continues the current phase. After that
        # phase finishes, the next phase begins and the next approval gate
        # fires. Each transition can take 1-3 seconds (LLM call + tool exec).
        # So after approve we wait longer before checking status again.
        just_approved = False
        for step in range(40):
            if just_approved:
                # Give the agent time to finish current phase + start next
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

        # 8. Verify files
        print("\n--- workspace contents ---")
        for sub in ("design", "src", "tests"):
            for p in sorted((WORKSPACE / sub).rglob("*")):
                if p.is_file():
                    size = p.stat().st_size
                    print(f"  {p.relative_to(WORKSPACE)}  ({size} bytes)")

        expected = [
            WORKSPACE / "design/architecture.md",
            WORKSPACE / "design/review_report.md",
            WORKSPACE / "src/hello.py",
            WORKSPACE / "tests/test_hello.py",
        ]
        missing = [str(p) for p in expected if not p.exists()]
        if missing:
            print(f"\n✗ MISSING FILES: {missing}")
            return False
        print(f"\n✓ all 4 expected files present")

        # Spot-check content
        arch = (WORKSPACE / "design/architecture.md").read_text()
        assert "Mock System Design" in arch, "architecture.md missing expected content"
        print("✓ architecture.md content verified")

        code = (WORKSPACE / "src/hello.py").read_text()
        assert "def greet" in code, "hello.py missing expected function"
        print("✓ hello.py content verified")

        review = (WORKSPACE / "design/review_report.md").read_text()
        assert "Mock Code Review" in review, "review_report.md missing expected content"
        print("✓ review_report.md content verified")

        return True

    finally:
        # Cleanup
        try:
            os.killpg(os.getpgid(server.pid), signal.SIGTERM)
            server.wait(timeout=3)
        except Exception:
            try: os.killpg(os.getpgid(server.pid), signal.SIGKILL)
            except: pass
        try:
            os.killpg(os.getpgid(mock.pid), signal.SIGTERM)
            mock.wait(timeout=3)
        except Exception:
            try: os.killpg(os.getpgid(mock.pid), signal.SIGKILL)
            except: pass


def main():
    ok = asyncio.run(drive())
    print("\n" + ("=" * 50))
    print("RESULT:", "✅ PASS" if ok else "❌ FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()