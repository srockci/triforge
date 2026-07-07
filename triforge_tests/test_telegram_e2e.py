"""E2E test for Telegram bidirectional approval.

Requires mock LLM server + mock Telegram server (started automatically).

Usage:
    python triforge_tests/test_telegram_e2e.py
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

ROOT = Path(__file__).resolve().parent.parent
WORKSPACE = ROOT / "workspace"
DB_PATH = ROOT / "data" / "board.db"
MOCK_PORT = int(os.environ.get("MOCK_PORT", 11520))
TG_PORT = int(os.environ.get("TG_PORT", 8801))
MOCK_TG_PORT = int(os.environ.get("MOCK_TG_PORT", 11522))

TEST_BOT_TOKEN = "test-bot-token-12345:ABC-DEF"
TEST_CHAT_ID = 67890
TEST_USER_ID = 12345


def cleanup():
    kill_all()
    time.sleep(0.5)
    if DB_PATH.exists():
        try:
            DB_PATH.unlink()
        except PermissionError:
            pass
    if WORKSPACE.exists():
        shutil.rmtree(WORKSPACE, ignore_errors=True)
    WORKSPACE.mkdir(parents=True, exist_ok=True)
    for sub in ("design", "src", "tests"):
        (WORKSPACE / sub).mkdir(parents=True, exist_ok=True)


def kill_all():
    if sys.platform == "win32":
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq *uvicorn*"],
                capture_output=True, timeout=5)
        except Exception:
            pass
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq *mock_llm*"],
                capture_output=True, timeout=5)
        except Exception:
            pass
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq *mock_telegram*"],
                capture_output=True, timeout=5)
        except Exception:
            pass
    else:
        for name in ("uvicorn", "mock_llm_server", "mock_telegram_server"):
            subprocess.run(["pkill", "-9", "-f", name], capture_output=True)


def start_mock_llm():
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
    raise RuntimeError("mock LLM did not start")


def start_mock_telegram():
    p = subprocess.Popen(
        [sys.executable, "-m", "triforge_tests.mock_telegram_server", str(MOCK_TG_PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=str(ROOT),
    )
    for _ in range(50):
        try:
            with _socket.create_connection(("127.0.0.1", MOCK_TG_PORT), timeout=0.1):
                return p
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("mock Telegram did not start")


def start_server():
    env = os.environ.copy()
    env["TRIFORGE_MINIMAX_BASE_URL"] = f"http://127.0.0.1:{MOCK_PORT}/v1"
    env["TRIFORGE_DEEPSEEK_BASE_URL"] = f"http://127.0.0.1:{MOCK_PORT}/v1"
    env["TRIFORGE_WORKSPACE"] = str(WORKSPACE)
    env["TRIFORGE_DB_PATH"] = str(DB_PATH)
    env["TRIFORGE_PORT"] = str(TG_PORT)
    env["PYTHONPATH"] = str(ROOT)
    env["MINIMAX_CN_API_KEY"] = "mock-key"
    env["DEEPSEEK_API_KEY"] = "mock-key"
    p = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "triforge_server.server:app",
         "--host", "127.0.0.1", "--port", str(TG_PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=str(ROOT), env=env,
    )
    for _ in range(100):
        try:
            with _socket.create_connection(("127.0.0.1", TG_PORT), timeout=0.1):
                time.sleep(2)
                return p
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def http_json(method, path, body=None):
    url = f"http://127.0.0.1:{TG_PORT}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"status": "error", "code": e.code, "body": e.read().decode()[:500]}


def tg_get(path):
    url = f"http://127.0.0.1:{MOCK_TG_PORT}{path}"
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read())


def tg_post(path, body):
    url = f"http://127.0.0.1:{MOCK_TG_PORT}{path}"
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def wait_for(predicate, timeout_s=15, interval=0.5):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError(f"timeout: {predicate.__name__}")


def setup_telegram_channel():
    settings = http_json("GET", "/board/settings")
    settings["notification_channels"] = [
        {
            "type": "telegram",
            "enabled": True,
            "mode": "complex",
            "bot_token": TEST_BOT_TOKEN,
            "chat_id": str(TEST_CHAT_ID),
            "polling_mode": True,
            "allowed_user_ids": str(TEST_USER_ID),
        }
    ]
    settings["public_url"] = "http://localhost:8800"
    http_json("POST", "/board/settings", settings)


def test_approve_button_callback():
    print("\n[1] test_approve_button_callback ...", flush=True)
    cleanup()
    llm = start_mock_llm()
    tg_server = start_mock_telegram()
    server = start_server()
    setup_telegram_channel()

    run_id = http_json("POST", "/board/runs",
                       {"requirement": "test approve button"})["run_id"]

    wait_for(lambda: http_json("GET", f"/board/runs/{run_id}")["status"] == "awaiting_approval",
             timeout_s=20)

    wait_for(lambda: len(tg_get("/test/sent_messages")) > 0, timeout_s=10)
    sent = tg_get("/test/sent_messages")[-1]
    assert sent.get("keyboard"), "expected inline_keyboard"
    assert len(sent["keyboard"]) >= 2, "expected at least 2 keyboard rows"

    approve_btn = None
    for row in sent["keyboard"]:
        for btn in row:
            if "Approve" in btn.get("text", ""):
                approve_btn = btn
                break
    assert approve_btn, "Approve button not found"
    assert "callback_data" in approve_btn, "Approve must use callback_data"

    callback_data = approve_btn["callback_data"]

    tg_post("/test/push_update", {
        "update_id": 100,
        "callback_query": {
            "id": "cb_001",
            "from": {"id": TEST_USER_ID, "first_name": "alice"},
            "data": callback_data,
            "message": {
                "chat": {"id": TEST_CHAT_ID},
                "message_id": sent["message_id"],
            },
        },
    })

    def got_callback_answer():
        ans = tg_get("/test/callback_answers")
        return any(a["id"] == "cb_001" for a in ans)
    wait_for(got_callback_answer, timeout_s=10)

    def got_edit():
        edits = tg_get("/test/edits")
        return len(edits) > 0 and "Approved" in edits[0]["text"]
    wait_for(got_edit, timeout_s=10)

    final = http_json("GET", f"/board/runs/{run_id}")
    assert final["status"] in ("running", "completed"), \
        f"expected running/completed, got {final['status']}"
    print(f"  \u2713 approved \u2192 {final['status']}", flush=True)

    server.kill()
    server.wait()
    tg_server.kill()
    tg_server.wait()
    llm.kill()
    llm.wait()


def test_reply_then_text():
    print("\n[2] test_reply_then_text ...", flush=True)
    cleanup()
    llm = start_mock_llm()
    tg_server = start_mock_telegram()
    server = start_server()
    setup_telegram_channel()

    run_id = http_json("POST", "/board/runs",
                       {"requirement": "test reply flow"})["run_id"]
    wait_for(lambda: http_json("GET", f"/board/runs/{run_id}")["status"] == "awaiting_approval",
             timeout_s=20)
    wait_for(lambda: len(tg_get("/test/sent_messages")) > 0, timeout_s=10)
    sent = tg_get("/test/sent_messages")[-1]

    reply_btn = next(b for row in sent["keyboard"] for b in row if "Reply" in b.get("text", ""))
    reply_data = reply_btn["callback_data"]

    tg_post("/test/push_update", {
        "update_id": 200,
        "callback_query": {
            "id": "cb_reply",
            "from": {"id": TEST_USER_ID},
            "data": reply_data,
            "message": {"chat": {"id": TEST_CHAT_ID}, "message_id": sent["message_id"]},
        },
    })
    wait_for(lambda: any(a["id"] == "cb_reply" for a in tg_get("/test/callback_answers")),
             timeout_s=10)

    tg_post("/test/push_update", {
        "update_id": 201,
        "message": {
            "from": {"id": TEST_USER_ID},
            "chat": {"id": TEST_CHAT_ID},
            "text": "looks OK",
        },
    })

    wait_for(lambda: http_json("GET", f"/board/runs/{run_id}")["status"]
             in ("running", "completed"), timeout_s=15)

    def has_comment_edit():
        for e in tg_get("/test/edits"):
            if "looks OK" in e["text"]:
                return True
        return False
    wait_for(has_comment_edit, timeout_s=10)
    print("  \u2713 reply + text \u2192 approved with comment", flush=True)

    server.kill()
    server.wait()
    tg_server.kill()
    tg_server.wait()
    llm.kill()
    llm.wait()


def test_unauthorized_user():
    print("\n[3] test_unauthorized_user ...", flush=True)
    cleanup()
    llm = start_mock_llm()
    tg_server = start_mock_telegram()
    server = start_server()

    settings = http_json("GET", "/board/settings")
    settings["notification_channels"] = [{
        "type": "telegram", "enabled": True, "mode": "complex",
        "bot_token": TEST_BOT_TOKEN, "chat_id": str(TEST_CHAT_ID),
        "polling_mode": True, "allowed_user_ids": "99999",
    }]
    http_json("POST", "/board/settings", settings)

    run_id = http_json("POST", "/board/runs",
                       {"requirement": "test unauthorized"})["run_id"]
    wait_for(lambda: http_json("GET", f"/board/runs/{run_id}")["status"] == "awaiting_approval",
             timeout_s=20)
    wait_for(lambda: len(tg_get("/test/sent_messages")) > 0, timeout_s=10)
    sent = tg_get("/test/sent_messages")[-1]
    approve_btn = next(b for row in sent["keyboard"] for b in row if "Approve" in b.get("text", ""))

    tg_post("/test/push_update", {
        "update_id": 300,
        "callback_query": {
            "id": "cb_unauth",
            "from": {"id": TEST_USER_ID},
            "data": approve_btn["callback_data"],
            "message": {"chat": {"id": TEST_CHAT_ID}, "message_id": sent["message_id"]},
        },
    })

    def got_unauth():
        ans = tg_get("/test/callback_answers")
        return any(a["id"] == "cb_unauth" and "Unauthorized" in a["text"] for a in ans)
    wait_for(got_unauth, timeout_s=10)
    print("  \u2713 unauthorized \u2192 '\u26d4 Unauthorized user'", flush=True)

    server.kill()
    server.wait()
    tg_server.kill()
    tg_server.wait()
    llm.kill()
    llm.wait()


def main():
    print("=" * 60)
    print("E2E: Telegram bidirectional approval")
    print("=" * 60)

    tests = [
        test_approve_button_callback,
        test_reply_then_text,
        test_unauthorized_user,
    ]

    passed, failed = 0, 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"  \u2717 {test.__name__}: {type(e).__name__}: {e}", flush=True)
            import traceback
            traceback.print_exc()

    print(f"\n{'=' * 60}", flush=True)
    print(f"RESULT: {passed} passed, {failed} failed", flush=True)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
