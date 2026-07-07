"""Mock iLink server for testing ILinkGateway and downstream changes.

Usage:
    python triforge_tests/mock_ilink_server.py [--port 8999] [--fail-first N]

The `--fail-first N` flag makes the first N getupdates calls return 503,
so tests can exercise the RECONNECTING state machine path.

Run the real server alongside app tests:
    from triforge_tests.mock_ilink_server import run_mock_ilink
    proc = run_mock_ilink(port=8999, fail_first=3)
    ... run your test with TRIFORGE_ILINK_BASE_URL=http://127.0.0.1:8999 ...
    proc.kill()
"""
from __future__ import annotations

import json
import logging
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from typing import Optional

log = logging.getLogger("mock_ilink")


class MockILinkHandler(BaseHTTPRequestHandler):
    """Minimal HTTP/1.1 server that speaks the iLink subset we depend on."""

    # ── class-level state (shared across handler instances for one process) ──
    fail_first: int = 0
    _call_count: int = 0
    _sendmessage_log: list[dict] = []
    _get_qrcode_status: str = "wait"

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/ilink/bot/get_bot_qrcode":
            self._handle_get_bot_qrcode()
        elif path == "/ilink/bot/get_qrcode_status":
            self._handle_get_qrcode_status()
        elif path == "/ilink/bot/getupdates":
            self._handle_getupdates()
        else:
            self._json(404, {"error": f"unknown path: {path}"})

    def do_POST(self):
        body = self._read_body()
        path = self.path.split("?")[0]
        if path == "/ilink/bot/sendmessage":
            self._handle_sendmessage(body)
        else:
            self._json(404, {"error": f"unknown path: {path}"})

    # ── internal helpers ──

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw)
        except ValueError:
            return {}

    def _json(self, status: int, data: dict):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _empty_200(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ── endpoint handlers ──

    def _handle_get_bot_qrcode(self):
        self._json(200, {
            "qrcode": "mock-qr-123",
            "qrcode_img_content": "https://liteapp.weixin.qq.com/q/mock?qrcode=mock-qr-123",
        })

    def _handle_get_qrcode_status(self):
        status = MockILinkHandler._get_qrcode_status
        if status == "confirmed":
            self._json(200, {
                "status": "confirmed",
                "bot_token": "mock-bot-token-abc",
                "ilink_bot_id": "mock-user@wechat",
                "baseurl": "http://127.0.0.1:8999",
            })
        elif status == "expired":
            self._json(200, {"status": "expired"})
        else:
            self._json(200, {"status": "wait"})

    def _handle_getupdates(self):
        MockILinkHandler._call_count += 1
        if MockILinkHandler.fail_first > 0 and \
                MockILinkHandler._call_count <= MockILinkHandler.fail_first:
            log.info("mock getupdates #%d → 503 (fail-first)", MockILinkHandler._call_count)
            self._json(503, {"error": "simulated failure"})
            return
        # Block for up to 25s (simulate long-poll), but respond quickly to
        # keep tests fast. Real iLink long-polls for ~30s — we send empty
        # updates after 0.5s for most tests.
        try:
            time.sleep(0.5)
        except BaseException:
            pass
        self._json(200, {"updates": []})

    def _handle_sendmessage(self, body: dict):
        MockILinkHandler._sendmessage_log.append(body)
        self._empty_200()

    def log_message(self, fmt, *args):
        log.debug(fmt, *args)


def run_mock_ilink(port: int = 8999, fail_first: int = 0) -> Thread:
    """Start the mock server in a daemon thread.

    Returns the thread handle; call thread.join() or just let it die with
    the process.
    """
    MockILinkHandler.fail_first = fail_first
    MockILinkHandler._call_count = 0
    MockILinkHandler._sendmessage_log.clear()
    server = HTTPServer(("127.0.0.1", port), MockILinkHandler)
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("mock iLink server listening on 127.0.0.1:%d", port)
    return t, server


def reset_mock_state():
    """Reset mock counters and send log (for test isolation)."""
    MockILinkHandler._call_count = 0
    MockILinkHandler._sendmessage_log.clear()
    MockILinkHandler._get_qrcode_status = "wait"


def pop_sendmessage_log() -> list[dict]:
    """Return and clear the sendmessage log."""
    log = list(MockILinkHandler._sendmessage_log)
    MockILinkHandler._sendmessage_log.clear()
    return log


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Mock iLink server")
    parser.add_argument("--port", type=int, default=8999)
    parser.add_argument("--fail-first", type=int, default=0,
                        help="First N getupdates calls return 503")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    MockILinkHandler.fail_first = args.fail_first
    server = HTTPServer(("127.0.0.1", args.port), MockILinkHandler)
    print(f"mock iLink listening on 127.0.0.1:{args.port} "
          f"(fail_first={args.fail_first})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
