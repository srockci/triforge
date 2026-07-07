"""Mock api.telegram.org for telegram bidirectional e2e test."""

from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class State:
    sent_messages = []
    edits = []
    callback_answers = []
    update_queue = []
    webhook_url = None
    webhook_secret = None
    next_message_id = 1000
    lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a, **kw):
        pass

    def _reply(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(n).decode()) if n else {}
        path = self.path

        if "/test/push_update" in path:
            State.update_queue.append(body)
            self._reply(200, {"ok": True})
            return

        if "/sendMessage" in path:
            with State.lock:
                mid = State.next_message_id
                State.next_message_id += 1
                keyboard = body.get("reply_markup")
                if keyboard:
                    keyboard = json.loads(keyboard) if isinstance(keyboard, str) else keyboard
                State.sent_messages.append({
                    "chat_id": body.get("chat_id"),
                    "text": body.get("text"),
                    "keyboard": keyboard.get("inline_keyboard") if keyboard else None,
                    "message_id": mid,
                })
            self._reply(200, {"ok": True, "result": {"message_id": mid}})
            return

        if "/editMessageText" in path:
            with State.lock:
                State.edits.append({
                    "chat_id": body.get("chat_id"),
                    "message_id": body.get("message_id"),
                    "text": body.get("text"),
                })
            self._reply(200, {"ok": True, "result": True})
            return

        if "/answerCallbackQuery" in path:
            with State.lock:
                State.callback_answers.append({
                    "id": body.get("callback_query_id"),
                    "text": body.get("text"),
                })
            self._reply(200, {"ok": True, "result": True})
            return

        if "/getUpdates" in path:
            with State.lock:
                upd = State.update_queue.pop(0) if State.update_queue else {}
            self._reply(200, {"ok": True, "result": [upd] if upd else []})
            return

        if "/setWebhook" in path:
            with State.lock:
                State.webhook_url = body.get("url")
                State.webhook_secret = body.get("secret_token")
            self._reply(200, {"ok": True, "result": True})
            return

        if "/deleteWebhook" in path:
            with State.lock:
                State.webhook_url = None
                State.webhook_secret = None
            self._reply(200, {"ok": True, "result": True})
            return

        self._reply(404, {"ok": False, "error": "not found"})

    def do_GET(self):
        if "/test/sent_messages" in self.path:
            with State.lock:
                msgs = list(State.sent_messages)
            self._reply(200, msgs)
            return
        if "/test/edits" in self.path:
            with State.lock:
                edits = list(State.edits)
            self._reply(200, edits)
            return
        if "/test/callback_answers" in self.path:
            with State.lock:
                ans = list(State.callback_answers)
            self._reply(200, ans)
            return
        self._reply(404, {"ok": False, "error": "not found"})


def main():
    port = int(sys.argv[1])
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
