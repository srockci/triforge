"""Mock LLM server for board e2e tests.

Serves OpenAI-compatible /v1/chat/completions on a configurable port.
Returns canned tool_calls based on system prompt role detection:
  - architect_design (first turn) -> write_file(design/architecture.md, "# X")
  - architect_design (after tool) -> finish("done")
  - module_code     (first turn)  -> write_file(src/hello.py, "# Y")
  - module_code     (after tool)  -> finish("done")
  - architect_review (first turn) -> write_file(design/review_report.md, "# Z")
  - architect_review (after tool)  -> finish("done")
  - module_detail   -> immediate finish("ok")
  - module_test     -> immediate finish("PASS")
"""
from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


def build_response(system: str, tool_msgs_so_far: int) -> dict:
    sys_l = (system or "").lower()
    # Role detection using whole-word matching to avoid
    # "implementation" / "implement" substring false positives.
    import re
    def has_word(text: str, word: str) -> bool:
        return bool(re.search(rf"\b{re.escape(word)}\b", text))

    is_finish_direct = False
    is_design   = has_word(sys_l, "architect") and has_word(sys_l, "design")
    is_coder    = has_word(sys_l, "module_code") or has_word(sys_l, "coder-b")
    is_detail   = has_word(sys_l, "module_detail")
    is_tester   = has_word(sys_l, "module_test")
    is_review   = has_word(sys_l, "architect") and has_word(sys_l, "review")

    if is_detail:
        path, content = "design/modules/mock.md", "# Module detail\n\nDesigned by mock."
        is_finish_direct = True
    elif is_tester:
        path, content = "PASS", ""
        is_finish_direct = True
    elif is_design:
        path, content = "design/architecture.md", "# Architecture\n\nDesigned by mock."
    elif is_coder:
        path, content = "src/hello.py", "# Coder output\nprint('hello')"
    elif is_review:
        path, content = "design/review_report.md", "# Review\n\nPASS — looks good."
    else:
        path, content = "design/architecture.md", "# Default\n\n"

    if is_finish_direct or tool_msgs_so_far > 0:
        args = {"summary": f"{path} written by mock"}
        tool_name = "finish"
    else:
        args = {"path": path, "content": content}
        tool_name = "write_file"

    tc = {"id": "c1", "type": "function",
          "function": {"name": tool_name, "arguments": json.dumps(args)}}
    msg = {"role": "assistant", "content": None, "tool_calls": [tc]}
    choice = {"index": 0, "finish_reason": "tool_calls", "message": msg}

    return {
        "id": "mock",
        "object": "chat.completion",
        "choices": [choice],
        "usage": {
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
        },
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a, **kw):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n).decode("utf-8")
        try:
            req = json.loads(body)
        except Exception:
            self.send_response(400)
            self.end_headers()
            return

        msgs = req.get("messages", [])
        system = (msgs[0].get("content") if msgs else "") or ""
        tool_msgs = sum(1 for m in msgs if m.get("role") == "tool")
        # DEBUG: print what we received
        resp = build_response(system, tool_msgs)
        data = json.dumps(resp).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    port = int(sys.argv[1])
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()