"""Mock LLM server for end-to-end pipeline testing.

Listens on http://127.0.0.1:11435/v1/chat/completions. Returns canned
tool_calls based on the agent's role (read from system prompt) and the
current conversation state. Lets us exercise the full A->B->A pipeline
without needing real API keys.

Behaviour:
  - Architect-A first turn: emits write_file(design/architecture.md, "# Mock Design ...")
  - Architect-A next turns: emits finish()
  - Coder-B first turn: emits write_file(src/hello.py, "print('hello')")
  - Coder-B next turns: emits write_file(tests/test_hello.py, "...") then finish()
  - Architect-A (review): emits write_file(design/review_report.md) then finish()
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


DESIGN_CONTENT = """# Mock System Design

## 1. Overview
A tiny demo system for end-to-end pipeline testing.

## 2. Tech Stack
- Python 3.11

## 3. Modules
- hello: prints a greeting

## 4. Data Flow
None.

## 5. File Layout
- src/hello.py
- tests/test_hello.py

## 6. Acceptance Criteria
- `python src/hello.py` prints "hello"
"""


CODE_CONTENT = '''"""Hello module."""


def greet(name: str = "world") -> str:
    """Return a greeting."""
    return f"hello, {name}"


if __name__ == "__main__":
    print(greet())
'''


TEST_CONTENT = '''"""Tests for hello module."""
from hello import greet


def test_greet_default():
    assert greet() == "hello, world"


def test_greet_named():
    assert greet("alice") == "hello, alice"
'''


REVIEW_CONTENT = """# Mock Code Review Report

## Architecture Consistency
PASS — file layout matches.

## Module Completeness
PASS — hello module present.

## Code Quality
PASS — type annotations, docstrings present.

## Test Coverage
PASS — test_hello.py covers both branches.

## Security
PASS — no secrets.

## Verdict
PASS
"""


def make_response(model: str, content: str = None, tool_calls: list = None) -> dict:
    msg = {"role": "assistant", "content": content or ""}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    return {
        "id": "mock-1",
        "object": "chat.completion",
        "model": model,
        "choices": [{
            "index": 0,
            "message": msg,
            "finish_reason": "tool_calls" if tool_calls else "stop",
        }],
        "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
    }


def tc(tool_call_id: str, name: str, args: dict) -> dict:
    return {
        "id": tool_call_id,
        "type": "function",
        "function": {
            "name": name,
            "arguments": json.dumps(args, ensure_ascii=False),
        },
    }


def pick_turn(system_prompt: str, history: list) -> dict:
    """Decide which tool_calls to emit based on agent role + step index.

    Step index = number of tool messages already in history. After the
    first tool execution, the next LLM turn should advance (finish or
    next file write), not repeat the same write.
    """
    tool_messages_so_far = sum(1 for m in history if m.get("role") == "tool")

    sys_lower = system_prompt.lower()
    # Order matters: check the most specific role first.
    # "review" appears only in architect_review; "implement"/"developer"
    # appears only in coder; "design" appears in BOTH architect_design
    # and coder (since coder reads architecture.md), so check coder first.
    is_review = "code review" in sys_lower or ("review report" in sys_lower)
    is_coder = ("python developer" in sys_lower
                or "implement the code" in sys_lower
                or "implement code" in sys_lower)
    is_design = "design the system architecture" in sys_lower or "design phase" in sys_lower

    if is_coder:
        # 0 -> src/hello.py. 1 -> tests/test_hello.py. 2+ -> finish.
        if tool_messages_so_far == 0:
            return make_response("mock", tool_calls=[
                tc("call_1", "write_file", {"path": "src/hello.py", "content": CODE_CONTENT})
            ])
        if tool_messages_so_far == 1:
            return make_response("mock", tool_calls=[
                tc("call_2", "write_file", {"path": "tests/test_hello.py", "content": TEST_CONTENT})
            ])
        return make_response("mock", tool_calls=[
            tc("call_3", "finish", {"summary": "Code complete (mock)"})
        ])

    if is_review:
        # 0 -> write review. 1+ -> finish.
        if tool_messages_so_far == 0:
            return make_response("mock", tool_calls=[
                tc("call_1", "write_file", {"path": "design/review_report.md", "content": REVIEW_CONTENT})
            ])
        return make_response("mock", tool_calls=[
            tc("call_2", "finish", {"summary": "Review complete (mock)"})
        ])

    if is_design:
        # 0 tool msgs -> write architecture. 1+ tool msgs -> finish.
        if tool_messages_so_far == 0:
            return make_response("mock", tool_calls=[
                tc("call_1", "write_file", {"path": "design/architecture.md", "content": DESIGN_CONTENT})
            ])
        return make_response("mock", tool_calls=[
            tc("call_2", "finish", {"summary": "Design complete (mock)"})
        ])

    # Unknown — finish immediately
    return make_response("mock", tool_calls=[
        tc("call_x", "finish", {"summary": "Unknown role, finishing"})
    ])


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # silence default access log
        pass

    def do_POST(self):
        if not self.path.startswith("/v1/chat/completions"):
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length))
        msgs = body.get("messages", [])
        system = next((m["content"] for m in msgs if m["role"] == "system"), "")
        resp = pick_turn(system, msgs)
        data = json.dumps(resp).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    srv = HTTPServer(("127.0.0.1", 11435), Handler)
    print("Mock LLM listening on http://127.0.0.1:11435")
    srv.serve_forever()