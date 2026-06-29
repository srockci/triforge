"""Agent runtime: thin wrapper around an OpenAI-compatible chat completion API.

Each Agent has:
  - a system prompt (role-specific)
  - a tool set (read_file, write_file, finish)
  - a generator that yields state events and accepts resume decisions

The Agent.step() method is a generator that yields between LLM calls and
tool executions. The workflow (workflow.py) drives it with .send(None) to
advance and catches pause events to surface to the user.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from openai import OpenAI

from .config import LLM_PROVIDERS, WORKSPACE_ROOT


# ---------------------------------------------------------------------------
# Tool schemas (OpenAI function-calling format)
# ---------------------------------------------------------------------------
TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read the contents of a file under the workspace.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path relative to workspace root, e.g. 'design/architecture.md'",
                    }
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Write content to a file under the workspace. Overwrites if exists.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path relative to workspace root, e.g. 'design/architecture.md'",
                    },
                    "content": {
                        "type": "string",
                        "description": "Full file content to write.",
                    },
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "finish",
            "description": "Call this when the task is complete. Provide a short summary.",
            "parameters": {
                "type": "object",
                "properties": {
                    "summary": {
                        "type": "string",
                        "description": "One-line summary of what was accomplished.",
                    }
                },
                "required": ["summary"],
            },
        },
    },
]


def _resolve_safe(rel_path: str) -> Path:
    """Resolve a workspace-relative path and confirm it stays under WORKSPACE_ROOT."""
    p = (WORKSPACE_ROOT / rel_path).resolve()
    if not str(p).startswith(str(WORKSPACE_ROOT)):
        raise ValueError(f"path escapes workspace: {rel_path}")
    return p


# ---------------------------------------------------------------------------
# Event types yielded by Agent.step()
# ---------------------------------------------------------------------------
@dataclass
class ToolCallEvent:
    """The agent wants to call a tool. Caller decides whether to approve."""
    tool: str
    args: Dict[str, Any]
    preview: str   # human-readable preview for the approval UI


@dataclass
class FinishEvent:
    """The agent called finish(). Done."""
    summary: str
    steps: int


@dataclass
class FailedEvent:
    """Agent failed (LLM error, max steps, etc.)."""
    error: str
    steps: int


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------
class Agent:
    """A role-specific LLM agent with file I/O tools."""

    def __init__(self, name: str, provider_key: str, system_prompt: str):
        self.name = name
        self.provider_key = provider_key
        self.system_prompt = system_prompt
        cfg = LLM_PROVIDERS[provider_key]
        api_key = os.environ.get(cfg["api_key_env"], "")
        if not api_key:
            raise RuntimeError(
                f"Missing env var {cfg['api_key_env']} for provider '{provider_key}'"
            )
        self.client = OpenAI(api_key=api_key, base_url=cfg["base_url"])
        self.model = cfg["model"]
        self.history: List[Dict[str, Any]] = [
            {"role": "system", "content": system_prompt}
        ]

    # ----- tool execution ---------------------------------------------------
    def _exec_tool(self, name: str, args: Dict[str, Any]) -> str:
        if name == "read_file":
            p = _resolve_safe(args["path"])
            if not p.exists():
                return f"[ERROR] file not found: {args['path']}"
            return p.read_text(encoding="utf-8", errors="replace")[:50_000]
        if name == "write_file":
            p = _resolve_safe(args["path"])
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(args["content"], encoding="utf-8")
            return f"[OK] wrote {len(args['content'])} bytes to {args['path']}"
        if name == "finish":
            return f"[FINISH] {args.get('summary', '')}"
        return f"[ERROR] unknown tool: {name}"

    # ----- the main loop (generator) ---------------------------------------
    def step(
        self,
        user_message: Optional[str] = None,
        max_steps: int = 12,
    ) -> Generator[Any, None, None]:
        """Generator yielding ToolCallEvent / FinishEvent / FailedEvent.

        If `user_message` is provided, it's appended to history first
        (use this for the FIRST call only).

        The generator returns when the agent finishes, fails, or hits
        max_steps. Caller iterates with `for ev in agent.step(...)` and
        handles each event.
        """
        if user_message is not None:
            self.history.append({"role": "user", "content": user_message})

        steps_used = 0
        for step_idx in range(max_steps):
            steps_used = step_idx + 1

            try:
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=list(self.history),
                    tools=TOOL_SCHEMAS,
                    tool_choice="auto",
                    max_tokens=4096,
                    temperature=0.2,
                )
            except Exception as e:
                yield FailedEvent(error=f"LLM call failed: {type(e).__name__}: {e}", steps=steps_used)
                return

            choice = resp.choices[0]
            msg = choice.message
            tool_calls_raw = msg.tool_calls or []

            if not tool_calls_raw:
                self.history.append({"role": "assistant", "content": msg.content or ""})
                if not (msg.content or "").strip():
                    self.history.append({
                        "role": "user",
                        "content": "[system] You must call a tool (write_file or finish) to make progress.",
                    })
                    continue
                continue

            tc_dicts = [
                {"name": tc.function.name, "arguments": _safe_json(tc.function.arguments)}
                for tc in tool_calls_raw
            ]
            self.history.append({
                "role": "assistant",
                "content": msg.content or "",
                "tool_calls": [
                    {
                        "id": f"call_{i}",
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": json.dumps(tc["arguments"], ensure_ascii=False),
                        },
                    }
                    for i, tc in enumerate(tc_dicts)
                ],
            })

            # Process each tool call. Special handling: finish short-circuits,
            # other tools yield ToolCallEvent and wait for next iteration to
            # see the tool result appended.
            for tc in tool_calls_raw:
                name = tc.function.name
                args = _safe_json(tc.function.arguments)

                if name == "finish":
                    # Record finish tool result for cleanliness, then signal done.
                    self.history.append({
                        "role": "tool",
                        "tool_call_id": f"call_{tool_calls_raw.index(tc)}",
                        "content": self._exec_tool(name, args),
                    })
                    yield FinishEvent(summary=args.get("summary", ""), steps=steps_used)
                    return

                # Execute the tool immediately (read_file is read-only,
                # safe; write_file will be confirmed by caller via event).
                # Actually we want to YIELD before executing writes so the
                # caller can approve. Read is safe to auto-execute.
                if name == "read_file":
                    result = self._exec_tool(name, args)
                    self.history.append({
                        "role": "tool",
                        "tool_call_id": f"call_{tool_calls_raw.index(tc)}",
                        "content": result,
                    })
                    continue

                # write_file: yield for approval, then resume
                preview = f"📝 write_file: {args.get('path', '?')}\n\n{(args.get('content', '') or '')[:600]}"
                yielded = yield ToolCallEvent(tool=name, args=args, preview=preview)
                # After caller decides: append tool result and continue.
                # 'yielded' will be the value sent via gen.send(decision) —
                # but with for-loop iteration, we use throw(StopIteration) and
                # just always proceed. To preserve sync approval semantics,
                # we use gen.send(None) to advance — see workflow.py.
                result = self._exec_tool(name, args)
                self.history.append({
                    "role": "tool",
                    "tool_call_id": f"call_{tool_calls_raw.index(tc)}",
                    "content": result,
                })

        yield FailedEvent(error=f"max_steps={max_steps} exceeded", steps=steps_used)


def _safe_json(s: str) -> Dict[str, Any]:
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s or "{}")
    except Exception:
        return {}


def make_agent(role: str, prompts: Dict[str, str]) -> Agent:
    """Factory: build the right agent for a pipeline role."""
    if role == "architect_design":
        return Agent(name="Architect-A", provider_key="minimax",
                     system_prompt=prompts["architect_design"])
    if role == "coder_implement":
        return Agent(name="Coder-B", provider_key="deepseek",
                     system_prompt=prompts["coder_implement"])
    if role == "architect_review":
        return Agent(name="Architect-A (review)", provider_key="minimax",
                     system_prompt=prompts["architect_review"])
    raise ValueError(f"unknown role: {role}")