"""Agent runtime: thin wrapper around an OpenAI-compatible chat completion API.

Each Agent has:
  - a system prompt (role-specific)
  - a tool set (read_file, write_file, finish)
  - a loop that: user_msg + history -> LLM -> tool_call OR finish

Approval gate: between agent steps, we pause and ask the workflow to confirm
before proceeding. The workflow resumes by calling agent.run(resume=True).
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    """Resolve a workspace-relative path and confirm it stays under WORKSPACE_ROOT.

    Prevents the LLM from writing anywhere outside the workspace via ../
    or absolute paths.
    """
    p = (WORKSPACE_ROOT / rel_path).resolve()
    if not str(p).startswith(str(WORKSPACE_ROOT)):
        raise ValueError(f"path escapes workspace: {rel_path}")
    return p


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------
@dataclass
class AgentStep:
    """One step in the agent loop. Persisted in the run history."""

    role: str               # "user" | "assistant" | "tool" | "system"
    content: str = ""
    tool_calls: Optional[List[Dict[str, Any]]] = None
    tool_name: Optional[str] = None
    tool_result: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


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
        self.history: List[AgentStep] = [AgentStep(role="system", content=system_prompt)]

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

    # ----- the main loop ---------------------------------------------------
    def run(
        self,
        user_message: str,
        max_steps: int = 12,
        on_tool_call: Optional[callable] = None,
    ) -> Dict[str, Any]:
        """Run the agent until it calls `finish` or hits max_steps.

        on_tool_call(tool_name, args) is invoked BEFORE the tool runs,
        so the workflow can pause for approval. Returning {"pause": True,
        "preview": "..."} from that callback stops the loop and returns a
        pause signal to the caller.
        """
        self.history.append(AgentStep(role="user", content=user_message))
        steps_used = 0
        for step_idx in range(max_steps):
            steps_used = step_idx + 1
            # Build messages for the API call
            msgs = []
            for h in self.history:
                if h.role == "system":
                    msgs.append({"role": "system", "content": h.content})
                elif h.role == "user":
                    msgs.append({"role": "user", "content": h.content})
                elif h.role == "assistant":
                    msg = {"role": "assistant", "content": h.content or ""}
                    if h.tool_calls:
                        msg["tool_calls"] = [
                            {
                                "id": f"call_{i}",
                                "type": "function",
                                "function": {
                                    "name": tc["name"],
                                    "arguments": json.dumps(tc["arguments"], ensure_ascii=False),
                                },
                            }
                            for i, tc in enumerate(h.tool_calls)
                        ]
                    msgs.append(msg)
                elif h.role == "tool":
                    msgs.append({
                        "role": "tool",
                        "tool_call_id": h.tool_name or "tool",
                        "content": h.tool_result or "",
                    })

            try:
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=msgs,
                    tools=TOOL_SCHEMAS,
                    tool_choice="auto",
                    max_tokens=4096,
                    temperature=0.2,
                )
            except Exception as e:
                return {"status": "failed", "error": f"LLM call failed: {type(e).__name__}: {e}"}

            choice = resp.choices[0]
            msg = choice.message
            tool_calls_raw = msg.tool_calls or []

            if not tool_calls_raw:
                # No tool call — assistant said something. Push it as content
                # and either loop (let it think again) or finish.
                self.history.append(AgentStep(role="assistant", content=msg.content or ""))
                # If the model didn't call a tool AND didn't really say anything
                # useful, nudge it to use a tool.
                if not (msg.content or "").strip():
                    self.history.append(AgentStep(
                        role="user",
                        content="[system] You must call a tool (write_file or finish) to make progress.",
                    ))
                    continue
                continue

            # Record assistant turn
            tc_dicts = [
                {"name": tc.function.name, "arguments": _safe_json(tc.function.arguments)}
                for tc in tool_calls_raw
            ]
            self.history.append(AgentStep(role="assistant", content=msg.content or "", tool_calls=tc_dicts))

            # Execute each tool call. Stop early if finish() or pause.
            pause_info = None
            for tc in tool_calls_raw:
                name = tc.function.name
                args = _safe_json(tc.function.arguments)
                # Approval hook
                if on_tool_call:
                    decision = on_tool_call(name, args)
                    if isinstance(decision, dict) and decision.get("pause"):
                        pause_info = {
                            "tool": name,
                            "args": args,
                            "preview": decision.get("preview", ""),
                            "step_index": step_idx,
                        }
                        break
                result = self._exec_tool(name, args)
                self.history.append(AgentStep(
                    role="tool",
                    tool_name=name,
                    tool_result=result,
                ))
                if name == "finish":
                    return {
                        "status": "finished",
                        "summary": args.get("summary", ""),
                        "steps": steps_used,
                        "history": [h.__dict__ for h in self.history[-20:]],
                    }

            if pause_info:
                return {
                    "status": "paused",
                    "tool": pause_info["tool"],
                    "args": pause_info["args"],
                    "preview": pause_info["preview"],
                    "step_index": pause_info["step_index"],
                }

        return {"status": "failed", "error": f"max_steps={max_steps} exceeded", "steps": steps_used}


def _safe_json(s: str) -> Dict[str, Any]:
    """Parse a tool-arguments string. Returns {} on failure."""
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s or "{}")
    except Exception:
        return {}


def make_agent(role: str, prompts: Dict[str, str]) -> Agent:
    """Factory: build the right agent for a pipeline role."""
    if role == "architect_design":
        return Agent(
            name="Architect-A",
            provider_key="minimax",
            system_prompt=prompts["architect_design"],
        )
    if role == "coder_implement":
        return Agent(
            name="Coder-B",
            provider_key="deepseek",
            system_prompt=prompts["coder_implement"],
        )
    if role == "architect_review":
        return Agent(
            name="Architect-A (review)",
            provider_key="minimax",
            system_prompt=prompts["architect_review"],
        )
    raise ValueError(f"unknown role: {role}")