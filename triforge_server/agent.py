"""Agent runtime: thin wrapper around an OpenAI-compatible chat completion API.

Each Agent has:
  - a system prompt (role-specific)
  - a tool set (read_file, write_file, finish)
  - a generator that yields state events and accepts resume decisions
  - per-run workspace isolation (workspace_root passed at construction)
  - token usage tracking (tokens_in, tokens_out, cost_estimate)

The Agent.step() method is a generator that yields between LLM calls and
tool executions. The workflow (workflow.py) drives it with .send(None) to
advance and catches pause events to surface to the user.
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from openai import OpenAI

from .config import LLM_PROVIDERS


log = logging.getLogger("triforge.agent")


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


# Approximate cost per 1K tokens (input, output) in CNY.
# These are rough estimates; adjust to match your provider pricing.
COST_PER_1K = {
    "minimax":  (0.01, 0.01),   # MiniMax-Text-01
    "deepseek": (0.002, 0.008), # deepseek-chat
}


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


@dataclass
class TokenUsageEvent:
    """Token usage for a single LLM call. Yielded after each API response."""
    tokens_in: int
    tokens_out: int
    cost: float
    model: str
    provider_key: str = ""


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------
class Agent:
    """A role-specific LLM agent with file I/O tools and per-run workspace."""

    def __init__(self, name: str, provider_key: str, system_prompt: str,
                 workspace_root: Path,
                 provider_config: Optional[Dict[str, Any]] = None,
                 model: Optional[str] = None,
                 temperature: float = 0.2,
                 max_tokens: int = 4096):
        self.name = name
        self.provider_key = provider_key
        self.system_prompt = system_prompt
        self.workspace_root = workspace_root.resolve()

        # Use provided config or fall back to hardcoded LLM_PROVIDERS
        if provider_config is None:
            provider_config = LLM_PROVIDERS.get(provider_key, {})
        self.provider_config = provider_config

        base_url = provider_config.get("base_url", "")
        # API key: prefer direct value from settings, fall back to env var
        api_key = provider_config.get("api_key", "")
        if not api_key:
            api_key_env = provider_config.get("api_key_env", "")
            api_key = os.environ.get(api_key_env, "") if api_key_env else ""
        if not api_key:
            env_hint = provider_config.get("api_key_env", "")
            raise RuntimeError(
                f"Missing API key for provider '{provider_key}'. "
                f"Set it in Settings or env var {env_hint}"
            )

        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model = model or provider_config.get("model", "")
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.history: List[Dict[str, Any]] = [
            {"role": "system", "content": system_prompt}
        ]

        # Token usage accumulators
        self.total_tokens_in: int = 0
        self.total_tokens_out: int = 0
        self.total_cost: float = 0.0

    def save_state_to(self, run_id: str, phase: str, store: Any, steps_used: int) -> None:
        """Persist current history + step count for resume."""
        module_id = getattr(store, '_current_module_id', '')
        store.save_agent_state(run_id, phase, self.history, steps_used, module_id)

    def _resolve_safe(self, rel_path: str) -> Path:
        """Resolve a workspace-relative path and confirm it stays under workspace_root."""
        rel_path = self._normalize_rel_path(rel_path)
        p = (self.workspace_root / rel_path).resolve()
        if os.path.commonpath([str(p), str(self.workspace_root)]) != str(self.workspace_root):
            raise ValueError(f"path escapes workspace: {rel_path}")
        return p

    # ----- tool execution ---------------------------------------------------
    def _exec_tool(self, name: str, args: Dict[str, Any]) -> str:
        if name == "read_file":
            rel = (args.get("path") or "").strip()
            if not rel or rel in (".", "/", "\\"):
                return (f"[ERROR] read_file: path is empty or refers to "
                        f"the workspace root. Please specify a file path "
                        f"like 'src/app/main.py'.")
            p = self._resolve_safe(rel)
            if not p.exists():
                return f"[ERROR] file not found: {rel}"
            if p.is_dir():
                return (f"[ERROR] read_file: '{rel}' is a directory, not a "
                        f"file. Pick a specific file inside it.")
            _TEXT_EXTS = frozenset({
                ".py", ".md", ".txt", ".json", ".toml", ".yaml", ".yml",
                ".html", ".css", ".js", ".sh", ".bat", ".ps1", ".env",
                ".cfg", ".ini", ".conf", ".gitignore", ".dockerfile",
                ".sql", ".xml", ".rst",
            })
            if p.suffix.lower() not in _TEXT_EXTS:
                return (f"[ERROR] Cannot read '{rel}' (this model does not "
                        f"support image input). Inform the user.")
            return p.read_text(encoding="utf-8", errors="replace")[:50_000]
        if name == "write_file":
            rel = (args.get("path") or "").strip()
            if not rel or rel in (".", "/", "\\"):
                return (f"[ERROR] write_file: path is empty or refers to "
                        f"the workspace root. Please specify a file path "
                        f"like 'design/review_report.md' or 'src/app/foo.py'.")
            p = self._resolve_safe(rel)
            if p == self.workspace_root or p.is_dir():
                return (f"[ERROR] write_file: '{rel}' resolves to a "
                        f"directory. write_file must target a file, not a "
                        f"directory.")
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(args.get("content", ""), encoding="utf-8")
            return f"[OK] wrote {len(args.get('content', ''))} bytes to {rel}"
        if name == "finish":
            return f"[FINISH] {args.get('summary', '')}"
        return f"[ERROR] unknown tool: {name}"

    def _normalize_rel_path(self, rel_path: str) -> str:
        """Strip accidental `workspace/`, `./`, or absolute prefixes that
        some LLMs add to relative paths. The pipeline user_msg already
        warns against these, but defense in depth: we don't want the
        agent creating nested `workspace/src/...` directories."""
        if not rel_path:
            return rel_path
        p = rel_path.replace("\\", "/")
        # Drop leading "./"
        while p.startswith("./"):
            p = p[2:]
        # Drop leading "workspace/" — common LLM misread of "Workspace root"
        for prefix in ("workspace/", "Workspace/", "WORKSPACE/"):
            if p.startswith(prefix):
                p = p[len(prefix):]
                break
        # Drop leading "/"
        p = p.lstrip("/")
        return p

    def _track_usage(self, resp) -> Optional[TokenUsageEvent]:
        """Extract usage from an LLM response and accumulate totals."""
        usage = getattr(resp, "usage", None)
        if usage is None:
            return None
        t_in = getattr(usage, "prompt_tokens", 0) or 0
        t_out = getattr(usage, "completion_tokens", 0) or 0
        
        # Read token_plan_mode from settings (P4: provider_models_refactor)
        from .settings import get_settings
        mode = get_settings().get_model_token_plan_mode(self.provider_key, self.model)
        
        if mode == "token_plan":
            # Token-plan models: no cost calculation, only track token usage
            cost = 0.0
        elif mode == "free":
            cost = 0.0
        else:
            # Regular charge models: calculate cost based on rates
            rate_in, rate_out = COST_PER_1K.get(self.provider_key, (0.01, 0.01))
            cost = (t_in * rate_in + t_out * rate_out) / 1000.0

        self.total_tokens_in += t_in
        self.total_tokens_out += t_out
        self.total_cost += cost
        return TokenUsageEvent(
            tokens_in=t_in, tokens_out=t_out,
            cost=cost, model=self.model,
            provider_key=self.provider_key,
        )

    # ----- the main loop (generator) ---------------------------------------
    def step(
        self,
        user_message: Optional[str] = None,
        max_steps: int = 12,
    ) -> Generator[Any, None, None]:
        """Generator yielding ToolCallEvent / FinishEvent / FailedEvent / TokenUsageEvent.

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

            # Build per-call kwargs. Provider-specific knobs go in
            # extra_body so the OpenAI SDK passes them through verbatim
            # without complaining about unknown standard params.
            call_kwargs = dict(
                model=self.model,
                messages=list(self.history),
                tools=TOOL_SCHEMAS,
                tool_choice="auto",
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )
            # DeepSeek v4-flash (and friends) default to thinking mode.
            # For code generation that's pure overhead — disable it.
            # The reasoning_content round-trip (below) keeps the agent
            # robust if a caller later flips this back on or switches to
            # deepseek-reasoner.
            extra = {}
            if "deepseek" in (self.model or "").lower():
                extra["thinking"] = {"type": "disabled"}
            if extra:
                call_kwargs["extra_body"] = extra

            try:
                resp = self.client.chat.completions.create(**call_kwargs)
            except Exception as e:
                yield FailedEvent(error=f"LLM call failed: {type(e).__name__}: {e}", steps=steps_used)
                return

            # Yield token usage event right after the API call
            usage_ev = self._track_usage(resp)
            if usage_ev:
                yield usage_ev

            choice = resp.choices[0]
            msg = choice.message
            tool_calls_raw = msg.tool_calls or []

            # Some providers (notably DeepSeek reasoner) attach a private
            # `reasoning_content` field to assistant messages when thinking
            # mode is on. The provider documents that this field MUST be
            # echoed back verbatim in subsequent request history — if we
            # drop it, the next call returns 400:
            #   "The `reasoning_content` in the thinking mode must be
            #    passed back to the API."
            # Use getattr to read the optional attribute; non-thinking
            # providers (chat models) simply have it as None.
            reasoning_content = getattr(msg, "reasoning_content", None)

            if not tool_calls_raw:
                assistant_msg = {"role": "assistant", "content": msg.content or ""}
                if reasoning_content:
                    assistant_msg["reasoning_content"] = reasoning_content
                self.history.append(assistant_msg)
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
            assistant_msg = {
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
            }
            if reasoning_content:
                assistant_msg["reasoning_content"] = reasoning_content
            self.history.append(assistant_msg)

            # Process each tool call. Special handling: finish short-circuits,
            # other tools yield ToolCallEvent and wait for next iteration to
            # see the tool result appended.
            for tc in tool_calls_raw:
                name = tc.function.name
                args = _safe_json(tc.function.arguments)

                # Defensive: skip malformed tool calls. Some models (or
                # truncation cases) emit a placeholder tool_call with empty
                # arguments. Executing it would crash on args["path"] /
                # log a useless failure event. Better to inject a synthetic
                # tool result telling the model "your last call had no
                # arguments — try again with the actual fields" and let
                # the next loop iteration re-prompt.
                if not name or not isinstance(args, dict) or not args:
                    log.warning("skipping malformed tool_call name=%r args=%r",
                                name, args)
                    self.history.append({
                        "role": "tool",
                        "tool_call_id": f"call_{tool_calls_raw.index(tc)}",
                        "content": ("[system] Your tool call was malformed "
                                    "(empty or missing arguments). Please "
                                    "re-issue with the required fields."),
                    })
                    continue

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
                preview = f"write_file: {args.get('path', '?')}\n\n{(args.get('content', '') or '')[:600]}"
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


def make_agent_with_resume(role: str, workspace_root: Path,
                            settings: Optional[Dict[str, Any]] = None,
                            run_id: str = "", phase: str = "") -> tuple[Agent, int]:
    """Factory: build agent, try to load saved history from DB.

    Returns (agent, saved_steps):
        saved_steps=0: fresh start
        saved_steps>0: history loaded — caller should use resume hint
    """
    agent = make_agent(role, workspace_root, settings)
    if run_id and phase:
        from .store import get_store
        loaded = get_store().load_agent_state(run_id, phase)
        if loaded:
            history, saved_steps = loaded
            agent.history = history
            return agent, saved_steps
    return agent, 0


def make_agent(role: str, workspace_root: Path,
               settings: Optional[Dict[str, Any]] = None) -> Agent:
    """Factory: build an agent from settings config.

    If settings is None, falls back to hardcoded defaults from config.py.
    Environment variables TRIFORGE_{PROVIDER}_BASE_URL override provider URLs.
    """
    from .settings import DEFAULT_SETTINGS
    if settings is None:
        settings = DEFAULT_SETTINGS

    role_cfg = settings.get("roles", {}).get(role)
    if not role_cfg:
        raise ValueError(f"unknown role: {role}")

    provider_key = role_cfg.get("provider", "minimax")
    provider_cfg = dict(settings.get("providers", {}).get(provider_key, {}))

    # Apply env var overrides on provider base_url
    env_url_key = f"TRIFORGE_{provider_key.upper()}_BASE_URL"
    override_url = os.environ.get(env_url_key)
    if override_url:
        provider_cfg["base_url"] = override_url

    model = role_cfg.get("model", "")
    prompt = role_cfg.get("prompt", "")
    name = role_cfg.get("name", role)

    # Pipeline params for this role's phase
    phase_map = {
        "architect_design": "design",
        "architect_review": "review",
        "module_detail": "module",
        "module_code": "module",
        "module_test": "module",
    }
    phase = phase_map.get(role, "design")
    params = settings.get("pipeline_params", {}).get(phase, {})
    temperature = params.get("temperature", 0.2)
    max_tokens = params.get("max_tokens", 4096)

    return Agent(
        name=name,
        provider_key=provider_key,
        system_prompt=prompt,
        workspace_root=workspace_root,
        provider_config=provider_cfg,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )