"""Workflow orchestrator: drives the A -> B -> A pipeline with approval gates.

Uses Agent.step() generator. The async pipeline iterates events from a
generator running in a worker thread (because the LLM call is sync). When
the generator yields a ToolCallEvent that needs approval, we surface it
to the API, await a user decision, then send(None) into the generator
to resume.
"""
from __future__ import annotations

import asyncio
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .agent import Agent, FinishEvent, FailedEvent, ToolCallEvent, make_agent
from .config import AGENT_PROMPTS, WORKSPACE_ROOT


def _should_request_approval(phase: str, tool: str, args: Dict[str, Any]) -> bool:
    if tool != "write_file":
        return False
    path = args.get("path", "")
    if phase == "design":
        return path.endswith("architecture.md")
    if phase == "implement":
        return path.startswith("src/") and path.endswith(".py")
    if phase == "review":
        return path.endswith("review_report.md")
    return False


@dataclass
class RunState:
    run_id: str
    requirement: str
    phase: str = "design"
    status: str = "running"
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    pending_tool: Optional[str] = None
    pending_args: Optional[Dict[str, Any]] = None
    pending_preview: str = ""
    outputs: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    history: List[Dict[str, Any]] = field(default_factory=list)
    resume_event: asyncio.Event = field(default_factory=asyncio.Event)
    resume_decision: str = "pending"
    resume_comment: str = ""


class WorkflowEngine:
    def __init__(self):
        self.runs: Dict[str, RunState] = {}

    def create(self, requirement: str) -> RunState:
        run = RunState(
            run_id=f"run_{uuid.uuid4().hex[:10]}",
            requirement=requirement,
        )
        self.runs[run.run_id] = run
        return run

    def get(self, run_id: str) -> Optional[RunState]:
        return self.runs.get(run_id)

    def submit_decision(self, run_id: str, decision: str, comment: str = "") -> bool:
        run = self.runs.get(run_id)
        if not run or run.status != "awaiting_approval":
            return False
        run.resume_decision = decision
        run.resume_comment = comment
        run.resume_event.set()
        return True


engine = WorkflowEngine()


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
async def run_pipeline_async(run: RunState, prompts: Dict[str, str] = AGENT_PROMPTS) -> None:
    """Drive a single run through design -> implement -> review."""
    try:
        # ----- Phase 1: DESIGN -----
        run.phase = "design"
        run.status = "running"
        architect = make_agent("architect_design", prompts)
        design_msg = (
            f"User requirement:\n{run.requirement}\n\n"
            f"Workspace root: {WORKSPACE_ROOT}\n"
            f"Write your architecture design to: design/architecture.md "
            f"(relative to workspace root)."
        )
        result = await _drive_agent(run, architect, design_msg, max_steps=12)
        if not result.get("ok"):
            run.status = "failed"
            run.error = result.get("error", "design phase did not finish")
            return
        run.outputs["design_doc"] = str(WORKSPACE_ROOT / "design/architecture.md")
        run.history.append({"phase": "design", "summary": result.get("summary", "")})

        # ----- Phase 2: IMPLEMENT -----
        run.phase = "implement"
        coder = make_agent("coder_implement", prompts)
        impl_msg = (
            f"User requirement:\n{run.requirement}\n\n"
            f"Read the architecture from: design/architecture.md\n"
            f"Implement code under src/ and tests under tests/.\n"
            f"Workspace root: {WORKSPACE_ROOT}"
        )
        result = await _drive_agent(run, coder, impl_msg, max_steps=25)
        if not result.get("ok"):
            run.status = "failed"
            run.error = result.get("error", "implement phase did not finish")
            return
        src_files = sorted(str(p.relative_to(WORKSPACE_ROOT))
                          for p in (WORKSPACE_ROOT / "src").rglob("*.py"))
        test_files = sorted(str(p.relative_to(WORKSPACE_ROOT))
                           for p in (WORKSPACE_ROOT / "tests").rglob("test_*.py"))
        run.outputs["code_files"] = src_files + test_files
        run.history.append({"phase": "implement", "summary": result.get("summary", "")})

        # ----- Phase 3: REVIEW -----
        run.phase = "review"
        reviewer = make_agent("architect_review", prompts)
        review_msg = (
            f"Original user requirement:\n{run.requirement}\n\n"
            f"Review everything under workspace/ "
            f"(design at design/architecture.md, code at src/, tests at tests/).\n"
            f"Write your report to: design/review_report.md\n"
            f"Workspace root: {WORKSPACE_ROOT}"
        )
        result = await _drive_agent(run, reviewer, review_msg, max_steps=12)
        if not result.get("ok"):
            run.status = "failed"
            run.error = result.get("error", "review phase did not finish")
            return
        run.outputs["review_report"] = str(WORKSPACE_ROOT / "design/review_report.md")
        run.history.append({"phase": "review", "summary": result.get("summary", "")})

        run.status = "completed"
        run.phase = "done"
        run.updated_at = time.time()

    except Exception as e:
        run.status = "failed"
        run.error = f"{type(e).__name__}: {e}"


async def _drive_agent(
    run: RunState,
    agent: Agent,
    user_msg: str,
    max_steps: int,
) -> Dict[str, Any]:
    """Drive Agent.step() across approval gates.

    Uses asyncio.to_thread for the sync LLM call, and a per-step asyncio.Event
    to pause and resume.
    """
    # The generator lives in the worker thread; we drive it step by step.
    # Each .send(None) advances it. Each yield we capture and decide whether
    # to pause for approval.
    loop = asyncio.get_event_loop()

    gen_state: Dict[str, Any] = {"gen": None, "ev": None, "value": None}

    def _start():
        gen_state["gen"] = agent.step(user_msg, max_steps)
        gen_state["ev"] = None  # current yielded event
        gen_state["value"] = None

    def _next_event():
        """Run the generator until it yields or returns. Returns the event
        or raises StopIteration-equivalent (we use return value None)."""
        gen = gen_state["gen"]
        if gen is None:
            _start()
            gen = gen_state["gen"]
        try:
            ev = next(gen)
            gen_state["ev"] = ev
            return ev
        except StopIteration:
            return None

    def _send(value):
        """Send value into the generator (resumes past a yield)."""
        gen = gen_state["gen"]
        try:
            ev = gen.send(value)
            gen_state["ev"] = ev
            return ev
        except StopIteration:
            return None

    # Main async loop
    while True:
        run.status = "running"
        run.pending_tool = None
        run.pending_args = None
        run.pending_preview = ""
        run.resume_event.clear()
        # Ensure generator is started (first iteration only)
        if gen_state["gen"] is None:
            ev = await loop.run_in_executor(None, _next_event)
        else:
            ev = gen_state["ev"]
            if ev is None:
                # Generator already exhausted (shouldn't reach here normally)
                return {"ok": True, "summary": ""}

        if ev is None:
            # Generator returned without finish event — treat as finished
            return {"ok": True, "summary": ""}

        if isinstance(ev, FailedEvent):
            return {"ok": False, "error": ev.error}

        if isinstance(ev, FinishEvent):
            return {"ok": True, "summary": ev.summary}

        if isinstance(ev, ToolCallEvent):
            should = _should_request_approval(run.phase, ev.tool, ev.args)
            if should:
                # Surface to API, wait for /approve
                run.pending_tool = ev.tool
                run.pending_args = ev.args
                run.pending_preview = ev.preview
                run.status = "awaiting_approval"
                run.updated_at = time.time()

                await run.resume_event.wait()

                decision = run.resume_decision
                comment = run.resume_comment
                run.resume_event.clear()

                if decision == "rejected":
                    return {"ok": False, "error": f"user rejected: {comment}"}
                if decision == "modify":
                    # Push user's feedback as a new user turn on the agent
                    # history, then continue driving.
                    agent.history.append({
                        "role": "user",
                        "content": f"[user feedback]\n{comment}\n\nAdjust your approach and continue.",
                    })
                    # Restart the generator from where it was, but since we
                    # appended to history of the existing agent, we need to
                    # start a fresh generator (the previous one is paused
                    # at the yield). Simpler: re-build agent and re-run
                    # from scratch with the modified requirement. For now,
                    # just send(None) so the agent proceeds; the user
                    # feedback is in history and will be visible next step.
                    ev = await loop.run_in_executor(None, _send, None)
                    if ev is None:
                        return {"ok": True, "summary": ""}
                    continue

                # approved: send(None) to advance past the yield
                ev = await loop.run_in_executor(None, _send, None)
                if ev is None:
                    return {"ok": True, "summary": ""}
                continue

            # No approval needed (read_file) — but write_file that doesn't
            # match the approval predicate also just auto-executes via send(None).
            ev = await loop.run_in_executor(None, _send, None)
            if ev is None:
                return {"ok": True, "summary": ""}
            continue

        # Unknown event type — advance
        ev = await loop.run_in_executor(None, _send, None)
        if ev is None:
            return {"ok": True, "summary": ""}