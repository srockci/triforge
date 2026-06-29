"""Workflow orchestrator: drives the A -> B -> A pipeline with approval gates.

State machine for one workflow run:
    design (Architect-A)  -> [AWAITING APPROVAL] -> approval1
    implement (Coder-B)   -> [AWAITING APPROVAL] -> approval2
    review (Architect-A)  -> [COMPLETED]

Approval gates happen BEFORE the agent calls write_file on key files
(architecture.md, the first src/*.py, review_report.md). The workflow
pauses, exposes its current state via /status, and resumes when /approve
is called.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .agent import Agent, make_agent
from .config import AGENT_PROMPTS, WORKSPACE_ROOT


# Files that trigger approval gates when written.
APPROVAL_FILES = {
    "design": ["design/architecture.md"],
    "implement": ["src"],  # any src/*.py write triggers approval
    "review": ["design/review_report.md"],
}


@dataclass
class RunState:
    run_id: str
    requirement: str
    phase: str = "design"           # design -> approval1 -> implement -> approval2 -> review -> done
    status: str = "running"         # running | awaiting_approval | completed | failed
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    pending_tool: Optional[str] = None
    pending_args: Optional[Dict[str, Any]] = None
    pending_preview: str = ""
    outputs: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    history: List[Dict[str, Any]] = field(default_factory=list)
    # asyncio.Event that /approve sets to resume the workflow
    resume_event: asyncio.Event = field(default_factory=asyncio.Event)
    resume_decision: str = "pending"   # pending | approved | rejected
    resume_comment: str = ""


class WorkflowEngine:
    """In-memory store of running workflows.

    Single-process for now. If we ever scale, swap for Redis.
    """

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


# Module-level singleton (FastAPI dependency-injection alternative)
engine = WorkflowEngine()


# ---------------------------------------------------------------------------
# Approval preview generator
# ---------------------------------------------------------------------------
def _preview_for(tool: str, args: Dict[str, Any]) -> str:
    """Return a short preview of the action for the user to approve."""
    if tool == "write_file":
        path = args.get("path", "?")
        content = args.get("content", "")
        head = content[:600]
        return f"📝 write_file: {path}\n\n{head}{'...' if len(content) > 600 else ''}"
    if tool == "read_file":
        return f"👀 read_file: {args.get('path', '?')}"
    if tool == "finish":
        return f"✅ finish: {args.get('summary', '')}"
    return f"{tool}: {args}"


def _should_request_approval(phase: str, tool: str, args: Dict[str, Any]) -> bool:
    """Return True if this tool call in this phase should pause for approval."""
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


# ---------------------------------------------------------------------------
# Pipeline driver
# ---------------------------------------------------------------------------
async def run_pipeline(run: RunState, prompts: Dict[str, str] = AGENT_PROMPTS) -> None:
    """Drive a single run through design -> implement -> review.

    This coroutine runs as a background asyncio.Task after /workflow/start.
    """
    try:
        # ----- Phase 1: DESIGN -----
        run.phase = "design"
        run.status = "running"
        architect = make_agent("architect_design", prompts)
        design_msg = (
            f"User requirement:\n{run.requirement}\n\n"
            f"Workspace root: {WORKSPACE_ROOT}\n"
            f"Write your architecture design to: workspace/design/architecture.md "
            f"(absolute path will be computed relative to workspace root)."
        )
        result = await asyncio.to_thread(
            architect.run,
            design_msg,
            12,
            lambda tool, args: _approval_hook(run, tool, args),
        )
        if result["status"] != "finished":
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
            f"Read the architecture from: workspace/design/architecture.md\n"
            f"Implement code under workspace/src/ and tests under workspace/tests/.\n"
            f"Workspace root: {WORKSPACE_ROOT}"
        )
        result = await asyncio.to_thread(
            coder.run,
            impl_msg,
            20,
            lambda tool, args: _approval_hook(run, tool, args),
        )
        if result["status"] != "finished":
            run.status = "failed"
            run.error = result.get("error", "implement phase did not finish")
            return
        # Collect produced files
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
            f"(design at workspace/design/architecture.md, code at workspace/src/, "
            f"tests at workspace/tests/).\n"
            f"Write your report to workspace/design/review_report.md.\n"
            f"Workspace root: {WORKSPACE_ROOT}"
        )
        result = await asyncio.to_thread(
            reviewer.run,
            review_msg,
            12,
            lambda tool, args: _approval_hook(run, tool, args),
        )
        if result["status"] != "finished":
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


def _approval_hook(run: RunState, tool: str, args: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Sync hook called by Agent.run before each tool execution.

    Returns {"pause": True, "preview": "..."} to halt the agent and surface
    an approval request to the API caller.
    """
    if not _should_request_approval(run.phase, tool, args):
        return None
    preview = _preview_for(tool, args)
    run.pending_tool = tool
    run.pending_args = args
    run.pending_preview = preview
    run.status = "awaiting_approval"
    run.updated_at = time.time()
    # Note: this is sync, called from a worker thread (asyncio.to_thread).
    # The async run_pipeline() is awaiting resume_event.set() in another
    # coroutine — but actually the agent loop blocks THIS thread, so we
    # can't easily wait here without deadlocking.
    #
    # Solution: don't wait here. Just signal pause. The pipeline coroutine
    # notices the pause return value, awaits the event itself, and re-runs
    # the agent step. See run_pipeline_async for the proper flow.
    return {"pause": True, "preview": preview}


def _make_hook(run: RunState):
    """Return a closure suitable for Agent.run(on_tool_call=...).

    The closure reads run.phase each call (so phase transitions between
    runs are picked up automatically) and signals pause via a dict.
    """
    def hook(tool: str, args: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _approval_hook(run, tool, args)
    return hook


# ---------------------------------------------------------------------------
# Async pipeline with proper resume (used by the server)
# ---------------------------------------------------------------------------
async def run_pipeline_async(run: RunState) -> None:
    """Like run_pipeline, but actually waits for /approve between phases.

    For simplicity, we run each phase as: call agent.run() once. If it
    returns 'paused', we wait on resume_event, then call agent.run() again
    to resume from where it left off (the agent's history is preserved).
    """
    try:
        # ----- Phase 1: DESIGN -----
        run.phase = "design"
        run.status = "running"
        architect = make_agent("architect_design", AGENT_PROMPTS)
        design_msg = (
            f"User requirement:\n{run.requirement}\n\n"
            f"Workspace root: {WORKSPACE_ROOT}\n"
            f"Write your architecture design to: design/architecture.md "
            f"(relative to workspace root)."
        )
        result = await _run_with_resume(run, architect, design_msg, max_steps=12)
        if result["status"] != "finished":
            run.status = "failed"
            run.error = result.get("error", "design phase did not finish")
            return
        run.outputs["design_doc"] = str(WORKSPACE_ROOT / "design/architecture.md")
        run.history.append({"phase": "design", "summary": result.get("summary", "")})

        # ----- Phase 2: IMPLEMENT -----
        run.phase = "implement"
        coder = make_agent("coder_implement", AGENT_PROMPTS)
        impl_msg = (
            f"User requirement:\n{run.requirement}\n\n"
            f"Read the architecture from: design/architecture.md\n"
            f"Implement code under src/ and tests under tests/.\n"
            f"Workspace root: {WORKSPACE_ROOT}"
        )
        result = await _run_with_resume(run, coder, impl_msg, max_steps=20)
        if result["status"] != "finished":
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
        reviewer = make_agent("architect_review", AGENT_PROMPTS)
        review_msg = (
            f"Original user requirement:\n{run.requirement}\n\n"
            f"Review everything under workspace/ "
            f"(design at design/architecture.md, code at src/, tests at tests/).\n"
            f"Write your report to: design/review_report.md\n"
            f"Workspace root: {WORKSPACE_ROOT}"
        )
        result = await _run_with_resume(run, reviewer, review_msg, max_steps=12)
        if result["status"] != "finished":
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


async def _run_with_resume(run: RunState, agent: Agent, user_msg: str, max_steps: int) -> Dict[str, Any]:
    """Run an agent, pause for approval when needed, resume when /approve fires."""
    while True:
        run.status = "running"
        run.pending_tool = None
        run.pending_args = None
        run.pending_preview = ""
        run.resume_event.clear()

        # Run the agent in a worker thread so it doesn't block the event loop
        result = await asyncio.to_thread(
            agent.run, user_msg, max_steps, _make_hook(run)
        )

        if result["status"] != "paused":
            return result

        # Agent paused for approval. Wait until /approve is called.
        # Tighten the wakeup: when resume_event is set, also accept the
        # comment / decision.
        await run.resume_event.wait()

        decision = run.resume_decision
        comment = run.resume_comment

        if decision == "rejected":
            return {"status": "failed", "error": f"user rejected: {comment}"}
        if decision == "modify":
            # Push the user's feedback as a new user turn, then continue.
            user_msg = (
                f"[user feedback after rejection of last action]\n{comment}\n\n"
                f"Adjust your approach and continue. Original task unchanged."
            )
            continue
        # approved -> loop again, agent.run() will resume from saved history
        continue