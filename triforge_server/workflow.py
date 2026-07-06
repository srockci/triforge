"""Workflow orchestrator: drives the A -> B -> A pipeline with approval gates.

Uses Agent.step() generator. The async pipeline iterates events from a
generator running in a worker thread (because the LLM call is sync). When
the generator yields a ToolCallEvent that needs approval, we surface it
to the API, await a user decision, then send(None) into the generator
to resume.

Each run gets its own isolated workspace directory (workspace_for_run)
so concurrent runs never collide.
"""
from __future__ import annotations

import asyncio
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .agent import Agent, FinishEvent, FailedEvent, ToolCallEvent, TokenUsageEvent, make_agent, make_agent_with_resume
from .config import WORKSPACE_ROOT, workspace_for_run
from .events import BoardEvent, bus
from .settings import get_settings
from .store import get_store


def _backfill_completed_phases(run: RunState) -> bool:
    """Augment `completed_phases` by inferring from outputs and disk.

    Three sources:
      1. The in-memory `run.completed_phases` set.
      2. The persisted `outputs` dict on the run.
      3. Disk filesystem — check for canonical phase artifacts.

    Returns True if any change was made.
    """
    before = set(run.completed_phases)
    outputs = run.outputs or {}

    if "design_doc" in outputs and outputs["design_doc"]:
        run.completed_phases.add("design")
    if "code_files" in outputs and outputs["code_files"]:
        run.completed_phases.add("implement")
    if "review_report" in outputs and outputs["review_report"]:
        run.completed_phases.add("review")

    # Disk fallback: when outputs are incomplete (e.g., max_steps failure
    # before phase_end wrote the snapshot), check the filesystem directly.
    ws = run.workspace_root
    if ws is not None and "design" not in run.completed_phases:
        if (Path(ws) / "design" / "architecture.md").exists():
            run.completed_phases.add("design")
    if ws is not None and "implement" not in run.completed_phases:
        src = Path(ws) / "src"
        if src.exists() and list(src.rglob("*.py")):
            run.completed_phases.add("implement")
    if ws is not None and "review" not in run.completed_phases:
        if (Path(ws) / "design" / "review_report.md").exists():
            run.completed_phases.add("review")

    return run.completed_phases != before


def _normalize_path(path: str) -> str:
    """Normalize a relative path for matching.

    Strips leading './' / '/' / '\\\\', collapses repeated '/',
    and lowercases for case-insensitive fs matches. Does NOT resolve
    symlinks — that would defeat the purpose of LLM-side decisions.
    """
    if not path:
        return ""
    p = path.replace("\\", "/").strip()
    while p.startswith("./"):
        p = p[2:]
    while p.startswith("/"):
        p = p[1:]
    while "//" in p:
        p = p.replace("//", "/")
    while p.endswith("/") and len(p) > 1:
        p = p[:-1]
    return p


def _should_request_approval(phase: str, tool: str, args: Dict[str, Any],
                               working_paths: Optional[List[str]] = None,
                               approved_paths: Optional[set] = None) -> bool:
    """Decide whether a tool call needs user approval.

    Skip approval if:
    - Not a write_file call
    - Path is under a configured working_path
    - Path was already approved earlier in this run (remember_approved)
    """
    if tool != "write_file":
        return False
    path = _normalize_path(args.get("path", ""))

    # Check working paths (auto-approve)
    if working_paths:
        for wp in working_paths:
            wp_n = _normalize_path(wp)
            if not wp_n:
                continue
            # '.' means the workspace root — auto-approve all writes
            if wp_n == '.':
                return False
            # Match exactly, or as a directory prefix
            if path == wp_n or path.startswith(wp_n.rstrip("/") + "/"):
                return False

    # Check previously approved paths in this run
    if approved_paths and path in approved_paths:
        return False

    # Default approval rules — all paths already normalized
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
    # Token usage tracking (accumulated across all phases)
    tokens_in: int = 0
    tokens_out: int = 0
    cost_estimate: float = 0.0
    # Model names actually used per phase (set at pipeline start)
    models: Dict[str, str] = field(default_factory=dict)
    
    # Token plan specific tracking
    token_plan_models: Dict[str, bool] = field(default_factory=dict)  # model_name: is_token_plan
    window_tokens_in: int = 0
    window_tokens_out: int = 0
    window_start_time: float = 0.0
    project_tokens_in: int = 0
    project_tokens_out: int = 0
    # Paths approved for write in this run (remember_approved feature)
    approved_paths: set = field(default_factory=set)
    # Cancellation: when set, _drive_agent checks between steps
    cancelled: bool = False
    # Per-run workspace path
    workspace_root: Optional[Any] = None  # Path, but use Any to avoid import
    # Per-project working paths: writes here skip approval.
    # Set at run creation (from /board/runs POST) and persisted to DB.
    # Falls back to global settings.approval.working_paths when empty.
    working_paths: List[str] = field(default_factory=list)
    # Phases that have completed successfully. On resume, these are skipped
    # so the pipeline continues from the next incomplete phase instead of
    # restarting from design.
    completed_phases: set = field(default_factory=set)
    # User-specified project path where files are written directly.
    # When empty, files go to the default WORKSPACE_ROOT/<run_id>/.
    project_path: str = ""
    # ---- Iteration loop (P5) ----
    # How many design→code→review cycles have completed. 0 means the
    # original run; 1+ means at least one user addendum was folded in.
    iteration: int = 0
    # Cumulative history of requirement addenda, in order. The original
    # requirement is run.requirement; each new user-submitted requirement
    # during the awaiting_iteration prompt is appended here so the
    # audit log preserves every change.
    requirement_addenda: List[str] = field(default_factory=list)
    # When True, the run has finished the latest review cycle and is
    # waiting for the user to either add another requirement or mark
    # the run as done. While True, the pipeline loop is suspended.
    awaiting_iteration_input: bool = False


class WorkflowEngine:
    def __init__(self):
        self.runs: Dict[str, RunState] = {}
        self.active_tasks: Dict[str, asyncio.Task] = {}

    def start_pipeline(self, run: RunState) -> None:
        """Launch or resume a pipeline, with anti-reentry protection."""
        existing = self.active_tasks.get(run.run_id)
        if existing and not existing.done():
            return
        task = asyncio.create_task(run_pipeline_async(run))
        self.active_tasks[run.run_id] = task
        task.add_done_callback(lambda t: self.active_tasks.pop(run.run_id, None))

    def create(self, requirement: str,
               working_paths: Optional[List[str]] = None,
               project_path: str = "") -> RunState:
        run_id = f"run_{uuid.uuid4().hex[:10]}"
        pp = project_path.strip() if project_path else ""
        if pp:
            from .config import workspace_from_path
            ws = workspace_from_path(pp)
        else:
            ws = workspace_for_run(run_id)
        run = RunState(
            run_id=run_id,
            requirement=requirement,
            workspace_root=ws,
            working_paths=list(working_paths or []),
            project_path=pp,
        )
        self.runs[run.run_id] = run
        return run

    def get(self, run_id: str) -> Optional[RunState]:
        run = self.runs.get(run_id)
        if run is not None:
            # Lazy backfill from outputs so legacy runs (where completed_phases
            # was never tracked) still skip completed phases on the next
            # pipeline run. Persist the resolved value back so future
            # server restarts don't need to re-derive.
            if _backfill_completed_phases(run):
                try:
                    from .store import get_store
                    get_store().update_snapshot(
                        run.run_id, _snapshot_for_board(run)
                    )
                except Exception:
                    pass  # persistence is best-effort
        return run

    def submit_decision(self, run_id: str, decision: str, comment: str = "") -> bool:
        run = self.runs.get(run_id)
        if not run or run.status != "awaiting_approval":
            return False
        run.resume_decision = decision
        run.resume_comment = comment
        run.resume_event.set()
        return True

    def cancel_run(self, run_id: str) -> bool:
        """Request cancellation of a running pipeline."""
        run = self.runs.get(run_id)
        if not run:
            return False
        if run.status in ("completed", "failed", "cancelled"):
            return False
        run.cancelled = True
        # If awaiting approval, unblock so the pipeline can check cancelled flag
        if run.status == "awaiting_approval":
            run.resume_decision = "rejected"
            run.resume_comment = "Cancelled by user"
            run.resume_event.set()
        return True

    def force_stop(self, run_id: str) -> bool:
        """Force a stuck run into 'failed' state.

        Use this when a run's background task has died but the status
        is still 'running' or 'awaiting_approval'. This directly sets
        the status to 'failed' (NOT 'cancelled') so the user can hit
        "Continue" to resume from the current phase, skipping any
        phases already recorded in run.completed_phases.
        """
        run = self.runs.get(run_id)
        if not run:
            return False
        if run.status in ("completed", "failed", "cancelled"):
            return False
        run.status = "failed"
        run.error = "Force-stopped by user"
        run.updated_at = time.time()
        # Wake the pipeline if it's blocked on the approval gate so it
        # notices status=failed on its next loop iteration.
        run.resume_event.set()
        # Persist the change
        try:
            get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
        except Exception:
            pass
        # Emit event
        try:
            ev = BoardEvent(run_id=run.run_id, kind="run_end",
                            data={"status": "failed", "error": run.error})
            get_store().append(ev)
            bus.emit(ev)
        except Exception:
            pass
        return True

    def delete_run(self, run_id: str) -> bool:
        """Remove a run from the engine (must be in a terminal state)."""
        run = self.runs.get(run_id)
        if not run:
            # Try to delete from store only
            try:
                get_store().delete_run(run_id)
                return True
            except Exception:
                return False
        if run.status in ("running", "awaiting_approval"):
            return False  # Cannot delete active runs
        del self.runs[run_id]
        try:
            get_store().delete_run(run_id)
        except Exception:
            pass
        return True


engine = WorkflowEngine()


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
async def run_pipeline_async(run: RunState, settings: Optional[Dict[str, Any]] = None) -> None:
    """Drive a single run through design -> implement -> review."""
    # Snapshot current settings at pipeline start (in-flight runs keep their config)
    if settings is None:
        settings = get_settings().get()

    # ----- Helpers (local) -----
    def _emit(kind: str, **data: Any) -> None:
        """Fire-and-forget event for the board. Never raises."""
        try:
            ev = BoardEvent(run_id=run.run_id, kind=kind, data=data)
            get_store().append(ev)   # persist for board replay
            bus.emit(ev)       # push to live subscribers
        except Exception:
            pass  # board events are non-critical

    _emit("run_start", requirement=run.requirement)

    # Ensure per-run workspace exists
    ws = run.workspace_root or workspace_for_run(run.run_id)
    run.workspace_root = ws

    # Pipeline params from settings
    pp = settings.get("pipeline_params", {})
    design_steps = pp.get("design", {}).get("max_steps", 12)
    implement_steps = pp.get("implement", {}).get("max_steps", 25)
    review_steps = pp.get("review", {}).get("max_steps", 12)

    # Snapshot model names for the API (what each phase actually uses)
    roles = settings.get("roles", {})
    run.models = {
        "design": roles.get("architect_design", {}).get("model", ""),
        "implement": roles.get("coder_implement", {}).get("model", ""),
        "review": roles.get("architect_review", {}).get("model", ""),
    }

    # Approval settings
    approval_cfg = settings.get("approval", {})
    # Working paths: per-project overrides global.
    # If the run specifies any, those REPLACE the global list. If empty,
    # the global list (from settings) is used.
    global_working_paths = approval_cfg.get("working_paths", [])
    project_working_paths = list(run.working_paths or [])
    working_paths = project_working_paths if project_working_paths else global_working_paths
    remember_approved = approval_cfg.get("remember_approved", True)

    try:
        # ----- Phase loop -----
        # Each phase records itself in run.completed_phases on success.
        # On resume (interrupted/failed), phases already in
        # run.completed_phases are skipped so the pipeline continues
        # from the next incomplete phase instead of restarting from
        # design.
        PHASES = ("design", "implement", "review")

        for phase in PHASES:
            if run.cancelled:
                _emit("run_end", status="cancelled")
                run.status = "cancelled"
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                return

            # Skip phases that finished before the interruption.
            if phase in run.completed_phases:
                continue

            run.phase = phase
            run.status = "running"
            get_store().update_snapshot(run.run_id, _snapshot_for_board(run))

            if phase == "design":
                role_cfg = settings.get("roles", {}).get("architect_design", {})
                _emit("phase_start", phase="design", agent="architect_design",
                      model=role_cfg.get("model", "MiniMax"))
                architect, saved_steps = make_agent_with_resume(
                    "architect_design", ws, settings, run.run_id, "design")
                remaining = design_steps - saved_steps
                if remaining <= 0:
                    return {"ok": False, "error": "max_steps exhausted before resume"}
                if saved_steps > 0:
                    existing = _list_existing_files(ws)
                    user_msg = RESUME_HINT_TEMPLATE.format(
                        existing_files=existing, max_steps_remaining=remaining)
                else:
                    design_user_msg = (
                        f"User requirement:\n{run.requirement}\n\n"
                        f"Workspace root: {ws}\n"
                        f"Write your architecture design to: design/architecture.md "
                        f"(relative to workspace root)."
                    )
                    if run.iteration > 0 and run.requirement_addenda:
                        last_add = run.requirement_addenda[-1]
                        design_user_msg += (
                            f"\n\n---\n"
                            f"## Iteration {run.iteration}\n"
                            f"This is iteration #{run.iteration} of the same "
                            f"run. Previous design is at "
                            f"design/architecture.md and previous review at "
                            f"design/review_report.md — read both first to "
                            f"understand what was already decided.\n"
                            f"The user just added this new requirement:\n\n"
                            f"> {last_add}\n\n"
                            f"Update design/architecture.md to address the new "
                            f"requirement (don't rewrite history; add a new "
                            f"section, update the affected modules, and bump "
                            f"the version header). Then call finish()."
                        )
                    user_msg = design_user_msg
                result = await _drive_agent(run, architect, user_msg,
                                            max_steps=remaining,
                                            working_paths=working_paths,
                                            remember_approved=remember_approved)
            elif phase == "implement":
                role_cfg = settings.get("roles", {}).get("coder_implement", {})
                _emit("phase_start", phase="implement", agent="coder_implement",
                      model=role_cfg.get("model", "DeepSeek"))
                coder, saved_steps = make_agent_with_resume(
                    "coder_implement", ws, settings, run.run_id, "implement")
                remaining = implement_steps - saved_steps
                if remaining <= 0:
                    return {"ok": False, "error": "max_steps exhausted before resume"}
                if saved_steps > 0:
                    existing = _list_existing_files(ws)
                    user_msg = RESUME_HINT_TEMPLATE.format(
                        existing_files=existing, max_steps_remaining=remaining)
                else:
                    user_msg = (
                        f"User requirement:\n{run.requirement}\n\n"
                        f"Read the architecture from: design/architecture.md\n"
                        f"\n"
                        f"## IMPORTANT — Path rules\n"
                        f"Your work goes inside this directory:\n"
                        f"  {ws}\n"
                        f"\n"
                        f"All tool calls (read_file / write_file) take paths RELATIVE to that directory.\n"
                        f"Do NOT prefix paths with `workspace/`, the project name, or any other segment.\n"
                        f"\n"
                        f"Correct examples:\n"
                        f"  - write_file path='src/__init__.py'        (file lives at <ws>/src/__init__.py)\n"
                        f"  - write_file path='src/app/foo.py'         (file lives at <ws>/src/app/foo.py)\n"
                        f"  - write_file path='tests/test_foo.py'      (file lives at <ws>/tests/test_foo.py)\n"
                        f"\n"
                        f"Wrong examples (will create nested junk dirs):\n"
                        f"  - write_file path='workspace/src/foo.py'\n"
                        f"  - write_file path='./src/foo.py'\n"
                        f"  - write_file path='/abs/path/foo.py'\n"
                        f"\n"
                        f"## What to produce\n"
                        f"- All implementation modules under src/\n"
                        f"- All test files under tests/ (mirror the src/ tree)\n"
                        f"- Both directories must exist after you finish.\n"
                        f"\n"
                        f"Begin by reading design/architecture.md, then call write_file for each module."
                    )
                result = await _drive_agent(run, coder, user_msg,
                                            max_steps=remaining,
                                            working_paths=working_paths,
                                            remember_approved=remember_approved)
            else:  # review
                role_cfg = settings.get("roles", {}).get("architect_review", {})
                _emit("phase_start", phase="review", agent="architect_review",
                      model=role_cfg.get("model", "MiniMax"))
                reviewer, saved_steps = make_agent_with_resume(
                    "architect_review", ws, settings, run.run_id, "review")
                remaining = review_steps - saved_steps
                if remaining <= 0:
                    return {"ok": False, "error": "max_steps exhausted before resume"}
                if saved_steps > 0:
                    existing = _list_existing_files(ws)
                    user_msg = RESUME_HINT_TEMPLATE.format(
                        existing_files=existing, max_steps_remaining=remaining)
                else:
                    user_msg = (
                        f"Original user requirement:\n{run.requirement}\n\n"
                        f"Review everything in this directory:\n"
                        f"  {ws}\n"
                        f"\n"
                        f"## IMPORTANT — Path rules\n"
                        f"All tool calls (read_file / write_file) take paths RELATIVE to that directory.\n"
                        f"Do NOT prefix paths with `workspace/`, the project name, or any other segment.\n"
                        f"\n"
                        f"Correct examples:\n"
                        f"  - read_file path='design/architecture.md'    (file at <ws>/design/architecture.md)\n"
                        f"  - read_file path='src/app/main.py'         (file at <ws>/src/app/main.py)\n"
                        f"  - write_file path='design/review_report.md' (writes to <ws>/design/review_report.md)\n"
                        f"\n"
                        f"Wrong examples (will create nested junk or fail):\n"
                        f"  - write_file path='workspace/design/review_report.md'\n"
                        f"  - write_file path='' or path='.'\n"
                        f"  - write_file path='/abs/path/foo.md'\n"
                        f"\n"
                        f"## Budget — STRICT\n"
                        f"You have at most 8-10 tool calls total. Do NOT try to read every file.\n"
                        f"Recommended reads (5 max):\n"
                        f"  1. design/architecture.md     (the design doc)\n"
                        f"  2. src/app/__init__.py or src/app/bootstrap.py   (entry point)\n"
                        f"  3. ONE core module — pick the most central one (e.g. src/app/scheduler.py or src/app/aggregator.py)\n"
                        f"  4. tests/conftest.py OR any one test file   (sample, not all)\n"
                        f"\n"
                        f"Then you MUST call write_file to produce design/review_report.md.\n"
                        f"If you do not call write_file within 10 tool calls, the run will fail.\n"
                        f"\n"
                        f"## Output structure (write this to design/review_report.md)\n"
                        f"  1. Architecture consistency (does code match the design doc?)\n"
                        f"  2. Module completeness (any missing pieces?)\n"
                        f"  3. Code quality (types, docstrings, error handling, PEP 8)\n"
                        f"  4. Test coverage (do tests exist and cover the main paths?)\n"
                        f"  5. Security (any hardcoded secrets, SQL injection, unsafe shell calls?)\n"
                        f"  6. Verdict (PASS / CONDITIONAL PASS / FAIL) + 1-3 concrete follow-ups.\n"
                        f"\n"
                        f"Cite file paths when raising issues. End by calling finish(summary='...')."
                    )
                result = await _drive_agent(run, reviewer, user_msg,
                                            max_steps=remaining,
                                            working_paths=working_paths,
                                            remember_approved=remember_approved)

            # ----- post-phase handling -----
            if run.cancelled:
                run.status = "cancelled"
                _emit("run_end", status="cancelled")
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                return
            if not result.get("ok"):
                # Preserve 'failed' status if it was already set by
                # engine.force_stop while we were awaiting — don't overwrite
                # the error message in that case.
                err = result.get("error", "")
                if run.status != "failed":
                    run.status = "failed"
                    run.error = err or f"{phase} phase did not finish"
                elif not run.error:
                    run.error = err or f"{phase} phase did not finish"
                _emit("phase_end", phase=phase, ok=False, error=run.error)
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                return

            # Record outputs and mark this phase complete
            if phase == "design":
                run.outputs["design_doc"] = str(ws / "design/architecture.md")
            elif phase == "implement":
                src_files = sorted(str(p.relative_to(ws)).replace("\\", "/")
                                  for p in (ws / "src").rglob("*.py"))
                test_files = sorted(str(p.relative_to(ws)).replace("\\", "/")
                                   for p in (ws / "tests").rglob("test_*.py"))
                run.outputs["code_files"] = src_files + test_files
            else:  # review
                run.outputs["review_report"] = str(ws / "design/review_report.md")

            run.completed_phases.add(phase)
            run.history.append({"phase": phase, "summary": result.get("summary", "")})

            # Persist intermediate progress so a crash mid-next-phase can resume.
            try:
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
            except Exception:
                pass

            # Phase-end event with extra info for implement (file lists)
            if phase == "implement":
                _emit("phase_end", phase="implement", ok=True,
                      src_files=run.outputs["code_files"],
                      summary=result.get("summary", ""))
            else:
                _emit("phase_end", phase=phase, ok=True,
                      summary=result.get("summary", ""))

        # After a successful review cycle, the pipeline pauses for the
        # user to add a new requirement (or mark done). The user can
        # trigger the next iteration by POSTing to
        # /board/runs/{id}/iteration with a new requirement, which
        # clears completed_phases and re-launches the pipeline. Or
        # they can POST {"done": true} to mark the run as completed.
        run.status = "awaiting_iteration"
        run.phase = "done"
        run.awaiting_iteration_input = True
        run.updated_at = time.time()
        _emit("iteration_pending",
              iteration=run.iteration,
              run_id=run.run_id)
        try:
            get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
        except Exception:
            pass

    except Exception as e:
        run.status = "failed"
        run.error = f"{type(e).__name__}: {e}"
        try:
            _emit("run_end", status="failed", error=run.error)
            get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
        except Exception:
            pass


# Resume hint template: shown to the agent when restarting a phase
# with saved history. Shorter than the full user_msg because the
# agent already has context from its restored history.
RESUME_HINT_TEMPLATE = (
    "[Resume from interrupted state]\n\n"
    "You were writing files in this workspace and ran out of step budget.\n"
    "Files already on disk:\n"
    "{existing_files}\n\n"
    "Continue from where you stopped. Read existing files before overwriting.\n"
    "You have {max_steps_remaining} steps remaining."
)


def _list_existing_files(ws_root: Path) -> str:
    """Return a compact listing of files under the workspace root."""
    ws = Path(ws_root)
    lines = []
    if (ws / "design" / "architecture.md").exists():
        lines.append("  design/architecture.md")
    if (ws / "design" / "review_report.md").exists():
        lines.append("  design/review_report.md")
    for p in sorted((ws / "src").rglob("*.py")):
        lines.append(f"  src/{p.relative_to(ws / 'src').as_posix()}")
    for p in sorted((ws / "tests").rglob("*.py")):
        lines.append(f"  tests/{p.relative_to(ws / 'tests').as_posix()}")
    return "\n".join(lines) if lines else "  (empty workspace)"


# Map role names to the max_steps settings key
_PHASE_ROLE_MAP = {
    "design": "architect_design",
    "implement": "coder_implement",
    "review": "architect_review",
}
_PHASE_STEPS_DEFAULTS = {"design": 12, "implement": 25, "review": 12}


def _get_phase_max_steps(phase: str, settings: Dict[str, Any]) -> int:
    role = _PHASE_ROLE_MAP.get(phase)
    return settings.get("pipeline_params", {}).get(phase, {}).get("max_steps",
           _PHASE_STEPS_DEFAULTS.get(phase, 12))


def _snapshot_for_board(run: RunState) -> Dict[str, Any]:
    """Board-friendly view of a run (what the kanban needs)."""
    phase_to_idx = {"design": 0, "implement": 1, "review": 2, "done": 3}
    return {
        "run_id": run.run_id,
        "status": run.status,
        "phase": run.phase,
        "phase_index": phase_to_idx.get(run.phase, 0),
        "requirement": run.requirement,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "outputs": run.outputs,
        "error": run.error,
        "pending_tool": run.pending_tool,
        "pending_args": run.pending_args,
        "tokens_in": run.tokens_in,
        "tokens_out": run.tokens_out,
        "cost_estimate": run.cost_estimate,
        "models": run.models,
        "working_paths": list(run.working_paths or []),
        "approved_paths": sorted(run.approved_paths or set()),
        "completed_phases": sorted(run.completed_phases or set()),
        # Iteration loop (P5) — exposed so the UI can show "iteration N"
        # and pre-fill the "what's new" textarea in the iteration modal.
        "iteration": run.iteration,
        "requirement_addenda": list(run.requirement_addenda or []),
        "awaiting_iteration_input": run.awaiting_iteration_input,
        # Token plan tracking for dashboard
        "token_plan_models": dict(run.token_plan_models or {}),
        "window_tokens_in": run.window_tokens_in,
        "window_tokens_out": run.window_tokens_out,
        "project_tokens_in": run.project_tokens_in,
        "project_tokens_out": run.project_tokens_out,
        "project_path": run.project_path or "",
        # Phase step progress (for resume-aware UI)
        "phase_steps_used": get_store().load_agent_state(run.run_id, run.phase)[1]
            if get_store().load_agent_state(run.run_id, run.phase) else 0,
        "phase_steps_max": _PHASE_STEPS_DEFAULTS.get(run.phase, 12),
        "phase_steps_remaining": 0,  # computed below
    }
    snap["phase_steps_remaining"] = max(0,
        snap["phase_steps_max"] - snap["phase_steps_used"])
    return snap


async def _drive_agent(
    run: RunState,
    agent: Agent,
    user_msg: str,
    max_steps: int,
    working_paths: Optional[List[str]] = None,
    remember_approved: bool = True,
) -> Dict[str, Any]:
    """Drive Agent.step() across approval gates.

    Uses asyncio.to_thread for the sync LLM call, and a per-step asyncio.Event
    to pause and resume. Also handles TokenUsageEvent by accumulating into
    the run state and emitting board events.
    """
    loop = asyncio.get_event_loop()

    gen_state: Dict[str, Any] = {"gen": None, "ev": None, "value": None}

    def _start():
        gen_state["gen"] = agent.step(user_msg, max_steps)
        gen_state["ev"] = None
        gen_state["value"] = None

    def _next_event():
        """Run the generator until it yields or returns."""
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

    def _handle_token_usage(ev: TokenUsageEvent) -> None:
        """Accumulate token usage and emit board event."""
        run.tokens_in += ev.tokens_in
        run.tokens_out += ev.tokens_out
        run.cost_estimate += ev.cost
        
        # Check if model is token-plan
        is_token_plan = run.token_plan_models.get(ev.model, False)
        
        if is_token_plan:
            # Token-plan models: track window and project usage
            run.window_tokens_in += ev.tokens_in
            run.window_tokens_out += ev.tokens_out
            run.project_tokens_in += ev.tokens_in
            run.project_tokens_out += ev.tokens_out
            
            # Check if we need to reset window usage (at specified hours)
            current_hour = time.localtime().tm_hour
            window_hours = get_settings().pipeline_params.token_plan.window_hours
            if current_hour in window_hours and run.window_start_time < time.time() - 3600:  # Reset if more than an hour has passed
                run.window_tokens_in = ev.tokens_in
                run.window_tokens_out = ev.tokens_out
                run.window_start_time = time.time()
        
        try:
            bev = BoardEvent(run_id=run.run_id, kind="token_usage",
                             data={"tokens_in": ev.tokens_in,
                                   "tokens_out": ev.tokens_out,
                                   "cost": ev.cost, 
                                   "model": ev.model,
                                   "is_token_plan": is_token_plan,
                                   "window_tokens_in": run.window_tokens_in if is_token_plan else 0,
                                   "window_tokens_out": run.window_tokens_out if is_token_plan else 0,
                                   "project_tokens_in": run.project_tokens_in if is_token_plan else 0,
                                   "project_tokens_out": run.project_tokens_out if is_token_plan else 0})
            get_store().append(bev)
            bus.emit(bev)
        except Exception:
            pass

    # Steps used this phase (for resume save)
    _steps_used = 0

    def _save_agent_state():
        """Best-effort persist agent history for resume."""
        nonlocal _steps_used
        try:
            agent.save_state_to(run.run_id, run.phase, get_store(), _steps_used)
        except Exception:
            pass  # best-effort — don't let DB failure break the LLM flow

    # Main async loop
    while True:
        # Check cancellation
        if run.cancelled:
            return {"ok": False, "error": "cancelled by user"}

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
                return {"ok": True, "summary": ""}

        if ev is None:
            return {"ok": True, "summary": ""}

        if isinstance(ev, TokenUsageEvent):
            _handle_token_usage(ev)
            # Advance past the usage event
            ev = await loop.run_in_executor(None, _send, None)
            if ev is None:
                return {"ok": True, "summary": ""}
            # Process the next event (could be another TokenUsageEvent or a tool call)
            gen_state["ev"] = ev
            # Re-enter the loop to handle the next event
            # But we need to handle it inline since we already advanced
            while isinstance(ev, TokenUsageEvent):
                _handle_token_usage(ev)
                ev = await loop.run_in_executor(None, _send, None)
                if ev is None:
                    return {"ok": True, "summary": ""}
                gen_state["ev"] = ev

            # Fall through to handle the non-usage event
            if isinstance(ev, FailedEvent):
                try:
                    ev_obj = BoardEvent(run_id=run.run_id, kind="agent_error",
                                        data={"error": ev.error, "phase": run.phase})
                    get_store().append(ev_obj); bus.emit(ev_obj)
                except Exception:
                    pass
                return {"ok": False, "error": ev.error}

            if isinstance(ev, FinishEvent):
                try:
                    ev_obj = BoardEvent(run_id=run.run_id, kind="agent_finish",
                                        data={"phase": run.phase, "summary": ev.summary})
                    get_store().append(ev_obj); bus.emit(ev_obj)
                except Exception:
                    pass
                return {"ok": True, "summary": ev.summary}

            # If it's a ToolCallEvent, handle below
            if isinstance(ev, ToolCallEvent):
                pass  # fall through to tool call handling

        if isinstance(ev, FailedEvent):
            try:
                ev_obj = BoardEvent(run_id=run.run_id, kind="agent_error",
                                    data={"error": ev.error, "phase": run.phase})
                get_store().append(ev_obj); bus.emit(ev_obj)
            except Exception:
                pass
            return {"ok": False, "error": ev.error}

        if isinstance(ev, FinishEvent):
            try:
                ev_obj = BoardEvent(run_id=run.run_id, kind="agent_finish",
                                    data={"phase": run.phase, "summary": ev.summary})
                get_store().append(ev_obj); bus.emit(ev_obj)
            except Exception:
                pass
            return {"ok": True, "summary": ev.summary}

        if isinstance(ev, ToolCallEvent):
            # Emit every tool call (visible in the live stream regardless
            # of whether it triggers an approval gate).
            try:
                ev_obj = BoardEvent(run_id=run.run_id, kind="tool_call",
                                    data={"phase": run.phase, "tool": ev.tool,
                                          "args": ev.args, "preview": ev.preview})
                get_store().append(ev_obj); bus.emit(ev_obj)
            except Exception:
                pass
            should = _should_request_approval(
                run.phase, ev.tool, ev.args,
                working_paths=working_paths,
                approved_paths=run.approved_paths if remember_approved else None,
            )
            if should:
                # Surface to API, wait for /approve
                run.pending_tool = ev.tool
                run.pending_args = ev.args
                run.pending_preview = ev.preview
                run.status = "awaiting_approval"
                run.updated_at = time.time()
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))

                try:
                    ev_obj = BoardEvent(run_id=run.run_id, kind="approval_requested",
                                        data={"phase": run.phase, "tool": ev.tool,
                                              "args": ev.args, "preview": ev.preview})
                    get_store().append(ev_obj); bus.emit(ev_obj)
                except Exception:
                    pass

                await run.resume_event.wait()

                # Check termination conditions after wakeup
                if run.cancelled:
                    return {"ok": False, "error": "cancelled by user"}
                if run.status == "failed":
                    # Force-stopped mid-flight by user. Status already
                    # set to failed in engine.force_stop; just return.
                    return {"ok": False, "error": run.error or "force stopped"}

                decision = run.resume_decision
                comment = run.resume_comment
                run.resume_event.clear()

                # Remember approved path for future writes in this run.
                # Stored in normalized form so subsequent writes with
                # ./foo or /foo or other variants still match.
                if decision == "approve" and remember_approved and ev.args:
                    approved_path = _normalize_path(ev.args.get("path", ""))
                    if approved_path:
                        run.approved_paths.add(approved_path)

                try:
                    ev_obj = BoardEvent(run_id=run.run_id, kind="approval_resolved",
                                        data={"phase": run.phase, "decision": decision,
                                              "comment": comment})
                    get_store().append(ev_obj); bus.emit(ev_obj)
                except Exception:
                    pass

                if decision == "rejected":
                    return {"ok": False, "error": f"user rejected: {comment}"}
                if decision == "modify":
                    agent.history.append({
                        "role": "user",
                        "content": f"[user feedback]\n{comment}\n\nAdjust your approach and continue.",
                    })
                    ev = await loop.run_in_executor(None, _send, None)
                    if ev is None:
                        return {"ok": True, "summary": ""}
                    gen_state["ev"] = ev
                    _steps_used += 1
                    _save_agent_state()
                    run.status = "running"
                    get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                    continue

                # approved
                run.status = "running"
                run.pending_tool = None
                run.pending_args = None
                run.pending_preview = ""
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                ev = await loop.run_in_executor(None, _send, None)
                if ev is None:
                    return {"ok": True, "summary": ""}
                gen_state["ev"] = ev
                _steps_used += 1
                _save_agent_state()
                continue

            # No approval needed — auto-execute via send(None)
            ev = await loop.run_in_executor(None, _send, None)
            if ev is None:
                return {"ok": True, "summary": ""}
            gen_state["ev"] = ev
            _steps_used += 1
            _save_agent_state()
            continue

        # Unknown event type — advance
        ev = await loop.run_in_executor(None, _send, None)
        if ev is None:
            return {"ok": True, "summary": ""}
        gen_state["ev"] = ev
