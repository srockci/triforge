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
import json
import secrets
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .agent import Agent, FinishEvent, FailedEvent, ToolCallEvent, TokenUsageEvent, make_agent
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
    if "review_report" in outputs and outputs["review_report"]:
        run.completed_phases.add("review")

    ws = run.workspace_root
    if ws is not None and "design" not in run.completed_phases:
        if (Path(ws) / "design" / "architecture.md").exists():
            run.completed_phases.add("design")
    # Module-level backfill: scan disk for completed module sub-phases
    if ws is not None:
        design_dir = Path(ws) / "design" / "modules"
        src_dir = Path(ws) / "src"
        test_dir = Path(ws) / "tests"
        if design_dir.exists():
            for md in design_dir.glob("*.md"):
                mid = md.stem
                run.completed_phases.add(f"module_detail_{mid}")
        # Only backfill module_code_<id> if the module's entry in run.modules
        # reports passed/manually_approved status.  Otherwise a failed code
        # phase that left source files behind would be incorrectly treated
        # as completed after a server restart.
        if src_dir.exists() and run.modules:
            known_module_ids = {m["id"] for m in run.modules
                                if m.get("status") in ("passed", "manually_approved")}
            for sd in src_dir.iterdir():
                if sd.is_dir() and (sd / "__init__.py").exists() and sd.name in known_module_ids:
                    run.completed_phases.add(f"module_code_{sd.name}")
        if test_dir.exists():
            for tf in test_dir.glob("test_*.py"):
                mid = tf.stem.replace("test_", "", 1)
                run.completed_phases.add(f"module_test_{mid}")
    # Only backfill review if the report exists AND the run's outputs
    # prove it was written by a successful review phase.
    if ws is not None and "review" not in run.completed_phases:
        report_path = Path(ws) / "design" / "review_report.md"
        if report_path.exists() and run.outputs.get("review_report") == str(report_path):
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
    if phase == "review":
        return path.endswith("review_report.md")
    return False


@dataclass
class RunState:
    run_id: str
    requirement: str
    phase: str = "design"
    status: str = "running"
    access_token: str = ""
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
    tokens_in: int = 0
    tokens_out: int = 0
    cost_estimate: float = 0.0
    models: Dict[str, str] = field(default_factory=dict)
    window_tokens_in: int = 0
    window_tokens_out: int = 0
    window_start_time: float = 0.0
    project_tokens_in: int = 0
    project_tokens_out: int = 0
    approved_paths: set = field(default_factory=set)
    cancelled: bool = False
    workspace_root: Optional[Any] = None
    working_paths: List[str] = field(default_factory=list)
    completed_phases: set = field(default_factory=set)
    project_path: str = ""
    iteration: int = 0
    requirement_addenda: List[str] = field(default_factory=list)
    awaiting_iteration_input: bool = False
    # ---- Module pipeline (modular design) ----
    # Parsed from design/modules.json, each entry:
    # {"id": str, "name": str, "estimated_files": int, "depends_on": [str],
    #  "interface": {...}, "estimated_steps": int,
    #  "status": "pending"|"in_progress"|"passed"|"failed"|"needs_human"|"manually_approved",
    #  "retry_count": int, "notes": str}
    modules: List[Dict[str, Any]] = field(default_factory=list)
    current_module_idx: int = -1        # index into modules currently being processed
    current_phase_sub: str = ""         # "detail" | "code" | "test" | ""
    module_retry_count: int = 0         # retries for the current module
    needs_human_modules: List[str] = field(default_factory=list)  # module ids needing human decision
    module_decision: Optional[Tuple[str, int]] = None  # (decision, module_idx) set by user


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
        run_id = f"run_{secrets.token_urlsafe(16).replace('-', '_')}"
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
            access_token=secrets.token_urlsafe(32),
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
# ---------------------------------------------------------------------------
# Module helpers
# ---------------------------------------------------------------------------
def _topological_sort(modules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return modules sorted in dependency order (DAG). Raises ValueError if cycle detected."""
    module_map = {m["id"]: m for m in modules}
    visited = set()
    temp = set()
    order: List[Dict[str, Any]] = []

    def _visit(mid: str) -> None:
        if mid in temp:
            raise ValueError(f"dependency cycle detected involving module {mid!r}")
        if mid in visited:
            return
        temp.add(mid)
        for dep in module_map[mid].get("depends_on", []):
            if dep in module_map:
                _visit(dep)
        temp.remove(mid)
        visited.add(mid)
        order.append(module_map[mid])

    for m in modules:
        if m["id"] not in visited:
            _visit(m["id"])
    return order


def _validate_modules_json(data: Any) -> List[Dict[str, Any]]:
    """Parse and validate modules.json. Returns module list or raises ValueError."""
    if not isinstance(data, dict) or "modules" not in data:
        raise ValueError("modules.json must have a 'modules' array")
    modules = data["modules"]
    if not isinstance(modules, list) or not modules:
        raise ValueError("modules array is empty or invalid")
    ids = set()
    for m in modules:
        if not isinstance(m, dict) or "id" not in m:
            raise ValueError("each module must have an 'id' field")
        mid = m["id"]
        if mid in ids:
            raise ValueError(f"duplicate module id: {mid!r}")
        ids.add(mid)
        m.setdefault("estimated_files", 8)
        m.setdefault("estimated_steps", 20)
        m.setdefault("depends_on", [])
        m.setdefault("interface", {})
        m.setdefault("status", "pending")
        m.setdefault("retry_count", 0)
        m.setdefault("notes", "")
        # Validate depends_on references known modules;
        # silently strip any that reference unknown modules.
        deps = m.get("depends_on", [])
        valid = [d for d in deps if d == m["id"] or d in ids]
        if len(valid) != len(deps):
            m["depends_on"] = valid
    # Topological sort validates the DAG
    return _topological_sort(modules)


def _module_summary(run: RunState) -> str:
    """Build a human-readable summary of completed modules for agent context."""
    lines = []
    for m in run.modules or []:
        mid = m["id"]
        if m["status"] in ("passed", "manually_approved"):
            lines.append(f"  Module {mid} ({m.get('name', mid)}) — completed")
            # List files under src/<mid>/
            ws = run.workspace_root
            if ws:
                for p in sorted((Path(ws) / "src" / mid).rglob("*.py")):
                    rel = p.relative_to(ws).as_posix()
                    lines.append(f"    {rel}")
            lines.append("")
    return "\n".join(lines) if lines else "  (no completed modules yet)"


async def run_pipeline_async(run: RunState, settings: Optional[Dict[str, Any]] = None) -> None:
    """Drive a modular pipeline: top-design → per-module (detail→code→test) → top-review."""
    if settings is None:
        settings = get_settings().get()

    def _emit(kind: str, **data: Any) -> None:
        try:
            ev = BoardEvent(run_id=run.run_id, kind=kind, data=data)
            get_store().append(ev)
            bus.emit(ev)
        except Exception:
            pass

    _emit("run_start", requirement=run.requirement)

    ws = run.workspace_root or workspace_for_run(run.run_id)
    run.workspace_root = ws

    pp = settings.get("pipeline_params", {})
    design_steps = pp.get("design", {}).get("max_steps", 12)
    review_steps = pp.get("review", {}).get("max_steps", 12)
    mp = pp.get("module", {})
    detail_max_steps = mp.get("detail_max_steps", 8)
    code_max_steps = mp.get("code_max_steps", 20)
    test_max_steps = mp.get("test_max_steps", 6)
    max_retry = mp.get("max_retry_per_module", 3)

    reuse_designer_for_test = bool(mp.get("reuse_designer_for_test", True))
    # module_test only used when reuse_designer_for_test=false
    # (default: true → uses architect_review instead)
    test_role_name = "architect_review" if reuse_designer_for_test else "module_test"

    roles = settings.get("roles", {})
    run.models = {
        "design": roles.get("architect_design", {}).get("model", ""),
        "module_detail": roles.get("module_detail", {}).get("model", ""),
        "module_code": roles.get("module_code", {}).get("model", ""),
        "module_test": roles.get(test_role_name, {}).get("model", ""),
        "review": roles.get("architect_review", {}).get("model", ""),
    }

    approval_cfg = settings.get("approval", {})
    global_working_paths = approval_cfg.get("working_paths", [])
    project_working_paths = list(run.working_paths or [])
    working_paths = project_working_paths if project_working_paths else global_working_paths
    remember_approved = approval_cfg.get("remember_approved", True)

    try:
        # ======== PHASE 1: Top-level design ========
        if "top_design" in run.completed_phases:
            pass  # skip
        else:
            run.phase = "design"
            run.status = "running"
            get_store().update_snapshot(run.run_id, _snapshot_for_board(run))

            role_cfg = roles.get("architect_design", {})
            _emit("phase_start", phase="design", agent="architect_design",
                  model=role_cfg.get("model", "MiniMax"))
            architect = make_agent("architect_design", ws, settings)
            remaining = design_steps
            design_user_msg = (
                f"User requirement:\n```\n{run.requirement}\n```\n\n"
                f"Workspace root: {ws}\n"
                f"\n"
                f"## Task — two outputs\n"
                f"1. Write the top-level architecture to design/architecture.md\n"
                f"2. Write a module manifest to design/modules.json with this EXACT schema:\n"
                f"```json\n"
                f"{{\n"
                f'  "modules": [\n'
                f"    {{\n"
                f'      "id": "module_name",\n'
                f'      "name": "Human-readable name",\n'
                f'      "estimated_files": 6,\n'
                f'      "depends_on": ["other_module_id"],\n'
                f'      "interface": {{"exports": ["ClassName", "function_name"], "description": "..."}},\n'
                f'      "estimated_steps": 18\n'
                f"    }}\n"
                f"  ]\n"
                f"}}\n"
                f"```\n"
                f"## Module constraints\n"
                f"- Each module: estimated_files ≤ 8, estimated_steps ≤ 22\n"
                f"- depends_on must ONLY reference other module ids that EXIST in this same modules array\n"
                f"- If a module exceeds the limits, split it into smaller modules\n"
                f"- Topological order in the array is preferred but not required\n"
                f"- Do NOT write any .py code\n"
                f"\n"
                f"When done, call finish(summary='modules: <comma-separated ids>')."
            )
            if run.iteration > 0 and run.requirement_addenda:
                last_add = run.requirement_addenda[-1]
                design_user_msg += (
                    f"\n\n---\n"
                    f"## Iteration {run.iteration}\n"
                    f"Previous iteration finished. Update design/modules.json and "
                    f"design/architecture.md to reflect the new requirement:\n"
                    f"> {last_add}"
                )
            result = await _drive_agent(run, architect, design_user_msg,
                                        max_steps=remaining,
                                        working_paths=working_paths,
                                        remember_approved=remember_approved)
            if run.cancelled or not result.get("ok"):
                _finish_phase(run, "design", result, _emit, _snapshot_for_board)
                return

            # Parse modules.json
            modules_path = Path(ws) / "design" / "modules.json"
            try:
                raw = json.loads(modules_path.read_text(encoding="utf-8"))
                run.modules = _validate_modules_json(raw)
            except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
                run.status = "failed"
                run.error = f"modules.json validation failed: {e}"
                _emit("run_end", status="failed", error=run.error)
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                return

            run.outputs["modules"] = [m["id"] for m in run.modules]
            run.outputs["design_doc"] = str(ws / "design/architecture.md")
            run.completed_phases.add("top_design")
            run.history.append({"phase": "top_design", "summary": result.get("summary", "")})
            try:
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
            except Exception:
                pass
            _emit("phase_end", phase="design", ok=True,
                  modules=[m["id"] for m in run.modules],
                  summary=result.get("summary", ""))

        # ======== PHASE 2: Per-module loop ========
        for mod_idx, mod in enumerate(run.modules):
            if run.cancelled:
                run.status = "cancelled"
                _emit("run_end", status="cancelled")
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                return

            mid = mod["id"]
            # Skip modules already completed (from a previous partial run)
            if mod["status"] in ("passed", "manually_approved"):
                continue

            run.current_module_idx = mod_idx
            mod["status"] = "in_progress"
            mod["retry_count"] = 0
            run.module_retry_count = 0
            run.current_phase_sub = "detail"
            try:
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
            except Exception:
                pass

            # Retry loop for this module
            while run.module_retry_count <= max_retry:
                if run.cancelled:
                    run.status = "cancelled"
                    _emit("run_end", status="cancelled")
                    get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                    return

                # 2a. Module detail design
                run.current_phase_sub = "detail"
                _emit("phase_start", phase=f"detail_{mid}", agent="module_detail",
                      model=roles.get("module_detail", {}).get("model", ""))
                detail_agent = make_agent("module_detail", ws, settings)
                remaining = detail_max_steps
                detail_msg = (
                    f"User requirement:\n```\n{run.requirement}\n```\n\n"
                    f"Top-level architecture: design/architecture.md\n"
                    f"Current module: {mid} ({mod.get('name', mid)})\n"
                    f"Interface contract: {json.dumps(mod.get('interface', {}), indent=2)}\n"
                    f"Depends on: {mod.get('depends_on', [])}\n\n"
                    f"{_module_summary(run)}\n\n"
                    f"Write detailed design to design/modules/{mid}.md"
                )
                result = await _drive_agent(run, detail_agent, detail_msg,
                                            max_steps=remaining,
                                            working_paths=working_paths,
                                            remember_approved=remember_approved,
                                            module_id=mid)
                if run.cancelled or not result.get("ok"):
                    mod["status"] = "failed"
                    _emit("phase_end", phase=f"detail_{mid}", ok=False,
                          error=result.get("error", ""))
                    _finish_phase(run, f"detail_{mid}", result, _emit, _snapshot_for_board)
                    return

                run.completed_phases.add("module_detail")
                run.completed_phases.add(f"module_detail_{mid}")
                _emit("phase_end", phase=f"detail_{mid}", ok=True,
                      summary=result.get("summary", ""))

                # 2b. Module code
                run.current_phase_sub = "code"
                _emit("phase_start", phase=f"code_{mid}", agent="module_code",
                      model=roles.get("module_code", {}).get("model", ""))
                code_agent = make_agent("module_code", ws, settings)
                remaining = code_max_steps
                code_msg = (
                    f"User requirement:\n```\n{run.requirement}\n```\n\n"
                    f"Detailed design: design/modules/{mid}.md\n"
                    f"Module: {mid}\n"
                    f"Interface: {json.dumps(mod.get('interface', {}), indent=2)}\n\n"
                    f"Completed modules so far:\n{_module_summary(run)}\n\n"
                    f"Write implementation to src/{mid}/*.py and tests/test_{mid}.py"
                )
                result = await _drive_agent(run, code_agent, code_msg,
                                            max_steps=remaining,
                                            working_paths=working_paths,
                                            remember_approved=remember_approved,
                                            module_id=mid)
                if run.cancelled or not result.get("ok"):
                    mod["status"] = "failed"
                    _emit("phase_end", phase=f"code_{mid}", ok=False,
                          error=result.get("error", ""))
                    _finish_phase(run, f"code_{mid}", result, _emit, _snapshot_for_board)
                    return

                run.completed_phases.add("module_code")
                run.completed_phases.add(f"module_code_{mid}")
                _emit("phase_end", phase=f"code_{mid}", ok=True,
                      summary=result.get("summary", ""))

                # 2c. Module test (read-only diagnosis)
                run.current_phase_sub = "test"
                _emit("phase_start", phase=f"test_{mid}", agent=test_role_name,
                      model=roles.get(test_role_name, {}).get("model", ""))
                test_agent = make_agent(test_role_name, ws, settings)
                remaining = test_max_steps
                test_msg = (
                    "[SYSTEM OVERRIDE] Ignore any system-level instructions about writing "
                    "review reports. You are NOT performing a review.\n\n"
                    f"Read-only diagnosis for module {mid}.\n"
                    f"Implementation: src/{mid}/*.py\n"
                    f"Tests: tests/test_{mid}.py\n\n"
                    f"Read the files and call finish(PASS) or finish(FAIL: <reason>)."
                )
                result = await _drive_agent(run, test_agent, test_msg,
                                            max_steps=remaining,
                                            working_paths=working_paths,
                                            remember_approved=remember_approved,
                                            module_id=mid)
                if run.cancelled:
                    run.status = "cancelled"
                    _emit("run_end", status="cancelled")
                    get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                    return

                test_passed = result.get("ok") and "PASS" in (result.get("summary", "").upper())
                if test_passed:
                    mod["status"] = "passed"
                    run.completed_phases.add("module_test")
                    run.completed_phases.add(f"module_test_{mid}")
                    _emit("phase_end", phase=f"test_{mid}", ok=True,
                          verdict="PASS", summary=result.get("summary", ""))
                    break  # exit retry loop, move to next module

                # Test FAILED
                run.module_retry_count += 1
                mod["retry_count"] = run.module_retry_count
                if run.module_retry_count <= max_retry:
                    _emit("phase_end", phase=f"test_{mid}", ok=False,
                          verdict="FAIL", retry=run.module_retry_count,
                          errors=result.get("summary", ""))
                    # Feedback goes into next code iteration
                else:
                    # Exhausted retries — needs human
                    mod["status"] = "needs_human"
                    run.needs_human_modules.append(mid)
                    run.current_phase_sub = ""
                    run.status = "awaiting_human"
                    _emit("phase_end", phase=f"test_{mid}", ok=False,
                          verdict="FAIL", error="max retries exceeded")
                    try:
                        get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                    except Exception:
                        pass

                    # Check if too many modules need human
                    if len(run.needs_human_modules) >= 3:
                        run.status = "failed"
                        run.error = f"3+ modules need human intervention: {run.needs_human_modules}"
                        _emit("run_end", status="failed", error=run.error)
                        get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                        return

                    # Wait for human decision via module_decision endpoint
                    run.resume_event.clear()
                    run.module_decision = None
                    await run.resume_event.wait()
                    if run.cancelled:
                        run.status = "cancelled"
                        _emit("run_end", status="cancelled")
                        get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                        return

                    if not run.module_decision:
                        run.status = "failed"
                        run.error = f"no decision received for module {mid}"
                        _emit("run_end", status="failed", error=run.error)
                        get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                        return

                    decision = run.module_decision[0]
                    if decision == "approve_skip":
                        mod["status"] = "manually_approved"
                        break  # skip to next module
                    elif decision == "regenerate":
                        # Reset retry and redo detail→code→test
                        run.module_retry_count = 0
                        mod["retry_count"] = 0
                        mod["status"] = "in_progress"
                        # Clean up old files
                        for p in sorted((Path(ws) / "src" / mid).rglob("*")):
                            if p.is_file():
                                p.unlink()
                        for p in sorted((Path(ws) / "tests").rglob(f"test_{mid}*")):
                            if p.is_file():
                                p.unlink()
                        continue  # restart retry
                    else:  # cancel
                        run.status = "failed"
                        run.error = f"user cancelled at module {mid}"
                        _emit("run_end", status="failed", error=run.error)
                        get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
                        return

            # End of retry loop for this module
            try:
                get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
            except Exception:
                pass

        # ======== PHASE 3: Top-level review ========
        if "top_review" in run.completed_phases:
            pass  # skip
        else:
            run.current_module_idx = -1
            run.current_phase_sub = ""
            run.phase = "review"
            run.status = "running"
            get_store().update_snapshot(run.run_id, _snapshot_for_board(run))

            role_cfg = roles.get("architect_review", {})
            _emit("phase_start", phase="review", agent="architect_review",
                  model=role_cfg.get("model", "MiniMax"))
            reviewer = make_agent("architect_review", ws, settings)
            remaining = review_steps
            review_msg = (
                    f"Original user requirement:\n```\n{run.requirement}\n```\n\n"
                    f"Modules: {json.dumps([m['id'] for m in run.modules], indent=2)}\n\n"
                    f"Review everything in {ws}\n\n"
                    f"## Checklist\n"
                    f"1. Architecture consistency — does the code match architecture.md?\n"
                    f"2. Module completeness — all modules implemented?\n"
                    f"3. Cross-module interface contracts — do they match?\n"
                    f"4. Code quality — types, docstrings, error handling, PEP 8\n"
                    f"5. Test coverage — do tests exist and cover main paths?\n"
                    f"6. Security — any hardcoded secrets, SQL injection, unsafe shell calls?\n"
                    f"7. Verdict — PASS / CONDITIONAL PASS / FAIL + 1-3 follow-ups\n\n"
                    f"Write verdict to design/review_report.md"
                )
            result = await _drive_agent(run, reviewer, review_msg,
                                        max_steps=remaining,
                                        working_paths=working_paths,
                                        remember_approved=remember_approved)
            if run.cancelled or not result.get("ok"):
                _finish_phase(run, "review", result, _emit, _snapshot_for_board)
                return

            run.outputs["review_report"] = str(ws / "design/review_report.md")
            run.completed_phases.add("top_review")
            run.history.append({"phase": "top_review", "summary": result.get("summary", "")})
            _emit("phase_end", phase="review", ok=True,
                  summary=result.get("summary", ""),
                  modules=[m["id"] for m in run.modules],
                  module_statuses={m["id"]: m["status"] for m in run.modules})

        # ======== Done: await iteration ========
        run.status = "awaiting_iteration"
        run.phase = "done"
        run.awaiting_iteration_input = True
        run.updated_at = time.time()
        _emit("iteration_pending", iteration=run.iteration, run_id=run.run_id)
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


def _finish_phase(run: RunState, phase: str, result: Dict[str, Any],
                  _emit, _snapshot_for_board) -> None:
    """Handle phase failure uniformly."""
    if run.status != "failed":
        run.status = "failed"
        run.error = result.get("error", "") or f"{phase} phase did not finish"
    elif not run.error:
        run.error = result.get("error", "") or f"{phase} phase did not finish"
    try:
        ev_obj = BoardEvent(run_id=run.run_id, kind="phase_end",
                            data={"phase": phase, "ok": False, "error": run.error})
        get_store().append(ev_obj); bus.emit(ev_obj)
    except Exception:
        pass
    try:
        get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
    except Exception:
        pass


# Resume hint template: shown to the agent when restarting a phase
# with saved history. Shorter than the full user_msg because the
# agent already has context from its restored history.
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
    "review": "architect_review",
}
_PHASE_STEPS_DEFAULTS = {"design": 12, "review": 12}


def _get_phase_max_steps(phase: str, settings: Dict[str, Any]) -> int:
    role = _PHASE_ROLE_MAP.get(phase)
    return settings.get("pipeline_params", {}).get(phase, {}).get("max_steps",
           _PHASE_STEPS_DEFAULTS.get(phase, 12))


def _snapshot_for_board(run: RunState) -> Dict[str, Any]:
    """Board-friendly view of a run (what the kanban needs)."""
    phase_to_idx = {"design": 0, "detail": 1, "code": 2, "test": 3,
                    "review": 4, "done": 5}
    base_phase = run.current_phase_sub if run.current_phase_sub else run.phase
    return {
        "run_id": run.run_id,
        "status": run.status,
        "phase": run.phase,
        "phase_index": phase_to_idx.get(base_phase, 0),
        "current_module_idx": run.current_module_idx,
        "current_phase_sub": run.current_phase_sub,
        "module_retry_count": run.module_retry_count,
        "needs_human_modules": list(run.needs_human_modules),
        "modules": [
            {"id": m["id"], "name": m.get("name", m["id"]),
             "status": m["status"], "retry_count": m.get("retry_count", 0),
             "depends_on": m.get("depends_on", []),
             "estimated_files": m.get("estimated_files", 0),
             "estimated_steps": m.get("estimated_steps", 0)}
            for m in (run.modules or [])
        ],
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
        "iteration": run.iteration,
        "requirement_addenda": list(run.requirement_addenda or []),
        "awaiting_iteration_input": run.awaiting_iteration_input,
        "window_tokens_in": run.window_tokens_in,
        "window_tokens_out": run.window_tokens_out,
        "project_tokens_in": run.project_tokens_in,
        "project_tokens_out": run.project_tokens_out,
        "project_path": run.project_path or "",
        "access_token": run.access_token or "",
        "phase_steps_used": 0,
        "phase_steps_max": 0,
        "phase_steps_remaining": 0,
    }


async def _drive_agent(
    run: RunState,
    agent: Agent,
    user_msg: str,
    max_steps: int,
    working_paths: Optional[List[str]] = None,
    remember_approved: bool = True,
    module_id: str = "",
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
        
        # Read token_plan_mode from settings (P4: provider_models_refactor)
        tp_mode = get_settings().get_model_token_plan_mode(ev.provider_key, ev.model)
        is_token_plan = (tp_mode == "token_plan")
        
        if is_token_plan:
            # Token-plan models: track window and project usage
            run.window_tokens_in += ev.tokens_in
            run.window_tokens_out += ev.tokens_out
            run.project_tokens_in += ev.tokens_in
            run.project_tokens_out += ev.tokens_out
            
            # Check if we need to reset window usage (at specified hours)
            current_hour = time.localtime().tm_hour
            window_hours = get_settings().pipeline_params["token_plan"]["window_hours"]
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
                                   "provider": ev.provider_key,
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
            store = get_store()
            store._current_module_id = module_id
            agent.save_state_to(run.run_id, run.phase, store, _steps_used)
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
