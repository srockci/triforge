"""Board API: kanban + run detail + SSE event stream.

Endpoints:
    GET  /board/runs               -> list of run snapshots (for kanban)
    GET  /board/runs/{run_id}      -> full detail for one run
    GET  /board/runs/{run_id}/events   -> SSE stream (live + replay)
    POST /board/runs/{run_id}/approve  -> approve / reject / modify
    POST /board/runs               -> create a new run (alias for /workflow/start)

All endpoints bind to 127.0.0.1 only — do not expose externally.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .config import WORKSPACE_ROOT
from .events import BoardEvent, bus
from .store import store
from .workflow import RunState, engine, _snapshot_for_board, run_pipeline_async

router = APIRouter(prefix="/board", tags=["board"])


# -----------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------
class StartRequest(BaseModel):
    requirement: str
    priority: str = "medium"   # "low" | "medium" | "high"


class ApproveRequest(BaseModel):
    decision: str   # "approve" | "reject" | "modify"
    comment: str = ""


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------
def _list_workspace_files() -> List[Dict[str, Any]]:
    """List files under workspace/ for the file tree panel."""
    out: List[Dict[str, Any]] = []
    if not WORKSPACE_ROOT.exists():
        return out
    for p in sorted(WORKSPACE_ROOT.rglob("*")):
        if p.is_file():
            try:
                size = p.stat().st_size
            except OSError:
                size = 0
            out.append({
                "path": str(p.relative_to(WORKSPACE_ROOT)),
                "size": size,
                "modified": p.stat().st_mtime,
            })
    return out


def _read_file_safe(rel_path: str) -> Optional[Dict[str, Any]]:
    """Read a file from workspace, with path-traversal protection."""
    target = (WORKSPACE_ROOT / rel_path).resolve()
    if not str(target).startswith(str(WORKSPACE_ROOT.resolve())):
        return None
    if not target.is_file():
        return None
    try:
        content = target.read_text(errors="replace")
        return {
            "path": rel_path,
            "content": content,
            "size": len(content),
        }
    except OSError as e:
        return {"path": rel_path, "error": str(e)}


# -----------------------------------------------------------------------
# Kanban: list all known runs
# -----------------------------------------------------------------------
@router.get("/runs")
async def list_runs() -> Dict[str, Any]:
    """Return every run the board knows about — combines:
       1. Currently-active runs from engine
       2. Historical runs from store (events persisted)
    """
    # Active runs from engine (live in-memory state)
    active: Dict[str, Dict[str, Any]] = {}
    for rid, run in engine.runs.items():
        active[rid] = _snapshot_for_board(run)

    # Merge with store snapshots (covers runs that ended and got GC'd
    # from engine, or runs we just persisted events for).
    merged: Dict[str, Dict[str, Any]] = dict(active)
    for snap in store.known_runs():
        rid = snap.get("run_id")
        if rid and rid not in merged:
            merged[rid] = snap

    runs = list(merged.values())
    # Sort: active first (running, awaiting_approval), then by updated_at desc
    order = {"awaiting_approval": 0, "running": 1, "failed": 2, "completed": 3}
    runs.sort(key=lambda r: (order.get(r.get("status", ""), 99),
                              -(r.get("updated_at") or 0)))
    return {"runs": runs, "total": len(runs)}


# -----------------------------------------------------------------------
# Run detail
# -----------------------------------------------------------------------
@router.get("/runs/{run_id}")
async def get_run(run_id: str) -> Dict[str, Any]:
    # Prefer live state from engine if available
    run = engine.get(run_id)
    if run:
        snap = _snapshot_for_board(run)
    else:
        snap = store.snapshot(run_id)
    if not snap:
        raise HTTPException(404, f"unknown run_id: {run_id}")

    # Add workspace files for the file-tree panel
    files = _list_workspace_files()

    # If currently awaiting approval, surface the pending preview
    pending = None
    if run and run.status == "awaiting_approval":
        pending = {
            "tool": run.pending_tool,
            "args": run.pending_args,
            "preview": run.pending_preview,
        }

    # Estimate cost (mock — real would come from agent step counters)
    phase = snap.get("phase", "design")
    cost_per_phase = {"design": 0.18, "implement": 0.27, "review": 0.20, "done": 0.65}
    cost = cost_per_phase.get(phase, 0.10)
    if snap.get("status") == "completed":
        cost = 0.65
    elif snap.get("status") == "failed":
        cost = 0.10

    return {
        **snap,
        "files": files,
        "pending": pending,
        "cost_estimate": cost,
    }


# -----------------------------------------------------------------------
# Run creation
# -----------------------------------------------------------------------
@router.post("/runs")
async def create_run(req: StartRequest) -> Dict[str, Any]:
    if not req.requirement.strip():
        raise HTTPException(400, "requirement is empty")
    run = engine.create(req.requirement)
    # Schedule the pipeline (same as /workflow/start)
    asyncio.create_task(run_pipeline_async(run))
    # Persist initial snapshot so the board sees it before any events fire
    try:
        store.update_snapshot(run.run_id, _snapshot_for_board(run))
    except Exception:
        pass
    return {
        "run_id": run.run_id,
        "status": "started",
        "phase": run.phase,
    }


# -----------------------------------------------------------------------
# Approval (delegates to engine — same logic as /workflow/{id}/approve)
# -----------------------------------------------------------------------
@router.post("/runs/{run_id}/approve")
async def approve(run_id: str, req: ApproveRequest) -> Dict[str, Any]:
    run = engine.get(run_id)
    if not run:
        raise HTTPException(404, f"unknown run_id: {run_id}")
    if run.status != "awaiting_approval":
        raise HTTPException(409, f"run not awaiting approval (status={run.status})")
    if req.decision not in ("approve", "reject", "modify"):
        raise HTTPException(400, f"decision must be approve|reject|modify, got {req.decision!r}")
    ok = engine.submit_decision(run_id, req.decision, req.comment)
    if not ok:
        raise HTTPException(409, "failed to submit decision (race?)")
    return {"status": "decision_submitted", "decision": req.decision}


# -----------------------------------------------------------------------
# File content (for code preview / diff toolbar)
# -----------------------------------------------------------------------
@router.get("/runs/{run_id}/files")
async def list_run_files(run_id: str) -> Dict[str, Any]:
    # Sanity-check the run exists
    if not engine.get(run_id) and not store.snapshot(run_id):
        raise HTTPException(404, f"unknown run_id: {run_id}")
    return {"files": _list_workspace_files()}


@router.get("/runs/{run_id}/files/{path:path}")
async def read_run_file(run_id: str, path: str) -> Dict[str, Any]:
    if not engine.get(run_id) and not store.snapshot(run_id):
        raise HTTPException(404, f"unknown run_id: {run_id}")
    content = _read_file_safe(path)
    if not content:
        raise HTTPException(404, f"file not found: {path}")
    return content


# -----------------------------------------------------------------------
# SSE event stream
# -----------------------------------------------------------------------
@router.get("/runs/{run_id}/events")
async def stream_events(run_id: str, request: Request,
                        since: float = 0.0) -> StreamingResponse:
    """SSE: replay past events, then stream live.

    Query: ?since=<unix-ts> — only events after this timestamp
    """
    # Validate run exists (or has history)
    if not engine.get(run_id) and not store.snapshot(run_id) \
            and not store.replay(run_id):
        raise HTTPException(404, f"unknown run_id: {run_id}")

    async def event_gen() -> AsyncIterator[bytes]:
        # Replay historical events first
        for ev in store.replay(run_id, since_ts=since):
            yield _format_sse(ev)
        # Subscribe to live events
        q = bus.subscribe(run_id)
        try:
            # Heartbeat every 15s so proxies don't kill the connection
            last_beat = time.time()
            while True:
                if await request.is_disconnected():
                    break
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=1.0)
                    yield _format_sse(ev)
                except asyncio.TimeoutError:
                    if time.time() - last_beat > 15:
                        yield b": heartbeat\n\n"
                        last_beat = time.time()
        finally:
            bus.unsubscribe(run_id, q)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering
            "Connection": "keep-alive",
        },
    )


def _format_sse(ev: BoardEvent) -> bytes:
    """Format a BoardEvent as an SSE frame."""
    data = ev.to_dict()
    payload = json.dumps(data, ensure_ascii=False, default=str)
    return f"event: {ev.kind}\ndata: {payload}\n\n".encode("utf-8")