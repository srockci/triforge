"""FastAPI server entrypoint.

Run with:
    # Linux / macOS
    cd <project_root> && source .venv/bin/activate
    uvicorn triforge_server.server:app --host 127.0.0.1 --port 8000

    # Windows
    cd <project_root>
    .venv/Scripts/python -X utf8 -m uvicorn triforge_server.server:app --host 127.0.0.1 --port 8000

Endpoints:
    GET  /health
    POST /workflow/start        body: {"requirement": "..."}
    GET  /workflow/{run_id}/status
    POST /workflow/{run_id}/approve  body: {"decision": "approve|reject|modify", "comment": "..."}
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .config import AGENT_PROMPTS, WORKSPACE_ROOT
from .workflow import RunState, engine, run_pipeline_async
from .board import router as board_router
from .store import get_store


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure base workspace dir exists (per-run subdirs are created on demand)
    WORKSPACE_ROOT.mkdir(parents=True, exist_ok=True)
    # Restore any persisted runs from previous server sessions.
    try:
        result = get_store().restore_to_engine(engine)
        restored = result.get("restored", 0)
        interrupted = result.get("interrupted", 0)
        reconciled = result.get("reconciled", 0)
        if restored or interrupted or reconciled:
            extras = []
            if interrupted:
                extras.append(f"{interrupted} interrupted (awaiting approval at crash)")
            if reconciled:
                extras.append(f"{reconciled} reconciled from event log")
            print(f"[startup] restored {restored} run(s)"
                  + (f" ({', '.join(extras)})" if extras else ""),
                  flush=True)
    except Exception as e:
        print(f"[startup] could not restore runs: {e}", flush=True)

    # Surface API key availability so missing keys are visible at startup
    # (rather than only surfacing when /board/runs is first called).
    try:
        from .settings import get_settings
        providers = get_settings().get().get("providers", {})
        for key, cfg in providers.items():
            api_key = cfg.get("api_key") or os.environ.get(cfg.get("api_key_env", ""), "")
            label = cfg.get("name") or key
            if api_key:
                print(f"[startup] provider {key} ({label}): API key OK",
                      flush=True)
            else:
                print(f"[startup] provider {key} ({label}): MISSING API key — "
                      f"edit Settings or set env var {cfg.get('api_key_env')}",
                      flush=True)
    except Exception as e:
        print(f"[startup] could not check providers: {e}", flush=True)

    # Spin up the SNS notification dispatcher worker (daemon thread).
    # It subscribes to the global event bus and forwards events to any
    # configured channels (Feishu / WeChat / DingTalk / Telegram).
    try:
        from .notifier import start_worker, publish
        start_worker()
        n_channels = len((get_settings().get() or {}).get("notification_channels") or [])
        if n_channels:
            print(f"[startup] notifier worker watching {n_channels} channel(s)",
                  flush=True)
        else:
            print("[startup] notifier worker up (no channels configured; "
                  "add them in Settings)", flush=True)
    except Exception as e:
        print(f"[startup] could not start notifier worker: {e}", flush=True)

    yield


app = FastAPI(title="TriForge Integration", lifespan=lifespan)
app.include_router(board_router)

# Serve dashboard static files from triforge_server/static/
_STATIC_DIR = Path(__file__).parent / "static"
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    async def dashboard_index():
        return FileResponse(str(_STATIC_DIR / "index.html"))


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------
class StartRequest(BaseModel):
    requirement: str
    workflow_id: str = "dev_pipeline"


class ApproveRequest(BaseModel):
    decision: str   # "approve" | "reject" | "modify"
    comment: str = ""


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "workspace": str(WORKSPACE_ROOT),
        "active_runs": sum(1 for r in engine.runs.values() if r.status in ("running", "awaiting_approval")),
        "total_runs": len(engine.runs),
    }


# ---------------------------------------------------------------------------
# Workflow lifecycle
# ---------------------------------------------------------------------------
@app.post("/workflow/start")
async def start_workflow(req: StartRequest) -> Dict[str, Any]:
    if not req.requirement.strip():
        raise HTTPException(400, "requirement is empty")
    run = engine.create(req.requirement)
    # Schedule the pipeline as a background task on the running event loop
    asyncio.create_task(run_pipeline_async(run))
    return {
        "run_id": run.run_id,
        "status": "started",
        "phase": run.phase,
    }


@app.get("/workflow/{run_id}/status")
async def get_status(run_id: str) -> Dict[str, Any]:
    run = engine.get(run_id)
    if not run:
        raise HTTPException(404, f"unknown run_id: {run_id}")
    return _snapshot(run)


@app.post("/workflow/{run_id}/approve")
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _snapshot(run: RunState) -> Dict[str, Any]:
    """Public-safe view of a RunState (no coroutine / event refs)."""
    return {
        "run_id": run.run_id,
        "status": run.status,
        "phase": run.phase,
        "requirement": run.requirement,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "pending_tool": run.pending_tool,
        "pending_args": run.pending_args,
        "pending_preview": run.pending_preview,
        "outputs": run.outputs,
        "error": run.error,
        "history": run.history,
    }