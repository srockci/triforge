"""FastAPI server entrypoint.

Run with:
    cd /root/openmanus-integration && source .venv/bin/activate
    uvicorn openmanus_server.server:app --host 127.0.0.1 --port 8000

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
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .config import AGENT_PROMPTS, WORKSPACE_ROOT
from .workflow import RunState, engine, run_pipeline_async


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure workspace dirs exist
    for sub in ("design", "src", "tests"):
        (WORKSPACE_ROOT / sub).mkdir(parents=True, exist_ok=True)
    yield


app = FastAPI(title="OpenManus Integration", lifespan=lifespan)


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