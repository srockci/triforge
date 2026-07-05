"""Board API: kanban + run detail + SSE event stream + lifecycle management.

Endpoints:
    GET  /board/runs                       -> list of run snapshots (for kanban)
    GET  /board/runs/{run_id}              -> full detail for one run
    GET  /board/runs/{run_id}/events       -> SSE stream (live + replay)
    POST /board/runs/{run_id}/approve      -> approve / reject / modify
    POST /board/runs/{run_id}/cancel       -> cancel a running pipeline
    DELETE /board/runs/{run_id}            -> delete a completed/failed run
    POST /board/runs                       -> create a new run

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
from .settings import get_settings
from .store import get_store
from .workflow import RunState, engine, _snapshot_for_board, run_pipeline_async
from openai import OpenAI

router = APIRouter(prefix="/board", tags=["board"])


# -----------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------
class StartRequest(BaseModel):
    requirement: str
    priority: str = "medium"   # "low" | "medium" | "high"
    working_paths: List[str] = []  # per-project paths where writes skip approval


class ApproveRequest(BaseModel):
    decision: str   # "approve" | "reject" | "modify"
    comment: str = ""


# Token usage statistics
class TokenUsageRequest(BaseModel):
    project_id: str
    model: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class TokenPlanModelRequest(BaseModel):
    model_name: str
    is_token_plan: bool


class TokenUsageResponse(BaseModel):
    tokens_in: int
    tokens_out: int
    cost: float
    window_tokens_in: int = 0
    window_tokens_out: int = 0
    project_tokens_in: int = 0
    project_tokens_out: int = 0
    is_token_plan: bool = False


class TokenPlanModelResponse(BaseModel):
    model_name: str
    is_token_plan: bool
    success: bool
    message: str = ""


# -----------------------------------------------------------------------
# Helpers — per-run workspace file access
# -----------------------------------------------------------------------
def _run_workspace(run_id: str) -> Path:
    """Resolve the workspace root for a specific run."""
    run = engine.get(run_id)
    if run and run.workspace_root:
        return run.workspace_root
    # Fallback: per-run subdirectory under WORKSPACE_ROOT
    return (WORKSPACE_ROOT / run_id).resolve()


def _list_workspace_files(run_id: str) -> List[Dict[str, Any]]:
    """List files under the per-run workspace for the file tree panel."""
    ws = _run_workspace(run_id)
    out: List[Dict[str, Any]] = []
    if not ws.exists():
        return out
    for p in sorted(ws.rglob("*")):
        if p.is_file():
            try:
                size = p.stat().st_size
            except OSError:
                size = 0
            out.append({
                "path": str(p.relative_to(ws)).replace("\\", "/"),
                "size": size,
                "modified": p.stat().st_mtime,
            })
    return out


def _read_file_safe(run_id: str, rel_path: str) -> Optional[Dict[str, Any]]:
    """Read a file from the per-run workspace, with path-traversal protection."""
    ws = _run_workspace(run_id)
    target = (ws / rel_path).resolve()
    if not str(target).startswith(str(ws.resolve())):
        return None
    if not target.is_file():
        return None
    try:
        content = target.read_text(encoding="utf-8", errors="replace")
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
    """Return every run the board knows about."""
    active: Dict[str, Dict[str, Any]] = {}
    for rid, run in engine.runs.items():
        active[rid] = _snapshot_for_board(run)

    merged: Dict[str, Dict[str, Any]] = dict(active)
    for snap in get_store().known_runs():
        rid = snap.get("run_id")
        if rid and rid not in merged:
            merged[rid] = snap

    runs = list(merged.values())
    order = {"awaiting_approval": 0, "running": 1, "interrupted": 2,
             "failed": 3, "cancelled": 4, "completed": 5}
    runs.sort(key=lambda r: (order.get(r.get("status", ""), 99),
                              -(r.get("updated_at") or 0)))
    return {"runs": runs, "total": len(runs)}


# -----------------------------------------------------------------------
# Run detail
# -----------------------------------------------------------------------
@router.get("/runs/{run_id}")
async def get_run(run_id: str) -> Dict[str, Any]:
    run = engine.get(run_id)
    if run:
        snap = _snapshot_for_board(run)
    else:
        snap = get_store().snapshot(run_id)
    if not snap:
        raise HTTPException(404, f"unknown run_id: {run_id}")

    files = _list_workspace_files(run_id)

    pending = None
    if run and run.status == "awaiting_approval":
        pending = {
            "tool": run.pending_tool,
            "args": run.pending_args,
            "preview": run.pending_preview,
        }

    return {
        **snap,
        "files": files,
        "pending": pending,
    }


# -----------------------------------------------------------------------
# Run creation
# -----------------------------------------------------------------------
@router.post("/runs")
async def create_run(req: StartRequest) -> Dict[str, Any]:
    if not req.requirement.strip():
        raise HTTPException(400, "requirement is empty")
    # Normalise & dedupe working paths before persisting
    clean_wp: List[str] = []
    seen: set = set()
    for wp in (req.working_paths or []):
        norm = wp.strip().strip("/\\")
        if norm and norm not in seen:
            seen.add(norm)
            clean_wp.append(norm)
    run = engine.create(req.requirement, working_paths=clean_wp)
    asyncio.create_task(run_pipeline_async(run))
    try:
        get_store().update_snapshot(run.run_id, _snapshot_for_board(run))
    except Exception:
        pass
    return {
        "run_id": run.run_id,
        "status": "started",
        "phase": run.phase,
        "working_paths": clean_wp,
    }


# -----------------------------------------------------------------------
# Approval (delegates to engine)
# -----------------------------------------------------------------------
@router.post("/runs/{run_id}/approve")
async def approve(run_id: str, req: ApproveRequest) -> Dict[str, Any]:
    run = engine.get(run_id)
    if not run:
        raise HTTPException(404, f"unknown run_id: {run_id}")
    if req.decision not in ("approve", "reject", "modify"):
        raise HTTPException(400, f"decision must be approve|reject|modify, got {req.decision!r}")

    if run.status == "interrupted":
        run.status = "running"
        run.pending_tool = None
        run.pending_args = None
        run.pending_preview = ""
        run.resume_event.clear()
        try:
            ev = BoardEvent(run_id=run.run_id, kind="run_resumed",
                            data={"phase": run.phase, "reason": "server_restart"})
            get_store().append(ev); bus.emit(ev)
        except Exception:
            pass
        asyncio.create_task(run_pipeline_async(run))
        return {"status": "pipeline_restarted", "decision": req.decision,
                "phase": run.phase}

    if run.status != "awaiting_approval":
        raise HTTPException(409, f"run not awaiting approval (status={run.status})")
    ok = engine.submit_decision(run_id, req.decision, req.comment)
    if not ok:
        raise HTTPException(409, "failed to submit decision (race?)")
    return {"status": "decision_submitted", "decision": req.decision}


# -----------------------------------------------------------------------
# Token usage statistics
# -----------------------------------------------------------------------
@router.post("/board/token-usage")
async def get_token_usage(req: TokenUsageRequest) -> Dict[str, Any]:
    """Get token usage statistics for a project or model."""
    # This would typically query a database for historical token usage
    # For now, return empty structure
    return {
        "tokens_in": 0,
        "tokens_out": 0,
        "cost": 0.0,
        "window_tokens_in": 0,
        "window_tokens_out": 0,
        "project_tokens_in": 0,
        "project_tokens_out": 0,
        "is_token_plan": False
    }


@router.post("/board/token-plan-model")
async def set_token_plan_model(req: TokenPlanModelRequest) -> TokenPlanModelResponse:
    """Set whether a model uses token-plan pricing."""
    settings = get_settings()
    token_plan_models = settings.pipeline_params.token_plan.models
    
    # Update the token plan models setting
    token_plan_models[req.model_name] = req.is_token_plan
    settings.save()
    
    return TokenPlanModelResponse(
        model_name=req.model_name,
        is_token_plan=req.is_token_plan,
        success=True,
        message=f"Model {req.model_name} token-plan setting updated"
    )


@router.get("/board/token-plan-models")
async def get_token_plan_models() -> Dict[str, bool]:
    """Get all token-plan model settings."""
    settings = get_settings()
    return settings.pipeline_params.token_plan.models


# -----------------------------------------------------------------------
# Cancel a running pipeline
# -----------------------------------------------------------------------
@router.post("/runs/{run_id}/cancel")
async def cancel(run_id: str) -> Dict[str, Any]:
    """Cancel a running or awaiting pipeline. Sets cancelled flag."""
    run = engine.get(run_id)
    if not run:
        raise HTTPException(404, f"unknown run_id: {run_id}")
    if run.status in ("completed", "failed", "cancelled"):
        raise HTTPException(409, f"run already in terminal state ({run.status})")
    ok = engine.cancel_run(run_id)
    if not ok:
        raise HTTPException(409, "failed to cancel run")
    return {"status": "cancellation_requested", "run_id": run_id}


@router.post("/runs/{run_id}/force-stop")
async def force_stop(run_id: str) -> Dict[str, Any]:
    """Force-stop a stuck run. Directly sets status to 'failed'.

    Use when a run's background task has died but status is still
    'running' or 'awaiting_approval' with no progress.
    """
    run = engine.get(run_id)
    if not run:
        raise HTTPException(404, f"unknown run_id: {run_id}")
    if run.status in ("completed", "failed", "cancelled"):
        raise HTTPException(409, f"run already in terminal state ({run.status})")
    ok = engine.force_stop(run_id)
    if not ok:
        raise HTTPException(500, "failed to force-stop run")
    return {"status": "force_stopped", "run_id": run_id}


# -----------------------------------------------------------------------
# Resume an interrupted or failed run
# -----------------------------------------------------------------------
@router.post("/runs/{run_id}/resume")
async def resume(run_id: str) -> Dict[str, Any]:
    """Resume an interrupted, failed, or cancelled run from its current phase.

    The pipeline restarts from the phase where it was interrupted /
    failed / cancelled. For 'interrupted' runs (server restart during
    approval), this re-launches the pipeline from the interrupted phase.
    For 'failed' runs, this retries from the failed phase.
    'cancelled' is also permitted so accidental cancels can be undone.
    Phases already recorded in run.completed_phases are skipped.
    """
    run = engine.get(run_id)
    if not run:
        raise HTTPException(404, f"unknown run_id: {run_id}")
    if run.status not in ("interrupted", "failed", "cancelled"):
        raise HTTPException(409,
            f"can only resume interrupted / failed / cancelled runs (status={run.status})")

    # Reset run state for restart
    run.status = "running"
    run.error = None
    run.pending_tool = None
    run.pending_args = None
    run.pending_preview = ""
    run.cancelled = False
    run.resume_event.clear()
    run.updated_at = time.time()

    # Persist the state change
    try:
        get_store().update_snapshot(run_id, _snapshot_for_board(run))
    except Exception:
        pass

    # Emit resume event
    try:
        ev = BoardEvent(run_id=run_id, kind="run_resumed",
                        data={"phase": run.phase, "reason": "user_resume"})
        get_store().append(ev)
        bus.emit(ev)
    except Exception:
        pass

    # Launch the pipeline
    asyncio.create_task(run_pipeline_async(run))
    return {"status": "resumed", "run_id": run_id, "phase": run.phase}


# ---------------------------------------------------------------------------
# Iteration loop (P5): after each review, the user can add a new
# requirement (or mark the run as done). The pipeline re-runs from
# scratch with the cumulative requirement; all previously-completed
# phases are cleared so iteration 1+ doesn't skip design/coder/review.
# ---------------------------------------------------------------------------
class IterationBody(BaseModel):
    requirement: Optional[str] = None   # new requirement text → start next iteration
    done:        bool = False            # → mark the run as completed
    # (Sending {"done": true} without a requirement is the canonical
    # way to end the loop. Sending only a requirement starts the next
    # iteration. Sending both is rejected as ambiguous.)


@router.post("/runs/{run_id}/iteration")
async def post_iteration(run_id: str, body: IterationBody) -> Dict[str, Any]:
    """User response to the post-review iteration prompt.

    Two mutually-exclusive shapes:
      {"requirement": "..."}  → append to requirement, re-run
                                  design/coder/review from scratch
      {"done": true}            → mark run as completed (terminal)
    """
    run = engine.get(run_id)
    if not run:
        raise HTTPException(404, f"unknown run_id: {run_id}")
    if not run.awaiting_iteration_input:
        raise HTTPException(409,
            f"run is not awaiting iteration input (status={run.status}, "
            f"phase={run.phase}). Iteration prompt only appears after a "
            f"review cycle completes.")
    if run.status != "awaiting_iteration":
        # Defense in depth — the two flags should be in sync but the
        # pipeline loop sets them together. If they ever drift, the
        # run is in a weird state and we shouldn't accept input.
        raise HTTPException(500, f"state drift: status={run.status} but "
                                f"awaiting_iteration_input=True")

    if body.done and body.requirement:
        raise HTTPException(400, "send either {requirement} OR {done: true}, "
                                "not both")

    # ----- DONE branch: mark run as completed -----
    if body.done:
        run.status = "completed"
        run.phase = "done"
        run.awaiting_iteration_input = False
        run.updated_at = time.time()
        try:
            ev = BoardEvent(
                run_id=run_id, kind="iteration_completed",
                data={"iteration": run.iteration},
            )
            get_store().append(ev); bus.emit(ev)
        except Exception:
            pass
        try:
            get_store().update_snapshot(run_id, _snapshot_for_board(run))
        except Exception:
            pass
        return {"status": "completed", "iteration": run.iteration}

    # ----- NEW REQUIREMENT branch: re-launch pipeline -----
    new_req = (body.requirement or "").strip()
    if not new_req:
        raise HTTPException(400,
            "requirement is empty. Send non-empty text or "
            "{'done': true} to end the loop.")

    # Append the new requirement to the cumulative history. The agent
    # sees the full string in subsequent runs; the addenda list
    # preserves the audit log of every user change.
    run.requirement_addenda.append(new_req)
    run.requirement = (
        run.requirement
        + "\n\n"
        + f"[Iteration {run.iteration + 1} addendum, "
        + time.strftime("%Y-%m-%d %H:%M") + "]"
        + f"\n{new_req}"
    )
    run.iteration += 1
    run.awaiting_iteration_input = False
    run.completed_phases = set()         # full re-run
    run.phase = "design"                  # explicit (matches pipeline start)
    run.status = "running"
    run.error = None
    run.pending_tool = None
    run.pending_args = None
    run.pending_preview = ""
    run.cancelled = False
    run.resume_event.clear()
    run.outputs = {}                      # wipe prior artifacts
    run.approved_paths = set()
    run.updated_at = time.time()

    try:
        ev = BoardEvent(
            run_id=run_id, kind="iteration_started",
            data={"iteration": run.iteration, "addendum": new_req[:200]},
        )
        get_store().append(ev); bus.emit(ev)
    except Exception:
        pass
    try:
        get_store().update_snapshot(run_id, _snapshot_for_board(run))
    except Exception:
        pass

    # Re-launch the full design → code → review cycle.
    asyncio.create_task(run_pipeline_async(run))
    return {
        "status":    "iterating",
        "iteration": run.iteration,
        "phase":     run.phase,
    }


# -----------------------------------------------------------------------
# Delete a run (must be in terminal state)
# -----------------------------------------------------------------------
@router.delete("/runs/{run_id}")
async def delete_run(run_id: str) -> Dict[str, Any]:
    """Delete a completed, failed, or cancelled run from engine and DB."""
    ok = engine.delete_run(run_id)
    if not ok:
        run = engine.get(run_id)
        if run and run.status in ("running", "awaiting_approval"):
            raise HTTPException(409, f"cannot delete active run (status={run.status})")
        raise HTTPException(404, f"unknown run_id: {run_id}")
    return {"status": "deleted", "run_id": run_id}


# -----------------------------------------------------------------------
# File content (per-run workspace)
# -----------------------------------------------------------------------
@router.get("/runs/{run_id}/files")
async def list_run_files(run_id: str) -> Dict[str, Any]:
    if not engine.get(run_id) and not get_store().snapshot(run_id):
        raise HTTPException(404, f"unknown run_id: {run_id}")
    return {"files": _list_workspace_files(run_id)}


@router.get("/runs/{run_id}/files/{path:path}")
async def read_run_file(run_id: str, path: str) -> Dict[str, Any]:
    if not engine.get(run_id) and not get_store().snapshot(run_id):
        raise HTTPException(404, f"unknown run_id: {run_id}")
    content = _read_file_safe(run_id, path)
    if not content:
        raise HTTPException(404, f"file not found: {path}")
    return content


# -----------------------------------------------------------------------
# SSE event stream
# -----------------------------------------------------------------------
@router.get("/runs/{run_id}/events")
async def stream_events(run_id: str, request: Request,
                        since: float = 0.0) -> StreamingResponse:
    """SSE: replay past events, then stream live."""
    if not engine.get(run_id) and not get_store().snapshot(run_id) \
            and not get_store().replay(run_id):
        raise HTTPException(404, f"unknown run_id: {run_id}")

    async def event_gen() -> AsyncIterator[bytes]:
        for ev in get_store().replay(run_id, since_ts=since):
            yield _format_sse(ev)
        q = bus.subscribe(run_id)
        try:
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
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


def _format_sse(ev: BoardEvent) -> bytes:
    """Format a BoardEvent as an SSE frame."""
    data = ev.to_dict()
    payload = json.dumps(data, ensure_ascii=False, default=str)
    return f"event: {ev.kind}\ndata: {payload}\n\n".encode("utf-8")


# -----------------------------------------------------------------------
# Settings API
# -----------------------------------------------------------------------
_API_KEY_MASK = "********"


def _mask_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of settings with api_key values masked."""
    import copy
    masked = copy.deepcopy(data)
    for prov in masked.get("providers", {}).values():
        if prov.get("api_key"):
            prov["api_key"] = _API_KEY_MASK
    return masked


@router.get("/settings")
async def get_settings_api() -> Dict[str, Any]:
    """Return current settings (api_key values are masked)."""
    from .settings import get_settings
    return _mask_settings(get_settings().get())


@router.post("/settings")
async def save_settings(body: Dict[str, Any]) -> Dict[str, Any]:
    """Save settings (full or partial update). Persists to disk.

    If a provider's api_key is the mask placeholder '********',
    the existing stored value is preserved (not overwritten).
    """
    from .settings import get_settings
    mgr = get_settings()
    current = mgr.get()

    # Preserve masked API keys — don't overwrite with the mask placeholder
    for prov_key, prov_cfg in body.get("providers", {}).items():
        if prov_cfg.get("api_key") == _API_KEY_MASK:
            existing = current.get("providers", {}).get(prov_key, {})
            prov_cfg["api_key"] = existing.get("api_key", "")

    updated = mgr.update(body)
    return {"status": "saved", "settings": _mask_settings(updated)}


@router.get("/settings/defaults")
async def get_default_settings() -> Dict[str, Any]:
    """Return the built-in default settings."""
    from .settings import DEFAULT_SETTINGS
    return DEFAULT_SETTINGS


@router.post("/settings/reset")
async def reset_settings() -> Dict[str, Any]:
    """Reset all settings to defaults."""
    from .settings import get_settings, DEFAULT_SETTINGS
    mgr = get_settings()
    mgr.save(DEFAULT_SETTINGS)
    return {"status": "reset", "settings": DEFAULT_SETTINGS}


# ---------------------------------------------------------------------------
# Model listing API
# ---------------------------------------------------------------------------
class ModelListRequest(BaseModel):
    provider_key: str
    api_key: str = ""
    base_url: str = ""


@router.post("/models")
async def list_models(request: ModelListRequest) -> Dict[str, Any]:
    """Fetch available models from a provider's /v1/models endpoint."""
    try:
        from openai import OpenAI
        
        # Use provided credentials or fall back to env vars / settings
        _API_KEY_MASK = "********"
        api_key = request.api_key or ""
        if api_key == _API_KEY_MASK or not api_key:
            api_key = ""
            # Check env vars with proper names
            if request.provider_key.lower() == "minimax":
                api_key = os.environ.get("MINIMAX_CN_API_KEY", "") or os.environ.get("MINIMAX_API_KEY", "")
            else:
                api_key = os.environ.get(f"{request.provider_key.upper()}_API_KEY", "")
            # Fall back to settings storage
            if not api_key:
                from .settings import get_settings
                provider_cfg = get_settings().get_provider(request.provider_key)
                api_key = provider_cfg.get("api_key", "")
        
        base_url = request.base_url or os.environ.get(
            "TRIFORGE_" + request.provider_key.upper() + "_BASE_URL", ""
        )
        
        if not api_key:
            return {"error": f"API key required for provider '{request.provider_key}'"}
        
        # Use default base URL if not provided
        if not base_url:
            from .settings import get_settings
            provider_cfg = get_settings().get_provider(request.provider_key)
            base_url = provider_cfg.get("base_url", "")
        
        client = OpenAI(api_key=api_key, base_url=base_url)
        
        # MiniMax 没有 OpenAI 兼容的 /v1/models 端点，直接返回已知模型列表
        if request.provider_key.lower() == "minimax":
            minimax_models = [
                {"id": "MiniMax-Text-01", "name": "MiniMax-Text-01", "created": 0},
                {"id": "MiniMax-M3", "name": "MiniMax-M3", "created": 0},
                {"id": "MiniMax-M2.7", "name": "MiniMax-M2.7", "created": 0},
                {"id": "MiniMax-M2.5", "name": "MiniMax-M2.5", "created": 0},
                {"id": "MiniMax-abab6.5s", "name": "MiniMax-abab6.5s", "created": 0},
                {"id": "MiniMax-abab6.5s-chat", "name": "MiniMax-abab6.5s-chat", "created": 0},
            ]
            return {
                "status": "success",
                "provider": request.provider_key,
                "models": minimax_models,
                "total": len(minimax_models),
                "message": "MiniMax known models loaded (no API endpoint for model listing)"
            }

        # Use standard OpenAI client for other providers (DeepSeek, etc.)
        try:
            response = client.models.list()
            models = []
            for model in response.data:
                models.append({
                    "id": model.id,
                    "name": model.id,
                    "created": model.created,
                })
            
            return {
                "status": "success",
                "provider": request.provider_key,
                "models": models,
                "total": len(models)
            }
        except Exception as e:
            # If API fails, raise exception to trigger fallback
            raise Exception(f"OpenAI API failed: {str(e)}")
        
    except Exception as e:
        # Return fallback models if API call fails
        fallback_models = {
            "deepseek": [
                "deepseek-chat",
                "deepseek-reasoner",
                "deepseek-coder",
                "deepseek-v2.5"
            ]
        }
        
        fallback = fallback_models.get(request.provider_key.lower(), [])
        if fallback:
            return {
                "status": "fallback",
                "provider": request.provider_key,
                "models": [{"id": m, "name": m, "created": 0} for m in fallback],
                "total": len(fallback),
                "message": f"API call failed. Please check your API key. Using {len(fallback)} common models for {request.provider_key} as fallback.",
                "error": str(e)
            }
        else:
            return {
                "status": "error", 
                "error": str(e),
                "message": f"Failed to fetch models from {request.provider_key}: {str(e)}"
            }


# ---------------------------------------------------------------------------
# Notification channels
# ---------------------------------------------------------------------------
@router.get("/notifications/platforms")
async def list_notification_platforms() -> Dict[str, Any]:
    """List supported SNS platforms with friendly labels."""
    from .notifier import list_platforms
    return {"platforms": list_platforms()}


# ---------------------------------------------------------------------------
# Personal WeChat (iLink Bot API) pairing
# ---------------------------------------------------------------------------
#
# TriForge itself talks to Tencent's iLink Bot API — no bridge daemon,
# no extra process the user has to install. The flow is:
#
#   1. UI: user clicks "Connect Personal WeChat" → opens the wizard.
#   2. TriForge calls iLink GET /ilink/bot/get_bot_qrcode?bot_type=3
#      → returns {qrcode, qrcode_img_content (data:image/png;base64,...)}.
#   3. TriForge stores the qrcode in memory under a 5-minute pair code
#      and returns it to the UI. The wizard shows the QR image
#      (rendered from qrcode_img_content) for the user to scan with
#      WeChat.
#   4. UI polls /pair-status; TriForge long-polls iLink
#      GET /ilink/bot/get_qrcode_status?qrcode=... → returns "wait" /
#      "scaned" / "confirmed" / "expired". On "confirmed" the response
#      includes bot_token + ilink_bot_id + baseurl; TriForge auto-creates
#      a personal_wechat notification channel and stores bot_token
#      locally.
#   5. Subsequent notifications: WeChatBotNotifier calls
#      POST /ilink/bot/sendmessage with the stored bot_token. No extra
#      process, no network hops through a bridge.
#
# Network reachability:
#   - WeChat (the user's phone) does NOT need to reach TriForge.
#   - WeChat talks to Tencent's iLink servers, full stop.
#   - TriForge only needs to reach `https://ilinkai.weixin.qq.com`
#     which is internet-routable. No special firewall config.

import secrets
import time as _time
import threading as _threading

_PERSONAL_WECHAT_PAIRS: Dict[str, Dict[str, Any]] = {}
_PAIR_TTL_SECONDS = 5 * 60
_PAIR_LOCK = _threading.Lock()


def _purge_expired_pairs() -> None:
    cutoff = _time.time() - _PAIR_TTL_SECONDS
    with _PAIR_LOCK:
        for k in [k for k, v in _PERSONAL_WECHAT_PAIRS.items()
                  if v.get("created_at", 0) < cutoff]:
            _PERSONAL_WECHAT_PAIRS.pop(k, None)


@router.post("/notifications/personal-wechat/pair-start")
async def pw_pair_start() -> Dict[str, Any]:
    """Fetch a real iLink login QR for the user to scan with WeChat."""
    _purge_expired_pairs()
    from .wechat_bot import WeChatBot
    code = secrets.token_urlsafe(12)
    try:
        qr = WeChatBot.fetch_qrcode(bot_type=3)
    except Exception as e:
        raise HTTPException(502, f"iLink get_bot_qrcode failed: {e}")
    with _PAIR_LOCK:
        _PERSONAL_WECHAT_PAIRS[code] = {
            "created_at": _time.time(),
            "status":     "pending",
            "qrcode":     qr["qrcode"],
        }
    return {
        "code":              code,
        "expires_in_seconds": _PAIR_TTL_SECONDS,
        # qrcode_img_content is a data:image/png;base64,... URL the
        # UI can drop directly into <img src=...>. Saves us from
        # bundling a QR generator client-side.
        "qrcode_img_content": qr["qrcode_img_content"],
    }


@router.get("/notifications/personal-wechat/pair-status")
async def pw_pair_status(code: str) -> Dict[str, Any]:
    """Long-poll iLink for the user's scan confirmation, then create
    the notification channel automatically on success."""
    _purge_expired_pairs()
    with _PAIR_LOCK:
        rec = _PERSONAL_WECHAT_PAIRS.get(code)
    if not rec:
        return {"code": code, "status": "expired"}

    if rec.get("status") == "paired":
        return {"code": code, "status": "paired", "channel": rec.get("channel")}

    # Long-poll iLink for the user scanning the QR. Use a short poll
    # window (5s) so the wizard's per-call timeout stays bounded;
    # the wizard client polls repeatedly with setInterval().
    from .wechat_bot import WeChatBot
    try:
        status = WeChatBot.poll_status(rec["qrcode"], timeout=5.0)
    except Exception as e:
        # Treat network blip as "still pending" so the UI keeps
        # showing step 2. The next poll retry will recover.
        return {"code": code, "status": "pending", "poll_error": str(e)}

    s = status.get("status")
    if s == "expired":
        with _PAIR_LOCK:
            _PERSONAL_WECHAT_PAIRS.pop(code, None)
        return {"code": code, "status": "expired"}
    if s != "confirmed":
        return {"code": code, "status": "pending"}

    # Confirmed — auto-create the personal_wechat channel and persist
    # the bot credentials.
    bot_token    = status["bot_token"]
    ilink_bot_id = status["ilink_bot_id"]
    baseurl      = status.get("baseurl") or "https://ilinkai.weixin.qq.com"
    account_label = (ilink_bot_id.split("@", 1)[0]
                     if "@" in ilink_bot_id else ilink_bot_id)
    new_channel = {
        "type":           "personal_wechat",
        "enabled":        True,
        "mode":           "simple",
        "bot_token":      bot_token,
        "ilink_bot_id":   ilink_bot_id,
        "baseurl":        baseurl,
        "account_label":  account_label,
        "at_all_on_error": False,
    }
    from .settings import get_settings
    mgr = get_settings()
    cfg = mgr.get()
    channels = cfg.get("notification_channels") or []
    # If a personal_wechat channel for the same ilink_bot_id exists,
    # update it in-place; otherwise append a new one.
    for i, ch in enumerate(channels):
        if ch.get("type") == "personal_wechat" and \
           ch.get("ilink_bot_id") == ilink_bot_id:
            channels[i] = new_channel
            break
    else:
        channels.append(new_channel)
    cfg["notification_channels"] = channels
    mgr.save(cfg)

    with _PAIR_LOCK:
        _PERSONAL_WECHAT_PAIRS[code]["status"] = "paired"
        _PERSONAL_WECHAT_PAIRS[code]["channel"] = new_channel
        _PERSONAL_WECHAT_PAIRS[code]["paired_at"] = _time.time()

    return {"code": code, "status": "paired", "channel": new_channel}


@router.post("/notifications/personal-wechat/pair-cancel")
async def pw_pair_cancel(body: Dict[str, Any]) -> Dict[str, Any]:
    code = (body or {}).get("code") or ""
    with _PAIR_LOCK:
        _PERSONAL_WECHAT_PAIRS.pop(code, None)
    return {"status": "cancelled", "code": code}


class NotificationChannel(BaseModel):
    type: str
    enabled: bool = True
    mode: str = "simple"   # "simple" | "complex"
    webhook_url: Optional[str] = None
    secret: Optional[str] = None
    bot_token: Optional[str] = None
    chat_id: Optional[str] = None
    at_all_on_error: bool = False


@router.post("/notifications/test")
async def test_notification_channel(body: Dict[str, Any]) -> Dict[str, Any]:
    """Send a test message to one channel via its configured creds.

    Body: a full channel dict (as in settings.notification_channels[]).
    Useful for the UI's "Send test" button. The attempt is also recorded
    in /board/notifications/history so the user can verify delivery.
    """
    from .notifier import build_notifier
    try:
        notifier = build_notifier(body)
        notifier.test()
        _record_notification(body.get("type", "?"), "test_send", True,
                             "test message delivered")
        return {"status": "sent"}
    except Exception as e:
        _record_notification(body.get("type", "?"), "test_send", False,
                             f"{type(e).__name__}: {e}")
        raise HTTPException(502, f"notifier delivery failed: {type(e).__name__}: {e}")


@router.get("/notifications/history")
async def recent_notifications(limit: int = 50) -> Dict[str, Any]:
    """Last `limit` notification deliveries (successes + failures).

    Useful for the UI to show whether push is working without watching
    raw platform responses.
    """
    history = list(getattr(_NOTIFICATION_STATE, "history", []))[-limit:]
    return {"history": history}


_NOTIFICATION_STATE = type("S", (), {"history": []})()


def _record_notification(channel_type: str, kind: str, ok: bool, detail: str) -> None:
    """Called by the dispatcher worker; appends to in-memory ring buffer."""
    _NOTIFICATION_STATE.history.append({
        "ts": time.time(),
        "channel": channel_type,
        "kind": kind,
        "ok": ok,
        "detail": detail[:200],
    })
    # cap to a sensible bound
    if len(_NOTIFICATION_STATE.history) > 200:
        del _NOTIFICATION_STATE.history[:-200]


# Patch publish() to record outcomes. Done with a thin wrapper
# module-level so we keep notifier.py publish() clean.
_orig_publish = None


def _instrumented_publish(ev: Any) -> None:
    from .notifier import format_event, build_notifier, _should_send
    settings = get_settings().get()
    channels = settings.get("notification_channels") or []
    for ch in channels:
        if not ch.get("enabled", False):
            continue
        mode = ch.get("mode", "simple")
        if not _should_send(mode, ev.kind):
            continue
        msg = format_event(ev)
        if not msg:
            continue
        try:
            notifier = build_notifier(ch)
            notifier.send(msg)
            _record_notification(ch.get("type", "?"), ev.kind, True, "ok")
        except Exception as e:
            _record_notification(ch.get("type", "?"), ev.kind, False,
                                 f"{type(e).__name__}: {e}")


# Replace the publish function in notifier module. Imported on first use.
def _install_instrumented_publish() -> None:
    import triforge_server.notifier as n
    if getattr(n, "_INSTRUMENTED", False):
        return
    n.publish = _instrumented_publish
    n._INSTRUMENTED = True


_install_instrumented_publish()


# -----------------------------------------------------------------------
# Version Control API
# -----------------------------------------------------------------------
from .version_control import (
    VersionControlManager, PlatformType, GitRepository, PlatformConfig,
    GitHubIntegration, GiteeIntegration, GitLabIntegration, get_integration
)
from .config import WORKSPACE_ROOT

# Global version control manager
vc_manager = VersionControlManager()

class PlatformConfigRequest(BaseModel):
    """平台配置请求"""
    platform: str
    auth_token: str
    username: Optional[str] = None
    email: Optional[str] = None
    git_url: Optional[str] = None

class RepositoryRequest(BaseModel):
    """仓库请求"""
    name: str
    description: str = ""
    platform: str
    private: bool = False

class PushRequest(BaseModel):
    """推送请求"""
    repo_name: str
    commit_message: str = "TriForge auto push"

class PullRequest(BaseModel):
    """拉取请求"""
    repo_name: str
    target_path: str
    branch: str = "main"

@router.get("/version-control/platforms")
async def get_version_control_platforms():
    """获取版本控制平台列表"""
    try:
        platforms = []
        for platform in PlatformType:
            platforms.append({
                "id": platform.value,
                "name": platform.name.title(),
                "description": {
                    "github": "GitHub - 全球最大的代码托管平台",
                    "gitee": "Gitee - 国内领先的代码托管平台",
                    "gitlab": "GitLab - 开源DevOps平台",
                    "custom_git": "自建Git - 任意Git服务器"
                }.get(platform.value, "")
            })
        return {"platforms": platforms}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/version-control/platforms/{platform}/config")
async def get_platform_config(platform: str):
    """获取平台配置"""
    try:
        platform_type = PlatformType(platform)
        config = vc_manager.get_platform_config(platform_type)
        if not config:
            return {"config": None}
        return {"config": {
            "platform": platform.value,
            "api_url": config.api_url,
            "auth_token": config.auth_token,
            "username": config.username,
            "email": config.email,
            "git_url": config.git_url
        }}
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {platform}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/version-control/platforms/{platform}/config")
async def update_platform_config(platform: str, request: PlatformConfigRequest):
    """更新平台配置"""
    try:
        platform_type = PlatformType(platform)
        config = PlatformConfig(
            platform=platform_type,
            api_url={
                "github": "https://api.github.com",
                "gitee": "https://gitee.com/api/v5",
                "gitlab": "https://gitlab.com/api/v4",
                "custom_git": request.git_url or ""
            }.get(platform.value, ""),
            auth_token=request.auth_token,
            username=request.username,
            email=request.email,
            git_url=request.git_url
        )
        
        # 保存到设置
        settings = get_settings()
        settings.update_platform_config(platform.value, {
            "platform": platform.value,
            "api_url": config.api_url,
            "auth_token": config.auth_token,
            "username": config.username,
            "email": config.email,
            "git_url": config.git_url
        })
        
        # 保存到版本控制管理器
        vc_manager.add_platform_config(config)
        
        return {"success": True, "message": f"Platform {platform} configuration updated"}
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {platform}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/version-control/repositories")
async def get_repositories(platform: Optional[str] = None):
    """获取仓库列表"""
    try:
        if platform:
            platform_type = PlatformType(platform)
            repositories = vc_manager.list_repositories(platform_type)
        else:
            repositories = vc_manager.list_repositories()
        
        return {"repositories": [
            {
                "name": repo.name,
                "full_name": repo.full_name,
                "description": repo.description,
                "html_url": repo.html_url,
                "clone_url": repo.clone_url,
                "default_branch": repo.default_branch,
                "platform": repo.platform.value,
                "owner": repo.owner,
                "private": repo.private
            }
            for repo in repositories
        ]}
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {platform}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/version-control/repositories")
async def create_repository(request: RepositoryRequest):
    """创建新仓库"""
    try:
        platform_type = PlatformType(request.platform)
        config = vc_manager.get_platform_config(platform_type)
        
        if not config:
            raise HTTPException(status_code=400, detail=f"Platform {request.platform} not configured")
        
        # 获取集成实例
        integration = get_integration(platform_type, config)
        if not integration:
            raise HTTPException(status_code=400, detail=f"Platform {request.platform} integration not supported")
        
        # 创建仓库
        repo_data = integration.create_repo(request.name, request.description, request.private)
        if not repo_data:
            raise HTTPException(status_code=400, detail=f"Failed to create repository {request.name}")
        
        # 添加到版本控制管理器
        repo = GitRepository(
            name=repo_data["name"],
            full_name=repo_data["full_name"],
            description=repo_data.get("description", ""),
            html_url=repo_data["html_url"],
            clone_url=repo_data["clone_url"],
            default_branch=repo_data.get("default_branch", "main"),
            platform=platform_type,
            owner=repo_data["owner"]["login"],
            private=request.private
        )
        
        vc_manager.add_repository(repo)
        
        return {"success": True, "repository": {
            "name": repo.name,
            "full_name": repo.full_name,
            "description": repo.description,
            "html_url": repo.html_url,
            "clone_url": repo.clone_url,
            "default_branch": repo.default_branch,
            "platform": repo.platform.value,
            "owner": repo.owner,
            "private": repo.private
        }}
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {request.platform}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/version-control/repositories/{repo_name}")
async def delete_repository(repo_name: str):
    """删除仓库"""
    try:
        success = vc_manager.remove_repository(repo_name)
        if success:
            return {"success": True, "message": f"Repository {repo_name} deleted"}
        else:
            raise HTTPException(status_code=404, detail=f"Repository {repo_name} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/version-control/push")
async def push_project(request: PushRequest):
    """推送项目到远程仓库"""
    try:
        project_path = WORKSPACE_ROOT
        success = vc_manager.push_project_to_repo(project_path, request.repo_name, request.commit_message)
        
        if success:
            return {"success": True, "message": f"Project pushed to repository {request.repo_name}"}
        else:
            raise HTTPException(status_code=400, detail=f"Failed to push project to repository {request.repo_name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/version-control/pull")
async def pull_project(request: PullRequest):
    """从远程仓库拉取项目"""
    try:
        target_path = Path(request.target_path)
        success = vc_manager.pull_project_from_repo(request.repo_name, target_path, request.branch)
        
        if success:
            return {"success": True, "message": f"Project pulled from repository {request.repo_name}"}
        else:
            raise HTTPException(status_code=400, detail=f"Failed to pull project from repository {request.repo_name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/version-control/platforms/{platform}/repositories")
async def get_platform_repositories(platform: str):
    """获取平台上的仓库列表"""
    try:
        platform_type = PlatformType(platform)
        config = vc_manager.get_platform_config(platform_type)
        
        if not config:
            raise HTTPException(status_code=400, detail=f"Platform {platform} not configured")
        
        # 获取集成实例
        integration = get_integration(platform_type, config)
        if not integration:
            raise HTTPException(status_code=400, detail=f"Platform {platform} integration not supported")
        
        # 获取用户仓库
        repositories = integration.get_user_repos()
        
        return {"repositories": [
            {
                "name": repo["name"],
                "full_name": repo["full_name"],
                "description": repo.get("description", ""),
                "html_url": repo["html_url"],
                "clone_url": repo["clone_url"],
                "default_branch": repo.get("default_branch", "main"),
                "owner": repo["owner"]["login"],
                "private": repo.get("private", False)
            }
            for repo in repositories
        ]}
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {platform}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/version-control/platforms/{platform}/user-info")
async def get_platform_user_info(platform: str):
    """获取平台用户信息"""
    try:
        platform_type = PlatformType(platform)
        config = vc_manager.get_platform_config(platform_type)
        
        if not config:
            raise HTTPException(status_code=400, detail=f"Platform {platform} not configured")
        
        # 获取集成实例
        integration = get_integration(platform_type, config)
        if not integration:
            raise HTTPException(status_code=400, detail=f"Platform {platform} integration not supported")
        
        # 获取用户信息
        user_info = integration.get_user_info()
        
        if user_info:
            return {"user_info": {
                "username": user_info.get("login"),
                "name": user_info.get("name"),
                "email": user_info.get("email"),
                "avatar_url": user_info.get("avatar_url")
            }}
        else:
            raise HTTPException(status_code=400, detail=f"Failed to get user info from platform {platform}")
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {platform}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
