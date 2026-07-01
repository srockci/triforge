"""Run/event store backed by SQLite (see persistence.py).

This module wraps BoardDB with the interface that workflow.py + board.py
already use:
  - append(ev: BoardEvent)
  - update_snapshot(run_id, snapshot_dict)
  - replay(run_id, since_ts=0.0) -> list of BoardEvent-like dicts
  - snapshot(run_id) -> dict or None
  - known_runs() -> list of snapshot dicts (for the kanban)
  - restore_from_db(engine) -> put any DB-persisted runs into the
    in-memory WorkflowEngine so the server picks them up after restart

Thread-safety is delegated to BoardDB.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from .events import BoardEvent
from .persistence import get_db


def _event_from_row(ts: float, kind: str, data: Dict[str, Any], run_id: str) -> BoardEvent:
    """Reconstruct a BoardEvent from a persisted row."""
    return BoardEvent(run_id=run_id, kind=kind, ts=ts, data=data)


class RunStore:
    """SQLite-backed store. Each method persists to disk."""

    def __init__(self) -> None:
        self._db = get_db()

    def append(self, ev: BoardEvent) -> None:
        try:
            self._db.append_event(ev.run_id, ev.ts, ev.kind, ev.data)
        except Exception:
            pass  # persistence is non-critical for live operations

    def update_snapshot(self, run_id: str, snapshot: Dict[str, Any]) -> None:
        try:
            # snapshot already has the right shape (from _snapshot_for_board).
            # We need to make sure it has all the keys the DB expects.
            row = {
                "run_id": run_id,
                "status": snapshot.get("status", "running"),
                "phase": snapshot.get("phase", "design"),
                "requirement": snapshot.get("requirement", ""),
                "pending_tool": snapshot.get("pending_tool"),
                "pending_args": snapshot.get("pending_args"),
                "pending_preview": "",  # not stored in the board snapshot
                "outputs": snapshot.get("outputs") or {},
                "error": snapshot.get("error"),
                "created_at": snapshot.get("created_at", time.time()),
                "updated_at": snapshot.get("updated_at", time.time()),
            }
            self._db.upsert_run(row)
        except Exception:
            pass

    def replay(self, run_id: str, since_ts: float = 0.0) -> List[BoardEvent]:
        rows = self._db.load_events(run_id, since_ts=since_ts)
        return [_event_from_row(ts, kind, data, run_id) for ts, kind, data in rows]

    def snapshot(self, run_id: str) -> Optional[Dict[str, Any]]:
        return self._db.get_run(run_id)

    def known_runs(self) -> List[Dict[str, Any]]:
        """Return a board-friendly list (same shape as the kanban snapshot).

        The DB stores the canonical row; we add phase_index and a few
        convenience fields the frontend expects.
        """
        rows = self._db.load_runs()
        phase_to_idx = {"design": 0, "implement": 1, "review": 2, "done": 3}
        out = []
        for r in rows:
            r["phase_index"] = phase_to_idx.get(r.get("phase", "design"), 0)
            out.append(r)
        return out

    # ----- restore helper -----
    def restore_to_engine(self, engine) -> Dict[str, int]:
        """After server start, re-create RunState objects for any
        persisted runs so the in-memory engine knows about them.

        For runs in awaiting_approval at the time of crash: the agent
        generator state is lost, so we cannot resume the pipeline. We
        mark these runs as "interrupted" so the UI can tell the user to
        re-approve. Re-approving an interrupted run restarts that phase
        from scratch.

        Returns counts: {"restored": N, "interrupted": M}.
        """
        import asyncio
        from .workflow import RunState

        restored = 0
        interrupted = 0
        for snap in self._db.load_runs():
            if snap["run_id"] in engine.runs:
                continue  # already loaded
            status = snap.get("status", "running")
            # If the server died while awaiting approval, we cannot
            # resume the in-flight generator. Mark as interrupted.
            if status == "awaiting_approval":
                status = "interrupted"
                interrupted += 1
                # Update the DB row to reflect "interrupted" before we
                # construct the in-memory RunState, so we read the
                # updated error message below.
                self._db.upsert_run({
                    "run_id": snap["run_id"],
                    "status": "interrupted",
                    "phase": snap.get("phase", "design"),
                    "requirement": snap.get("requirement", ""),
                    "pending_tool": snap.get("pending_tool"),
                    "pending_args": snap.get("pending_args"),
                    "pending_preview": snap.get("pending_preview") or "",
                    "outputs": snap.get("outputs") or {},
                    "error": "Server restarted while awaiting approval — please re-approve to retry this phase",
                    "created_at": snap.get("created_at") or time.time(),
                    "updated_at": time.time(),
                })
                # Re-read so the snapshot we build the RunState from has
                # the new status + error.
                snap = self._db.get_run(snap["run_id"]) or snap
            run = RunState(
                run_id=snap["run_id"],
                requirement=snap.get("requirement", ""),
                phase=snap.get("phase", "design"),
                status=status,
                pending_tool=snap.get("pending_tool"),
                pending_args=snap.get("pending_args"),
                pending_preview=snap.get("pending_preview") or "",
                outputs=snap.get("outputs") or {},
                error=snap.get("error"),
                created_at=snap.get("created_at") or time.time(),
                updated_at=snap.get("updated_at") or time.time(),
            )
            # Re-create the resume_event so any future approve is a no-op
            # (since there's no listener for interrupted runs).
            run.resume_event = asyncio.Event()
            engine.runs[run.run_id] = run
            restored += 1
        return {"restored": restored, "interrupted": interrupted}


# Module-level singleton (initialized lazily).
_store: Optional[RunStore] = None


def get_store() -> RunStore:
    global _store
    if _store is None:
        _store = RunStore()
    return _store


def set_store(store: RunStore) -> None:
    """Override the singleton (used by tests)."""
    global _store
    _store = store