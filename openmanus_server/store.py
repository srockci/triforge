"""In-memory event & run store for the board.

Persists BoardEvents as they fire so:
  - The board can show full event history even for runs that finished
    before the page was opened.
  - Server restart can (in P4) recover event timeline.

For now this is in-process only. P4 will swap to SQLite.
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional

from .events import BoardEvent


class RunStore:
    """Thread-safe in-memory store."""

    def __init__(self, max_events_per_run: int = 5000) -> None:
        # run_id -> ordered list of BoardEvent
        self._events: Dict[str, Deque[BoardEvent]] = defaultdict(
            lambda: deque(maxlen=max_events_per_run)
        )
        # run_id -> summary snapshot (last known state)
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def append(self, ev: BoardEvent) -> None:
        with self._lock:
            self._events[ev.run_id].append(ev)

    def update_snapshot(self, run_id: str, snapshot: Dict[str, Any]) -> None:
        with self._lock:
            self._snapshots[run_id] = snapshot

    def replay(self, run_id: str, since_ts: float = 0.0) -> List[BoardEvent]:
        with self._lock:
            return [ev for ev in self._events.get(run_id, []) if ev.ts > since_ts]

    def snapshot(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._snapshots.get(run_id)

    def known_runs(self) -> List[Dict[str, Any]]:
        """Return a board-friendly list of all runs with their last known
        status — used by the kanban."""
        with self._lock:
            out = []
            for run_id, snap in self._snapshots.items():
                out.append(snap)
            return out


store = RunStore()