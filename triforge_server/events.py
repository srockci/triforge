"""In-process pub-sub event bus for the board.

The workflow emits events at every meaningful transition (phase start,
tool call, approval gate, phase end, run end). The board's SSE endpoint
subscribes and streams them to connected browsers.

This is intentionally lightweight — no external broker. If we ever need
cross-process pub-sub, swap the implementation here; the emit/subscribe
interface stays the same.

Thread-safety: emits are non-blocking. Subscribers receive events via
asyncio.Queue, so they MUST be in an async context.
"""
from __future__ import annotations

import asyncio
import queue
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from typing import Any, AsyncIterator, Dict, List, Optional


@dataclass
class BoardEvent:
    """A single event in the run's timeline."""
    run_id: str
    kind: str            # e.g. "phase_start", "tool_call", "approval", "phase_end", "run_end"
    ts: float = field(default_factory=time.time)
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d


class EventBus:
    """Per-run pub-sub. Subscribers get events from the moment they
    subscribe; for historical events use RunStore.replay().
    
    Also supports a global subscription channel (`subscribe_global`)
    that delivers every event regardless of run_id — used by the
    notification dispatcher worker.
    """

    def __init__(self) -> None:
        # run_id -> list of per-run subscriber queues (asyncio, for SSE)
        self._subs: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        # global subscribers (sync queues, for background workers)
        self._global_subs: List["queue.Queue[BoardEvent]"] = []
        # bookkeeping
        self._lock = asyncio.Lock()

    def subscribe(self, run_id: str) -> asyncio.Queue:
        """Create a new per-run asyncio subscription queue. Caller MUST
        drain it (or eventually call unsubscribe) to avoid memory leak."""
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subs[run_id].append(q)
        return q

    def subscribe_global(self, q: "queue.Queue[BoardEvent]") -> None:
        """Subscribe to ALL events regardless of run_id. The dispatcher
        worker uses this. `q` is a sync queue.Queue (background thread)."""
        self._global_subs.append(q)

    def unsubscribe_global(self, q: "queue.Queue[BoardEvent]") -> None:
        if q in self._global_subs:
            self._global_subs.remove(q)

    def unsubscribe(self, run_id: str, q: asyncio.Queue) -> None:
        if run_id in self._subs and q in self._subs[run_id]:
            self._subs[run_id].remove(q)
            if not self._subs[run_id]:
                del self._subs[run_id]

    def emit(self, event: BoardEvent) -> None:
        """Synchronous emit. Pushes to all subscriber queues; if a queue
        is full we drop the OLDEST event to make room (FIFO overflow).
        
        Also fans out to global subscribers (notification dispatcher).
        """
        # Per-run asyncio subscribers (SSE endpoints, live stream UI)
        for q in list(self._subs.get(event.run_id, [])):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # drop oldest, push new
                try:
                    q.get_nowait()
                    q.put_nowait(event)
                except Exception:
                    pass
        # Global subscribers (sync queues, background workers)
        for q in list(self._global_subs):
            try:
                q.put_nowait(event)
            except queue.Full:
                try:
                    q.get_nowait()
                    q.put_nowait(event)
                except Exception:
                    pass


# Single global bus — workflow.py imports this
bus = EventBus()