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
    subscribe; for historical events use RunStore.replay()."""

    def __init__(self) -> None:
        # run_id -> list of subscriber queues
        self._subs: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        # bookkeeping
        self._lock = asyncio.Lock()

    def subscribe(self, run_id: str) -> asyncio.Queue:
        """Create a new subscription queue for a run. Caller MUST drain
        it (or eventually call unsubscribe) to avoid memory leak."""
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subs[run_id].append(q)
        return q

    def unsubscribe(self, run_id: str, q: asyncio.Queue) -> None:
        if run_id in self._subs and q in self._subs[run_id]:
            self._subs[run_id].remove(q)
            if not self._subs[run_id]:
                del self._subs[run_id]

    def emit(self, event: BoardEvent) -> None:
        """Synchronous emit. Pushes to all subscriber queues; if a queue
        is full we drop the OLDEST event to make room (FIFO overflow).

        We accept that subscribers may miss events if they're slow —
        the board also pulls from RunStore to backfill."""
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


# Single global bus — workflow.py imports this
bus = EventBus()