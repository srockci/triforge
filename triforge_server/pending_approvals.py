"""In-memory pending approval store with TTL cleanup."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("triforge.telegram.pending")


@dataclass
class PendingApproval:
    short_run_id: str
    full_run_id: str
    phase: str
    file_path: str
    file_hash: str
    chat_id: int
    message_id: int
    created_at: float


class PendingApprovalStore:
    """In-memory store keyed by (short_run_id, file_hash).

    TTL: 30 minutes. Background daemon thread cleans expired entries
    every 60 seconds.
    """

    TTL_SECONDS = 30 * 60

    def __init__(self):
        self._store: Dict[Tuple[str, str], PendingApproval] = {}
        self._lock = threading.Lock()
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop, daemon=True,
            name="pending-approvals-cleanup",
        )
        self._cleanup_thread.start()

    def add(self, pending: PendingApproval) -> None:
        with self._lock:
            self._store[(pending.short_run_id, pending.file_hash)] = pending

    def get(self, short_run_id: str, file_hash: str) -> Optional[PendingApproval]:
        with self._lock:
            p = self._store.get((short_run_id, file_hash))
            if not p:
                return None
            if time.time() - p.created_at > self.TTL_SECONDS:
                self._store.pop((short_run_id, file_hash), None)
                return None
            return p

    def consume(self, short_run_id: str, file_hash: str) -> None:
        with self._lock:
            self._store.pop((short_run_id, file_hash), None)

    def get_file_path(self, short_run_id: str, file_hash: str) -> Optional[str]:
        with self._lock:
            p = self._store.get((short_run_id, file_hash))
            return p.file_path if p else None

    def _cleanup_loop(self) -> None:
        while True:
            time.sleep(60)
            cutoff = time.time() - self.TTL_SECONDS
            with self._lock:
                expired = [k for k, v in self._store.items()
                          if v.created_at < cutoff]
                for k in expired:
                    self._store.pop(k, None)
                if expired:
                    log.info("cleaned up %d expired pending approvals", len(expired))

    def list_pending(self) -> List[PendingApproval]:
        with self._lock:
            return list(self._store.values())
