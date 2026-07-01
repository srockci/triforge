"""SQLite persistence for runs + events.

Replaces the in-memory store with a SQLite DB so the server can
restart without losing run state.

Schema:
  runs(
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    requirement TEXT NOT NULL,
    pending_tool TEXT,
    pending_args TEXT,           -- JSON
    pending_preview TEXT,
    outputs TEXT,                -- JSON
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
  )

  events(
    run_id TEXT NOT NULL,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    data TEXT NOT NULL,           -- JSON
    PRIMARY KEY (run_id, ts, kind)
  )

We store events as flat rows (not a single blob per run) so the SSE
replay path can do `WHERE run_id = ? AND ts > ?` queries.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_DB_PATH = os.environ.get(
    "OPENMANUS_DB_PATH",
    "/root/openmanus-integration/data/board.db",
)


class BoardDB:
    """Thread-safe SQLite wrapper."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False: we serialize via a lock anyway, but
        # SQLite's connection is per-thread by default. We use a single
        # connection guarded by a re-entrant lock for simplicity.
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None,
            timeout=30.0,
        )
        self._lock = threading.RLock()
        self._init_schema()

    # ----- schema -----
    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    requirement TEXT NOT NULL,
                    pending_tool TEXT,
                    pending_args TEXT,
                    pending_preview TEXT,
                    outputs TEXT,
                    error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    run_id TEXT NOT NULL,
                    ts REAL NOT NULL,
                    kind TEXT NOT NULL,
                    data TEXT NOT NULL,
                    PRIMARY KEY (run_id, ts, kind)
                );

                CREATE INDEX IF NOT EXISTS idx_events_run
                    ON events(run_id, ts);
            """)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ----- runs -----
    def upsert_run(self, run: Dict[str, Any]) -> None:
        """Insert or update a run row."""
        with self._lock:
            self._conn.execute("""
                INSERT INTO runs (
                    run_id, status, phase, requirement,
                    pending_tool, pending_args, pending_preview,
                    outputs, error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status=excluded.status,
                    phase=excluded.phase,
                    requirement=excluded.requirement,
                    pending_tool=excluded.pending_tool,
                    pending_args=excluded.pending_args,
                    pending_preview=excluded.pending_preview,
                    outputs=excluded.outputs,
                    error=excluded.error,
                    created_at=excluded.created_at,
                    updated_at=excluded.updated_at
            """, (
                run["run_id"],
                run.get("status", "running"),
                run.get("phase", "design"),
                run.get("requirement", ""),
                run.get("pending_tool"),
                json.dumps(run.get("pending_args")) if run.get("pending_args") is not None else None,
                run.get("pending_preview"),
                json.dumps(run.get("outputs") or {}),
                run.get("error"),
                run.get("created_at", time.time()),
                run.get("updated_at", time.time()),
            ))

    def load_runs(self) -> List[Dict[str, Any]]:
        """Return every persisted run as a board-friendly dict."""
        with self._lock:
            cur = self._conn.execute("""
                SELECT run_id, status, phase, requirement,
                       pending_tool, pending_args, pending_preview,
                       outputs, error, created_at, updated_at
                FROM runs
                ORDER BY updated_at DESC
            """)
            rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            args = json.loads(r[5]) if r[5] else None
            outputs = json.loads(r[7]) if r[7] else {}
            out.append({
                "run_id": r[0],
                "status": r[1],
                "phase": r[2],
                "requirement": r[3],
                "pending_tool": r[4],
                "pending_args": args,
                "pending_preview": r[6],
                "outputs": outputs,
                "error": r[8],
                "created_at": r[9],
                "updated_at": r[10],
            })
        return out

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute("""
                SELECT run_id, status, phase, requirement,
                       pending_tool, pending_args, pending_preview,
                       outputs, error, created_at, updated_at
                FROM runs WHERE run_id = ?
            """, (run_id,))
            row = cur.fetchone()
        if not row:
            return None
        args = json.loads(row[5]) if row[5] else None
        outputs = json.loads(row[7]) if row[7] else {}
        return {
            "run_id": row[0],
            "status": row[1],
            "phase": row[2],
            "requirement": row[3],
            "pending_tool": row[4],
            "pending_args": args,
            "pending_preview": row[6],
            "outputs": outputs,
            "error": row[8],
            "created_at": row[9],
            "updated_at": row[10],
        }

    # ----- events -----
    def append_event(self, run_id: str, ts: float, kind: str,
                     data: Dict[str, Any]) -> None:
        with self._lock:
            # Use INSERT OR IGNORE so re-emitting the same event is a no-op.
            # We keep (run_id, ts, kind) as the dedup key.
            self._conn.execute("""
                INSERT OR IGNORE INTO events (run_id, ts, kind, data)
                VALUES (?, ?, ?, ?)
            """, (run_id, ts, kind, json.dumps(data, default=str)))

    def load_events(self, run_id: str, since_ts: float = 0.0,
                    limit: int = 5000) -> List[Tuple[float, str, Dict[str, Any]]]:
        """Return events for a run in chronological order."""
        with self._lock:
            cur = self._conn.execute("""
                SELECT ts, kind, data FROM events
                WHERE run_id = ? AND ts > ?
                ORDER BY ts ASC
                LIMIT ?
            """, (run_id, since_ts, limit))
            rows = cur.fetchall()
        out: List[Tuple[float, str, Dict[str, Any]]] = []
        for ts, kind, data in rows:
            try:
                out.append((ts, kind, json.loads(data)))
            except json.JSONDecodeError:
                continue
        return out

    def count_events(self, run_id: str) -> int:
        with self._lock:
            cur = self._conn.execute(
                "SELECT COUNT(*) FROM events WHERE run_id = ?", (run_id,)
            )
            return cur.fetchone()[0]

    def delete_run(self, run_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM events WHERE run_id = ?", (run_id,))
            self._conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))

    def clear_all(self) -> None:
        """For tests only — wipe everything."""
        with self._lock:
            self._conn.execute("DELETE FROM events")
            self._conn.execute("DELETE FROM runs")


# Singleton accessor
_db: Optional[BoardDB] = None


def get_db() -> BoardDB:
    global _db
    if _db is None:
        _db = BoardDB()
    return _db


def set_db(db: BoardDB) -> None:
    """Override the singleton (used by tests)."""
    global _db
    _db = db