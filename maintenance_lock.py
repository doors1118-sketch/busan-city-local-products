"""Shared maintenance lock, write fence, and SQLite generation helpers."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - exercised through injected fallbacks on Windows
    fcntl = None


class WriteFenceError(RuntimeError):
    pass


class CheckpointError(RuntimeError):
    pass


_thread_locks: dict[Path, threading.Lock] = {}
_thread_locks_guard = threading.Lock()


def _thread_lock(path: Path) -> threading.Lock:
    with _thread_locks_guard:
        return _thread_locks.setdefault(path, threading.Lock())


@contextmanager
def maintenance_lock(path: str | Path, timeout_seconds: float, *, fallback: Any = None) -> Iterator[Path]:
    """Acquire the one resolved maintenance path, timing out rather than waiting forever."""
    resolved = Path(path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    thread_lock = _thread_lock(resolved)
    if not thread_lock.acquire(timeout=max(timeout_seconds, 0)):
        raise TimeoutError(f"timed out waiting for maintenance lock: {resolved}")
    handle = resolved.open("a+b")
    locked = False
    deadline = time.monotonic() + timeout_seconds
    try:
        while not locked:
            try:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                elif fallback is not None:
                    fallback.acquire(handle)
                locked = True
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out waiting for maintenance lock: {resolved}")
                time.sleep(0.01)
        yield resolved
    finally:
        if locked:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            elif fallback is not None:
                fallback.release(handle)
        handle.close()
        thread_lock.release()


def _activation_row(conn: sqlite3.Connection) -> tuple[int, str | None, int] | None:
    return conn.execute(
        "SELECT writes_enabled, active_generation_id, ever_snapshot_activated "
        "FROM locality_activation_state WHERE singleton_id = 1"
    ).fetchone()


def _assert_writes_open(
    conn: sqlite3.Connection,
    *,
    journal_path: str | Path | None,
    marker_path: str | Path | None,
    pointer_path: str | Path | None,
    peer_conn: sqlite3.Connection | None,
) -> None:
    for path in (journal_path, marker_path):
        if path is not None and Path(path).exists():
            raise WriteFenceError("locality writes are paused")
    row = _activation_row(conn)
    if row is None or not row[0]:
        raise WriteFenceError("persisted locality write fence is closed")
    if peer_conn is not None:
        peer = _activation_row(peer_conn)
        if peer is None or not peer[0] or peer[1] != row[1]:
            raise WriteFenceError("locality activation rows disagree")
    if pointer_path is not None and Path(pointer_path).exists():
        try:
            pointer = json.loads(Path(pointer_path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise WriteFenceError("activation pointer is unreadable") from error
        generation = pointer.get("active_generation_id", pointer.get("generation_id"))
        if generation != row[1]:
            raise WriteFenceError("activation pointer and database disagree")


@contextmanager
def guarded_write_session(
    conn: sqlite3.Connection,
    *,
    journal_path: str | Path | None = None,
    marker_path: str | Path | None = None,
    pointer_path: str | Path | None = None,
    peer_conn: sqlite3.Connection | None = None,
) -> Iterator[sqlite3.Connection]:
    """Fence-check before an explicit write transaction; rollback is automatic on failure."""
    _assert_writes_open(
        conn,
        journal_path=journal_path,
        marker_path=marker_path,
        pointer_path=pointer_path,
        peer_conn=peer_conn,
    )
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()


def set_write_fence(
    conn: sqlite3.Connection,
    writes_enabled: bool,
    operator: str,
    reason: str,
    *,
    active_generation_id: str | None = None,
    ever_snapshot_activated: bool | None = None,
) -> None:
    """Persist a fence transition and its audit in one SQLite transaction."""
    from company_locality import ensure_locality_schema

    ensure_locality_schema(conn)
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    with conn:
        current = _activation_row(conn)
        if current is None:
            raise WriteFenceError("activation state is missing")
        generation = active_generation_id if active_generation_id is not None else current[1]
        activated = int(ever_snapshot_activated) if ever_snapshot_activated is not None else current[2]
        conn.execute(
            "UPDATE locality_activation_state SET writes_enabled=?, active_generation_id=?, "
            "ever_snapshot_activated=?, updated_at=? WHERE singleton_id=1",
            (int(writes_enabled), generation, activated, now),
        )
        conn.execute(
            "INSERT INTO locality_fence_audit (writes_enabled, operator, reason, changed_at) VALUES (?, ?, ?, ?)",
            (int(writes_enabled), operator, reason, now),
        )


def read_data_generation(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT data_generation FROM locality_generation_clock WHERE singleton_id = 1"
    ).fetchone()
    if row is None:
        raise RuntimeError("locality generation clock is missing")
    return int(row[0])


def read_control_revision(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT control_revision FROM locality_generation_clock WHERE singleton_id = 1"
    ).fetchone()
    if row is None:
        raise RuntimeError("locality generation clock is missing")
    return int(row[0])


def checkpoint_wal(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """Truncate WAL only when SQLite reports a fully successful checkpoint."""
    result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    row = result.fetchone() if hasattr(result, "fetchone") else result[0]
    values = tuple(int(value) for value in row)
    if len(values) != 3 or values[0] != 0 or values[1] != 0:
        raise CheckpointError(f"WAL checkpoint did not complete: {values}")
    return values  # type: ignore[return-value]
