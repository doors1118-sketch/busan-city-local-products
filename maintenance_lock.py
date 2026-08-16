"""Shared maintenance lock, mandatory write sessions, and generation helpers."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from typing import Any, Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows uses the injectable fallback.
    fcntl = None


class WriteFenceError(RuntimeError):
    pass


class CheckpointError(RuntimeError):
    pass


@dataclass(frozen=True)
class LocalityPaths:
    maintenance_path: Path
    journal_path: Path
    marker_path: Path
    pointer_path: Path


_thread_locks: dict[Path, threading.Lock] = {}
_thread_locks_guard = threading.Lock()
_UNSET = object()


def _thread_lock(path: Path) -> threading.Lock:
    with _thread_locks_guard:
        return _thread_locks.setdefault(path, threading.Lock())


def _database_path(conn: sqlite3.Connection) -> Path | None:
    for _, name, path in conn.execute("PRAGMA database_list"):
        if name == "main" and path:
            return Path(path).resolve()
    return None


def locality_paths(conn: sqlite3.Connection) -> LocalityPaths:
    """Derive the one shared transition path set for a database directory."""
    database_path = _database_path(conn)
    root = database_path.parent if database_path else Path(tempfile.gettempdir())
    return LocalityPaths(
        maintenance_path=root / "locality_maintenance.lock",
        journal_path=root / "locality_transition.json",
        marker_path=root / "locality_writes_paused",
        pointer_path=root / "active_locality_generation.json",
    )


def install_write_guard(conn: sqlite3.Connection) -> dict[str, int]:
    """Register a connection-local authorizer used by protected-table triggers."""
    state = {"mode": 0}
    conn.create_function("locality_guarded_write", 0, lambda: state["mode"])
    return state


@contextmanager
def maintenance_write_permission(conn: sqlite3.Connection, *, fence_admin: bool = False) -> Iterator[None]:
    """Temporarily authorize a configured connection while it owns a write transaction."""
    state = install_write_guard(conn)
    previous = state["mode"]
    state["mode"] = 2 if fence_admin else 1
    try:
        yield
    finally:
        state["mode"] = previous


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
    journal_path: Path,
    marker_path: Path,
    pointer_path: Path,
    peer_conn: sqlite3.Connection | None,
) -> None:
    if journal_path.exists() or marker_path.exists():
        raise WriteFenceError("locality writes are paused")
    row = _activation_row(conn)
    if row is None or not row[0]:
        raise WriteFenceError("persisted locality write fence is closed")
    if peer_conn is not None:
        peer = _activation_row(peer_conn)
        if peer is None or not peer[0] or peer[1] != row[1]:
            raise WriteFenceError("locality activation rows disagree")
    if pointer_path.exists():
        try:
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
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
    maintenance_path: str | Path | None = None,
    timeout_seconds: float = 5,
    acquire_lock: bool = True,
) -> Iterator[sqlite3.Connection]:
    """Open the only supported write path after fence and peer checks pass."""
    install_write_guard(conn)
    defaults = locality_paths(conn)
    resolved_journal = Path(journal_path) if journal_path is not None else defaults.journal_path
    resolved_marker = Path(marker_path) if marker_path is not None else defaults.marker_path
    resolved_pointer = Path(pointer_path) if pointer_path is not None else defaults.pointer_path
    resolved_lock = Path(maintenance_path) if maintenance_path is not None else defaults.maintenance_path
    lock_context = maintenance_lock(resolved_lock, timeout_seconds) if acquire_lock else nullcontext()
    with lock_context:
        _assert_writes_open(
            conn,
            journal_path=resolved_journal,
            marker_path=resolved_marker,
            pointer_path=resolved_pointer,
            peer_conn=peer_conn,
        )
        started_transaction = not conn.in_transaction
        if started_transaction:
            conn.execute("BEGIN IMMEDIATE")
        try:
            with maintenance_write_permission(conn):
                yield conn
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()


def _set_write_fence_in_transaction(
    conn: sqlite3.Connection,
    writes_enabled: bool,
    operator: str,
    reason: str,
    *,
    active_generation_id: str | None | object = _UNSET,
    ever_snapshot_activated: bool | None = None,
) -> None:
    current = _activation_row(conn)
    if current is None:
        raise WriteFenceError("activation state is missing")
    generation = current[1] if active_generation_id is _UNSET else active_generation_id
    activated = int(ever_snapshot_activated) if ever_snapshot_activated is not None else current[2]
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO locality_fence_audit (writes_enabled, operator, reason, changed_at) VALUES (?, ?, ?, ?)",
        (int(writes_enabled), operator, reason, now),
    )
    conn.execute(
        "UPDATE locality_activation_state SET writes_enabled=?, active_generation_id=?, "
        "ever_snapshot_activated=?, updated_at=? WHERE singleton_id=1",
        (int(writes_enabled), generation, activated, now),
    )


def set_write_fence(
    conn: sqlite3.Connection,
    writes_enabled: bool,
    operator: str,
    reason: str,
    *,
    active_generation_id: str | None | object = _UNSET,
    ever_snapshot_activated: bool | None = None,
) -> None:
    """Single-database primitive; dual-database transitions use the admin command."""
    from company_locality import ensure_locality_schema

    ensure_locality_schema(conn)
    with maintenance_lock(locality_paths(conn).maintenance_path, 5):
        conn.execute("BEGIN IMMEDIATE")
        try:
            with maintenance_write_permission(conn, fence_admin=True):
                _set_write_fence_in_transaction(
                    conn,
                    writes_enabled,
                    operator,
                    reason,
                    active_generation_id=active_generation_id,
                    ever_snapshot_activated=ever_snapshot_activated,
                )
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()


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
