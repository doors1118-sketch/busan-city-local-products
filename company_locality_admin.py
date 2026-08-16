"""Audited supplier-conflict and dual-database write-transition commands."""

from __future__ import annotations

from contextlib import ExitStack, nullcontext
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Callable

from company_locality import Resolution, resolve_company_conflict
from locality_quiesce import assert_databases_quiesced, dual_exclusive_transition
from maintenance_lock import (
    LocalityPaths,
    _activation_row,
    _set_write_fence_in_transaction,
    maintenance_lock,
    maintenance_write_permission,
    require_locality_paths,
)


class TransitionError(RuntimeError):
    pass


def _database_path(conn: sqlite3.Connection) -> Path:
    for _, name, path in conn.execute("PRAGMA database_list"):
        if name == "main" and path:
            return Path(path).resolve()
    raise TransitionError("dual-database transitions require file-backed SQLite databases")


def _fsync_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _remove_fsynced(path: Path) -> None:
    if path.exists():
        path.unlink()
        _fsync_directory(path.parent)


def _pointer_generation(pointer_path: Path) -> str | None:
    if not pointer_path.exists():
        return None
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise TransitionError("activation pointer is unreadable") from error
    if not isinstance(pointer, dict):
        raise TransitionError("activation pointer must be an object")
    return pointer.get("active_generation_id", pointer.get("generation_id"))


def _activation_rows_agree(
    company_conn: sqlite3.Connection,
    procurement_conn: sqlite3.Connection,
    generation_id: str | None,
    *,
    writes_enabled: bool | None = None,
) -> None:
    company = _activation_row(company_conn)
    procurement = _activation_row(procurement_conn)
    if company is None or procurement is None:
        raise TransitionError("activation state is missing")
    if company[1] != generation_id or procurement[1] != generation_id:
        raise TransitionError("activation rows do not match the authoritative pointer")
    if writes_enabled is not None and (bool(company[0]) != writes_enabled or bool(procurement[0]) != writes_enabled):
        raise TransitionError("activation rows do not agree on the write fence")


def _fail_if_requested(fail_at: str | None, boundary: str) -> None:
    if fail_at == boundary:
        raise RuntimeError(f"injected failure at {boundary}")


def _write_journal(
    journal_path: Path,
    *,
    operation: str,
    generation_id: str | None,
    operator: str,
    reason: str,
) -> None:
    _fsync_file(
        journal_path,
        json.dumps(
            {
                "operation": operation,
                "active_generation_id": generation_id,
                "operator": operator,
                "reason": reason,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
    )


def _write_marker(marker_path: Path) -> None:
    _fsync_file(marker_path, b"locality writes paused\n")


def _pause_locality_writes_open(
    company_conn: sqlite3.Connection,
    procurement_conn: sqlite3.Connection,
    *,
    maintenance_path: str | Path,
    journal_path: str | Path,
    marker_path: str | Path,
    pointer_path: str | Path,
    process_inspector: Callable[[Path], list[object]],
    operator: str,
    reason: str,
    first_snapshot: bool = False,
    fail_at: str | None = None,
    _quiesced: bool = False,
    _lock_held: bool = False,
) -> None:
    """Fsync a pause record and disable both writer rows under exclusive locks."""
    company_path = _database_path(company_conn)
    procurement_path = _database_path(procurement_conn)
    journal = Path(journal_path)
    marker = Path(marker_path)
    pointer = Path(pointer_path)
    if not _quiesced:
        assert_databases_quiesced((company_path, procurement_path), process_inspector)
    lock_context = nullcontext() if _lock_held else maintenance_lock(maintenance_path, 5)
    with lock_context:
        generation_id = _pointer_generation(pointer)
        _activation_rows_agree(company_conn, procurement_conn, generation_id, writes_enabled=True)
        _write_journal(journal, operation="pause", generation_id=generation_id, operator=operator, reason=reason)
        _fail_if_requested(fail_at, "after_journal")
        _write_marker(marker)
        _fail_if_requested(fail_at, "after_marker")
        with dual_exclusive_transition(
            company_conn,
            procurement_conn,
            after_procurement_commit=lambda: _fail_if_requested(fail_at, "after_procurement_commit"),
        ):
            with ExitStack() as permissions:
                permissions.enter_context(maintenance_write_permission(company_conn, fence_admin=True))
                permissions.enter_context(maintenance_write_permission(procurement_conn, fence_admin=True))
                _set_write_fence_in_transaction(
                    company_conn, False, operator, reason,
                    active_generation_id=generation_id, ever_snapshot_activated=first_snapshot or _activation_row(company_conn)[2],
                )
                _set_write_fence_in_transaction(
                    procurement_conn, False, operator, reason,
                    active_generation_id=generation_id, ever_snapshot_activated=first_snapshot or _activation_row(procurement_conn)[2],
                )
        _fail_if_requested(fail_at, "after_company_commit")
        _activation_rows_agree(company_conn, procurement_conn, generation_id, writes_enabled=False)


def _resume_locality_writes_open(
    company_conn: sqlite3.Connection,
    procurement_conn: sqlite3.Connection,
    *,
    maintenance_path: str | Path,
    journal_path: str | Path,
    marker_path: str | Path,
    pointer_path: str | Path,
    process_inspector: Callable[[Path], list[object]],
    operator: str,
    reason: str,
    fail_at: str | None = None,
    _quiesced: bool = False,
    _lock_held: bool = False,
) -> None:
    """Enable verified peer rows, then remove the journal and marker in order."""
    company_path = _database_path(company_conn)
    procurement_path = _database_path(procurement_conn)
    journal = Path(journal_path)
    marker = Path(marker_path)
    pointer = Path(pointer_path)
    if not _quiesced:
        assert_databases_quiesced((company_path, procurement_path), process_inspector)
    lock_context = nullcontext() if _lock_held else maintenance_lock(maintenance_path, 5)
    with lock_context:
        if not marker.exists() or not journal.exists():
            raise TransitionError("resume requires a durable paused transition")
        generation_id = _pointer_generation(pointer)
        _activation_rows_agree(company_conn, procurement_conn, generation_id, writes_enabled=False)
        _fail_if_requested(fail_at, "after_pointer_verification")
        with dual_exclusive_transition(
            company_conn,
            procurement_conn,
            after_procurement_commit=lambda: _fail_if_requested(fail_at, "after_procurement_commit"),
        ):
            with ExitStack() as permissions:
                permissions.enter_context(maintenance_write_permission(company_conn, fence_admin=True))
                permissions.enter_context(maintenance_write_permission(procurement_conn, fence_admin=True))
                _set_write_fence_in_transaction(company_conn, True, operator, reason, active_generation_id=generation_id)
                _set_write_fence_in_transaction(procurement_conn, True, operator, reason, active_generation_id=generation_id)
        _fail_if_requested(fail_at, "after_company_commit")
        _activation_rows_agree(company_conn, procurement_conn, generation_id, writes_enabled=True)
        _fail_if_requested(fail_at, "before_journal_removal")
        _remove_fsynced(journal)
        _fail_if_requested(fail_at, "before_marker_removal")
        _remove_fsynced(marker)


def _recover_locality_transition_open(
    company_conn: sqlite3.Connection,
    procurement_conn: sqlite3.Connection,
    *,
    maintenance_path: str | Path,
    journal_path: str | Path,
    marker_path: str | Path,
    pointer_path: str | Path,
    process_inspector: Callable[[Path], list[object]],
    operator: str,
    reason: str,
    fail_at: str | None = None,
    _quiesced: bool = False,
    _lock_held: bool = False,
) -> None:
    """Reconcile both databases to the pointer while retaining the fail-closed marker."""
    company_path = _database_path(company_conn)
    procurement_path = _database_path(procurement_conn)
    journal = Path(journal_path)
    marker = Path(marker_path)
    pointer = Path(pointer_path)
    if not _quiesced:
        assert_databases_quiesced((company_path, procurement_path), process_inspector)
    lock_context = nullcontext() if _lock_held else maintenance_lock(maintenance_path, 5)
    with lock_context:
        if not marker.exists():
            raise TransitionError("recovery requires a durable paused marker")
        generation_id = _pointer_generation(pointer)
        if not journal.exists():
            _write_journal(
                journal,
                operation="recovery",
                generation_id=generation_id,
                operator=operator,
                reason=reason,
            )
        with dual_exclusive_transition(
            company_conn,
            procurement_conn,
            after_procurement_commit=lambda: _fail_if_requested(fail_at, "after_procurement_commit"),
        ):
            with ExitStack() as permissions:
                permissions.enter_context(maintenance_write_permission(company_conn, fence_admin=True))
                permissions.enter_context(maintenance_write_permission(procurement_conn, fence_admin=True))
                _set_write_fence_in_transaction(company_conn, False, operator, reason, active_generation_id=generation_id)
                _set_write_fence_in_transaction(procurement_conn, False, operator, reason, active_generation_id=generation_id)
        _fail_if_requested(fail_at, "after_company_commit")
        _activation_rows_agree(company_conn, procurement_conn, generation_id, writes_enabled=False)


def _run_transition(
    operation: Callable[..., None],
    paths: LocalityPaths,
    connection_factory: Callable[[Path], sqlite3.Connection],
    *,
    process_inspector: Callable[[Path], list[object]],
    operator: str,
    reason: str,
    **options: Any,
) -> None:
    """Inspect closed database files, then open both connections under the shared lock."""
    configured = require_locality_paths(paths)
    if configured.company_db_path is None or configured.procurement_db_path is None:
        raise TransitionError("dual-database transitions require configured database paths")
    assert_databases_quiesced(
        (configured.company_db_path, configured.procurement_db_path), process_inspector
    )
    with maintenance_lock(configured.maintenance_path, 5):
        company_conn = connection_factory(configured.company_db_path)
        procurement_conn = connection_factory(configured.procurement_db_path)
        try:
            operation(
                company_conn,
                procurement_conn,
                maintenance_path=configured.maintenance_path,
                journal_path=configured.journal_path,
                marker_path=configured.marker_path,
                pointer_path=configured.pointer_path,
                process_inspector=process_inspector,
                operator=operator,
                reason=reason,
                _quiesced=True,
                _lock_held=True,
                **options,
            )
        finally:
            procurement_conn.close()
            company_conn.close()


def pause_locality_writes(
    paths: LocalityPaths,
    connection_factory: Callable[[Path], sqlite3.Connection],
    *,
    process_inspector: Callable[[Path], list[object]],
    operator: str,
    reason: str,
    first_snapshot: bool = False,
    fail_at: str | None = None,
) -> None:
    _run_transition(
        _pause_locality_writes_open,
        paths,
        connection_factory,
        process_inspector=process_inspector,
        operator=operator,
        reason=reason,
        first_snapshot=first_snapshot,
        fail_at=fail_at,
    )


def resume_locality_writes(
    paths: LocalityPaths,
    connection_factory: Callable[[Path], sqlite3.Connection],
    *,
    process_inspector: Callable[[Path], list[object]],
    operator: str,
    reason: str,
    fail_at: str | None = None,
) -> None:
    _run_transition(
        _resume_locality_writes_open,
        paths,
        connection_factory,
        process_inspector=process_inspector,
        operator=operator,
        reason=reason,
        fail_at=fail_at,
    )


def recover_locality_transition(
    paths: LocalityPaths,
    connection_factory: Callable[[Path], sqlite3.Connection],
    *,
    process_inspector: Callable[[Path], list[object]],
    operator: str,
    reason: str,
    fail_at: str | None = None,
) -> None:
    _run_transition(
        _recover_locality_transition_open,
        paths,
        connection_factory,
        process_inspector=process_inspector,
        operator=operator,
        reason=reason,
        fail_at=fail_at,
    )


__all__ = [
    "Resolution",
    "TransitionError",
    "pause_locality_writes",
    "recover_locality_transition",
    "resolve_company_conflict",
    "resume_locality_writes",
]
