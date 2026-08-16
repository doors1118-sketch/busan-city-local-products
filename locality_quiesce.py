"""Maintenance-window checks and ordered dual-database exclusive transactions."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3
from typing import Callable, Iterable, Iterator, Sequence


class QuiescenceError(RuntimeError):
    pass


def assert_databases_quiesced(
    paths: Iterable[str | Path], process_inspector: Callable[[Path], Sequence[object]]
) -> None:
    """Abort before mutation when any database, WAL, or SHM file is held open."""
    busy: list[tuple[Path, Sequence[object]]] = []
    for database_path in paths:
        resolved = Path(database_path).resolve()
        for candidate in (resolved, Path(str(resolved) + "-wal"), Path(str(resolved) + "-shm")):
            holders = process_inspector(candidate)
            if holders:
                busy.append((candidate, holders))
    if busy:
        names = ", ".join(str(path) for path, _ in busy)
        raise QuiescenceError(f"database files are still open: {names}")


@contextmanager
def dual_exclusive_transition(
    company_conn: sqlite3.Connection, procurement_conn: sqlite3.Connection
) -> Iterator[None]:
    """Acquire exclusive SQLite transactions in the required company-before-procurement order."""
    company_started = False
    procurement_started = False
    try:
        company_conn.execute("BEGIN EXCLUSIVE")
        company_started = True
        procurement_conn.execute("BEGIN EXCLUSIVE")
        procurement_started = True
        yield
    except BaseException:
        if procurement_started:
            procurement_conn.rollback()
        if company_started:
            company_conn.rollback()
        raise
    else:
        procurement_conn.commit()
        company_conn.commit()
