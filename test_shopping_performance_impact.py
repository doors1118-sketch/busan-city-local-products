import sqlite3

import pytest

import shopping_performance_impact as impact


def _create_db(path, rows):
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE shopping_cntrct (
            dlvrReqNo TEXT,
            prdctSno TEXT,
            dlvrReqChgOrd TEXT,
            dlvrReqRcptDate TEXT,
            prdctAmt TEXT
        )"""
    )
    conn.executemany("INSERT INTO shopping_cntrct VALUES (?, ?, ?, ?, ?)", rows)
    conn.commit()
    return conn


def test_readonly_proxy_exposes_union_as_temp_view(tmp_path):
    production = tmp_path / "production.db"
    shadow = tmp_path / "shadow.db"
    _create_db(
        production,
        [("A", "1", "0", "20260908", "100")],
    ).close()
    _create_db(
        shadow,
        [("B", "1", "0", "20260909", "200")],
    ).close()

    proxy = impact.ReadonlyProcurementConnections(production.resolve(), shadow.resolve())
    conn = proxy.connect(str(production))
    try:
        assert conn.execute(
            "SELECT dlvrReqNo, prdctAmt FROM shopping_cntrct ORDER BY dlvrReqNo"
        ).fetchall() == [("A", "100"), ("B", "200")]
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("DELETE FROM main.shopping_cntrct")
    finally:
        conn.close()


def test_validate_scope_requires_every_missing_date_complete(tmp_path):
    production = tmp_path / "production.db"
    shadow = tmp_path / "shadow.db"
    _create_db(
        production,
        [("A", "1", "0", "20260908", "100")],
    ).close()
    conn = _create_db(
        shadow,
        [("B", "1", "0", "20260909", "200")],
    )
    conn.execute(
        """CREATE TABLE shopping_sync_log (
            target_date TEXT PRIMARY KEY,
            status TEXT,
            source_total INTEGER,
            source_received INTEGER,
            busan_rows INTEGER,
            stored_rows INTEGER,
            request_count INTEGER
        )"""
    )
    conn.execute(
        "INSERT INTO shopping_sync_log VALUES ('20260909','complete',20,20,1,1,1)"
    )
    conn.commit()
    conn.close()

    result = impact.validate_backfill_scope(
        production.resolve(), shadow.resolve(), "20260908", "20260909"
    )
    assert result["production_missing_days"] == 1
    assert result["complete_missing_days"] == 1
    assert result["source_total"] == 20

    with pytest.raises(impact.ImpactError):
        impact.validate_backfill_scope(
            production.resolve(), shadow.resolve(), "20260908", "20260910"
        )
