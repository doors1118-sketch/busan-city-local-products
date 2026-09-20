import datetime
import json

import alert_check


def _write_status(path, *, core_ok=True, optional_ok=True, generated_at=None):
    generated_at = generated_at or datetime.datetime.now(datetime.timezone.utc)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": generated_at.isoformat(timespec="seconds"),
                "core": {
                    "ok": core_ok,
                    "created": ["procurement_contracts.db"] if core_ok else [],
                    "failures": [] if core_ok else ["simulated core failure"],
                },
                "optional": {
                    "enabled": True,
                    "ok": optional_ok,
                    "created": ["chatbot_company.db"] if optional_ok else [],
                    "failures": [] if optional_ok else ["simulated capacity guard"],
                },
                "maintenance_failures": [],
            }
        ),
        encoding="utf-8",
    )


def test_backup_alert_separates_optional_failure(tmp_path, monkeypatch):
    status_path = tmp_path / "backup_status.json"
    _write_status(status_path, core_ok=True, optional_ok=False)
    monkeypatch.setattr(alert_check, "BACKUP_STATUS_FILE", str(status_path))

    alerts = alert_check.check_backup_status()

    assert [level for level, _ in alerts] == ["WARNING"]
    assert "선택 대형 DB" in alerts[0][1]
    assert "핵심 조달 DB 백업과 별도" in alerts[0][1]


def test_backup_alert_marks_core_failure_critical(tmp_path, monkeypatch):
    status_path = tmp_path / "backup_status.json"
    _write_status(status_path, core_ok=False, optional_ok=True)
    monkeypatch.setattr(alert_check, "BACKUP_STATUS_FILE", str(status_path))

    alerts = alert_check.check_backup_status()

    assert [level for level, _ in alerts] == ["CRITICAL"]
    assert "핵심 DB 백업 실패" in alerts[0][1]


def test_backup_alert_marks_stale_status_critical(tmp_path, monkeypatch):
    status_path = tmp_path / "backup_status.json"
    now = datetime.datetime(2026, 9, 20, 9, 0, tzinfo=datetime.timezone.utc)
    _write_status(status_path, generated_at=now - datetime.timedelta(hours=29))
    monkeypatch.setattr(alert_check, "BACKUP_STATUS_FILE", str(status_path))
    monkeypatch.setattr(alert_check, "BACKUP_STATUS_MAX_AGE_HOURS", 28)

    alerts = alert_check.check_backup_status(now=now)

    assert [level for level, _ in alerts] == ["CRITICAL"]
    assert "백업 지연" in alerts[0][1]
