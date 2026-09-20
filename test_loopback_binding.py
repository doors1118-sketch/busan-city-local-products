from pathlib import Path


ROOT = Path(__file__).resolve().parent


def test_api_defaults_to_loopback():
    source = (ROOT / "api_server.py").read_text(encoding="utf-8")
    assert 'os.environ.get("BUSAN_API_BIND_HOST", "127.0.0.1")' in source
    assert 'uvicorn.run(app, host="0.0.0.0"' not in source


def test_dashboard_systemd_dropin_binds_loopback():
    source = (
        ROOT / "ops" / "systemd" / "busan-dashboard.service.d" / "20-loopback.conf"
    ).read_text(encoding="utf-8")
    assert "--server.address 127.0.0.1" in source
    assert "--server.address 0.0.0.0" not in source
