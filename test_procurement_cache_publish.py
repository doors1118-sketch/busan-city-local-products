import json
from pathlib import Path
import sys
import types

import pytest

import refresh_procurement_caches as refresh


def example_cache():
    return {'1_전체': {'발주액': 100, '수주액': 60, '수주율': 60.0},
            '2_분야별': {'쇼핑몰': {'발주액': 100, '수주액': 60, '수주율': 60.0}}}


def test_nonfinite_and_inconsistent_totals_rejected():
    cache = example_cache()
    refresh.validate_api(cache)
    cache['1_전체']['수주액'] = float('nan')
    with pytest.raises(ValueError):
        refresh.validate_api(cache)
    cache = example_cache()
    cache['2_분야별']['쇼핑몰']['발주액'] = 90
    with pytest.raises(ValueError):
        refresh.validate_api(cache)


def test_failure_before_publish_preserves_both_caches(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    Path('api_cache.json').write_text('original-api')
    Path('monthly_cache.json').write_text('original-monthly')
    api = types.SimpleNamespace(CACHE_FILE='')
    monthly = types.SimpleNamespace(MONTHLY_CACHE='')
    api.build_cache = lambda: Path(api.CACHE_FILE).write_text(json.dumps(example_cache()))
    def fail():
        Path(monthly.MONTHLY_CACHE).write_text('{partial')
        raise ValueError('simulated generation error')
    monthly.build_monthly = fail
    monkeypatch.setitem(sys.modules, 'build_api_cache', api)
    monkeypatch.setitem(sys.modules, 'build_monthly_cache', monthly)
    monkeypatch.setattr(refresh.shutil, 'disk_usage', lambda _: types.SimpleNamespace(free=12 * 1024**3))
    with pytest.raises(ValueError):
        refresh.main()
    assert Path('api_cache.json').read_text() == 'original-api'
    assert Path('monthly_cache.json').read_text() == 'original-monthly'


def test_atomic_copy_preserves_target_on_copy_failure(monkeypatch, tmp_path):
    target = tmp_path / 'target.json'
    target.write_text('good')
    with pytest.raises(FileNotFoundError):
        refresh.atomic_copy(tmp_path / 'absent', target)
    assert target.read_text() == 'good'


def test_only_two_authorized_overrides_added():
    data = json.loads((Path(__file__).parent / 'site_exclusion_overrides.json').read_text(encoding='utf-8'))
    new = [r for r in data['exclusions'] if r.get('created_at') == '2026-09-13']
    assert {r['dcsnCntrctNo'] for r in new} == {'R25TA0111600801', 'R26TA0192750600'}
    assert sum(r['engine_total_amount'] for r in new) == 50300000000
    assert sum(r['engine_local_amount'] for r in new) == 6900000000
