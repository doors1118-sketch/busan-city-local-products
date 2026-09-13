"""Build both caches before publishing; caller must hold the pipeline lock."""
import json
import math
import os
from pathlib import Path
import shutil
import tempfile


def validate_api(cache):
    total = cache['1_전체']
    sectors = cache['2_분야별']
    for row in [total] + list(sectors.values()):
        for key in ('발주액', '수주액', '수주율'):
            if not isinstance(row[key], (int, float)) or not math.isfinite(row[key]):
                raise ValueError('non-finite cache metric')
        if row['발주액'] < 0 or row['수주액'] < 0:
            raise ValueError('negative cache total')
    for key in ('발주액', '수주액'):
        if abs(total[key] - sum(row[key] for row in sectors.values())) > 2:
            raise ValueError('sector and overall totals disagree')
    return cache


def atomic_copy(source, target):
    target = Path(target)
    fd, temporary = tempfile.mkstemp(prefix=target.name + '.', dir=target.parent)
    try:
        with os.fdopen(fd, 'wb') as output, open(source, 'rb') as inp:
            shutil.copyfileobj(inp, output)
            output.flush()
            os.fsync(output.fileno())
        if target.exists():
            os.chmod(temporary, target.stat().st_mode & 0o777)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    import build_api_cache
    import build_monthly_cache
    root = Path.cwd()
    if shutil.disk_usage(root).free < 8 * 1024**3:
        raise RuntimeError('cache refresh stopped: less than 8 GiB free')
    with tempfile.TemporaryDirectory(prefix='.cache-build-', dir=root) as temporary:
        stage = Path(temporary)
        api = stage / 'api_cache.json'
        monthly = stage / 'monthly_cache.json'
        build_api_cache.CACHE_FILE = str(api)
        build_monthly_cache.MONTHLY_CACHE = str(monthly)
        build_api_cache.build_cache()
        validate_api(json.loads(api.read_text(encoding='utf-8')))
        build_monthly_cache.build_monthly()
        data = json.loads(monthly.read_text(encoding='utf-8'))
        if not data or not data.get('generated_at'):
            raise ValueError('monthly cache is empty')
        # Preserve a recoverable previous API cache. No process restart needed:
        # api_server.load_cache reads each request from disk.
        atomic_copy(root / 'api_cache.json', root / 'api_cache_prev.json')
        atomic_copy(monthly, root / 'monthly_cache.json')
        atomic_copy(api, root / 'api_cache.json')
        print('CACHE_PUBLISH_OK')


if __name__ == '__main__':
    main()
