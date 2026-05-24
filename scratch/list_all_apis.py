import re, os

scripts = [
    'import_certified_product_api.py',
    'import_innovation_product_api.py',
    'import_mas_product_api.py',
    'nts_business_status_client.py',
    'nts_batch_sync.py',
    'import_cnstwk_types.py',
    'import_company_industry.py',
    'import_company_products.py',
]

for s in scripts:
    if not os.path.exists(s):
        print(f"\n{'='*60}")
        print(f"❌ {s} — 파일 없음")
        continue
    
    with open(s, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    
    print(f"\n{'='*60}")
    print(f"📄 {s}")
    
    # Find API URLs
    urls = re.findall(r'https?://[^\s\'"]+', content)
    seen = set()
    for u in urls:
        u_clean = u.rstrip('",\')')
        if u_clean not in seen and 'apis.data.go.kr' in u_clean:
            seen.add(u_clean)
            print(f"  API: {u_clean}")
    
    # Find SERVICE_KEY env vars
    keys = re.findall(r'os\.environ\.get\(["\']([^"\']+)["\']', content)
    for k in keys:
        if 'KEY' in k.upper() or 'SERVICE' in k.upper():
            val = os.environ.get(k, '(미설정)')
            masked = val[:4] + '***' if val != '(미설정)' and len(val) > 4 else val
            print(f"  ENV: {k} = {masked}")
    
    # Find source_name
    src_names = re.findall(r"source_name['\"]?\s*[=:]\s*['\"]([^'\"]+)['\"]", content)
    for sn in set(src_names):
        print(f"  source_name: {sn}")
    
    # Find target tables (INSERT INTO)
    tables = re.findall(r'INSERT\s+(?:OR\s+\w+\s+)?INTO\s+(\w+)', content, re.IGNORECASE)
    for t in sorted(set(tables)):
        print(f"  적재 테이블: {t}")
