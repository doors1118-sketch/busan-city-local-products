import json

try:
    with open('api_swagger.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        for path, methods in data.get('paths', {}).items():
            if 'getCntrctInfoListCnstwkServcInfo' in path:
                print('Found endpoint:', path)
                params = methods.get('get', {}).get('parameters', [])
                for p in params:
                    print(f"  - {p.get('name')} (required: {p.get('required', False)}): {p.get('description', '')}")
except Exception as e:
    print('Swagger file not found or parse error:', e)
