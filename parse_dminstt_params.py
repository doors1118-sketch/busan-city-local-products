import json

try:
    with open('openapi.html', 'r', encoding='utf-8') as f:
        text = f.read()

    start = text.find('{"swagger":"2.0"')
    if start != -1:
        end = -1
        bracket_count = 0
        for i, char in enumerate(text[start:]):
            if char == '{': bracket_count += 1
            elif char == '}': bracket_count -= 1
            if bracket_count == 0:
                end = i + 1
                break
                
        if end != -1:
            swagger = json.loads(text[start:start+end])
            paths = swagger.get('paths', {})
            found = False
            for path, data in paths.items():
                if 'getDminsttInfo' in path:
                    found = True
                    print(f'\n=== Parameters for {path} ===')
                    params = data.get('get', {}).get('parameters', [])
                    for p in params:
                        req = '[Required]' if p.get('required') else '[Optional]'
                        print(f"{req} {p.get('name')} : {p.get('description')} ({p.get('type')})")
            if not found:
                print('Could not find getDminsttInfo in paths.')
except Exception as e:
    print('Error:', e)
