import json
import re

try:
    with open('openapi.html', 'r', encoding='utf-8') as f:
        text = f.read()

    start = text.find('{"swagger":"2.0"')
    if start != -1:
        # Extract the JSON object.
        obj_str = text[start:]
        
        # Find the end of JSON
        idx = 0
        bracket_count = 0
        end = -1
        for i, char in enumerate(obj_str):
            if char == '{': bracket_count += 1
            elif char == '}': bracket_count -= 1
            if bracket_count == 0:
                end = i + 1
                break
                
        if end != -1:
            swagger = json.loads(obj_str[:end])
            paths = swagger.get('paths', {})
            for path, data in paths.items():
                if 'getCntrctInfoListCnstwkServcInfo' in path:
                    print('Found Path:', path)
                    params = data.get('get', {}).get('parameters', [])
                    for p in params:
                        print(f"  - {p.get('name')}: {p.get('description')}")
        else:
            print("Could not find end of JSON")
    else:
        print("Could not find swagger tag")
        
except Exception as e:
    print('Error:', e)
