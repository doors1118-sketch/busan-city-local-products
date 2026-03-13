import re
import json

with open('openapi.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Try to find JSON block in the HTML
start = text.find('{"swagger":"2.0"')
if start != -1:
    end = text.rfind('}')
    # Simple extraction
    print("Found Swagger JSON Start")
else:
    print("No direct swagger JSON, searching using regex for 'getCntrctInfoListCnstwkServcInfo'")
    matches = re.finditer(r'getCntrctInfoListCnstwkServcInfo[^<]+', text)
    for m in matches:
        print("MATCH:", m.group(0)[:200])
        
    start_idx = text.find('getCntrctInfoListCnstwkServcInfo')
    if start_idx != -1:
        print("Context near API:")
        print(text[max(0, start_idx-200):start_idx+800])
