import re, glob, os
pattern = re.compile(r'(?i)(key|service_?key|api_?key)\s*[:=]\s*[\'"]([a-zA-Z0-9%_-]{20,})[\'"]')
count = 0
for f in glob.glob('**/*', recursive=True):
    if not os.path.isfile(f): continue
    if '.git' in f or '.gemini' in f or '__pycache__' in f or 'scratch' in f: continue
    try:
        content = open(f, 'r', encoding='utf-8', errors='ignore').read()
        matches = pattern.findall(content)
        if matches:
            for m in matches:
                print(f"{f}: {m}")
                count += 1
    except: pass
print("Total occurrences:", count)
