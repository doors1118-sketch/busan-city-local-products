import re, glob
pattern = re.compile(r'(?i)(key)\s*=\s*[\'"]([a-zA-Z0-9%_-]{20,})[\'"]')
count = 0
for f in glob.glob('**/*.py', recursive=True):
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
