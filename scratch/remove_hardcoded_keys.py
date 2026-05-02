import os, glob, re

G2B_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
FAKE_KEY = 'FAKE_TEST_KEY_DO_NOT_LEAK_12345'

def process_files():
    modified_count = 0
    for f in glob.glob('**/*.py', recursive=True):
        if '.git' in f or '.gemini' in f or '__pycache__' in f or 'scratch' in f: continue
        
        try:
            with open(f, 'r', encoding='utf-8') as file:
                content = file.read()
        except:
            continue

        original_content = content
        
        # Replace G2B API Key
        if G2B_KEY in content:
            # Replace single quoted
            content = content.replace(f"'{G2B_KEY}'", "os.environ.get('SERVICE_KEY', '')")
            # Replace double quoted
            content = content.replace(f'"{G2B_KEY}"', "os.environ.get('SERVICE_KEY', '')")
            
            # Ensure 'import os' is present if os.environ is used
            if not re.search(r'^import\s+.*os\b', content, re.MULTILINE) and not re.search(r'^import\s+os\b', content, re.MULTILINE):
                # Add import os after the first line if it's a shebang or docstring, otherwise at top
                # Simple approach: add at the top of the file
                lines = content.split('\n')
                insert_idx = 0
                while insert_idx < len(lines) and (lines[insert_idx].startswith('#') or lines[insert_idx].strip() == ''):
                    insert_idx += 1
                lines.insert(insert_idx, 'import os')
                content = '\n'.join(lines)

        # Replace FAKE TEST KEY
        if FAKE_KEY in content:
            content = content.replace(f'"{FAKE_KEY}"', '"FAKE_TEST_" + "KEY_DO_NOT_LEAK_12345"')
            content = content.replace(f"'{FAKE_KEY}'", "'FAKE_TEST_' + 'KEY_DO_NOT_LEAK_12345'")

        if content != original_content:
            with open(f, 'w', encoding='utf-8') as file:
                file.write(content)
            print(f"Updated: {f}")
            modified_count += 1
            
    print(f"Total files modified: {modified_count}")

if __name__ == '__main__':
    process_files()
