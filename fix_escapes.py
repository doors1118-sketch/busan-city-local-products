content = open('bootstrap_from_excel.py', 'r', encoding='utf-8').read()
content = content.replace('\\"', '"')
open('bootstrap_from_excel.py', 'w', encoding='utf-8').write(content)
print("Fixed all escaped quotes in bootstrap_from_excel.py")
