import codecs

with codecs.open('bootstrap_from_excel.py', 'r', 'utf-8') as f:
    content = f.read()

content = content.replace("df = pd.read_excel(file_path, skiprows=2)", "df = pd.read_excel(file_path, skiprows=4)")
content = content.replace("df = pd.read_excel(file_path, skiprows=1)", "df = pd.read_excel(file_path, skiprows=4)")

with codecs.open('bootstrap_from_excel.py', 'w', 'utf-8') as f:
    f.write(content)
