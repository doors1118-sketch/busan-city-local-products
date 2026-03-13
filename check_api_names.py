import zipfile, xml.etree.ElementTree as ET
import re
import sys
sys.stdout.reconfigure(encoding='utf-8')
try:
    with zipfile.ZipFile('조달청_OpenAPI참고자료_나라장터_계약정보서비스_1.0.docx') as docx:
        tree = ET.XML(docx.read('word/document.xml').decode('utf-8'))
        text = ''.join([x for x in tree.itertext()])
        print("PPSSrch:", set(re.findall(r'[A-Za-z]+PPSSrch', text)))
        print("Srch:", set(re.findall(r'[A-Za-z]+Srch', text)))
except Exception as e:
    print(e)
