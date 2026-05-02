import paramiko
client=paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@')
stdin, stdout, stderr = client.exec_command("python3 -c \"import json\nf1=open('/opt/busan/api_cache.json')\nf2=open('/opt/busan/monthly_cache.json')\na=json.load(f1)\nm=json.load(f2)\nprint('API:', a['1_전체']['발주액'], a['1_전체']['수주액'], a['1_전체']['수주율'])\nprint('MON:', m['누계_그룹']['전체'][-1]['발주액'], m['누계_그룹']['전체'][-1]['수주액'], m['누계_그룹']['전체'][-1]['수주율'])\"")
print("OUT:", stdout.read().decode('utf-8'))
print("ERR:", stderr.read().decode('utf-8'))
