"""Check cache values on server"""
import paramiko
import json

def run_cmd(client, cmd, label):
    print(f"\n  [{label}]")
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode('utf-8').strip()
    if out: print(out)

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

run_cmd(client, "cat /opt/busan/api_cache.json | grep 'total_rate' | head -1", "API Cache Total Rate")
run_cmd(client, "cat /opt/busan/monthly_cache.json | grep -A 5 '2024' | head -10", "Monthly Cache 2024")
run_cmd(client, "cat /opt/busan/monthly_cache.json | grep -A 5 '2026' | head -10", "Monthly Cache 2026")
run_cmd(client, "ls -l /opt/busan/api_cache.json /opt/busan/monthly_cache.json", "Cache Timestamps")

client.close()
