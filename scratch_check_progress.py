import paramiko, time

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 파이프라인 실행 상태 확인 (30초 후)
stdin, stdout, stderr = client.exec_command(
    "tail -50 /opt/busan/sync_log/daily.log 2>/dev/null",
    timeout=15
)
print(stdout.read().decode('utf-8', 'replace'))

client.close()
