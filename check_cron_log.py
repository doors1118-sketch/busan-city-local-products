import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

def run(cmd):
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode('utf-8').strip()
    return out

print("=== [크론탭 설정 확인] ===")
print(run("crontab -l"))

print("\n=== [로그 내용 확인] ===")
print(run("ls -lh /opt/busan/*.log /opt/busan/sync_log/*.log 2>/dev/null"))
print("--- [cron_pipeline.log (또는 관련 로그)] ---")
print(run("tail -n 30 /opt/busan/cron.log 2>/dev/null"))

client.close()
