import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

def run(cmd):
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode('utf-8').strip()
    err = stderr.read().decode('utf-8').strip()
    return out if out else err

print("1. alert_config.json 존재 여부 및 설정 확인")
print(run("cat /opt/busan/alert_config.json | jq '.ncp_sms | {enabled: .enabled, recipients: .recipients}'"))

print("\n2. 최근 문자 발송 로그 (cache_build.log 및 alert_check.log)")
print("--- [cache_build.log (파이프라인)] ---")
print(run("grep 'SMS' /opt/busan/sync_log/cache_build.log | tail -n 5"))

print("\n--- [alert_check.log (이상거래 경보)] ---")
print(run("tail -n 5 /opt/busan/sync_log/alert_check.log"))

client.close()
