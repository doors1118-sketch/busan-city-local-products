import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. busan-monitor의 환경에 SERVICE_KEY가 글로벌하게 설정되어 있었는지 확인
    "sudo -u busan-monitor bash -c 'printenv SERVICE_KEY' 2>/dev/null || echo 'EMPTY'",
    # 2. /etc/environment에 있는지
    "grep SERVICE_KEY /etc/environment 2>/dev/null || echo 'not in /etc/environment'",
    # 3. busan-monitor의 bash profile
    "cat /home/busan-monitor/.bashrc 2>/dev/null | grep -i key || echo 'not in .bashrc'",
    "cat /home/busan-monitor/.profile 2>/dev/null | grep -i key || echo 'not in .profile'",
    # 4. systemd user 환경
    "cat /etc/default/busan* 2>/dev/null || echo 'no /etc/default/busan*'",
    # 5. 크론탭에 . .env 추가하고 파이프라인 수동 실행
    # 먼저 현재 크론탭 백업
    "crontab -u busan-monitor -l > /tmp/crontab_backup.txt 2>/dev/null && cat /tmp/crontab_backup.txt",
]

for i, cmd in enumerate(commands, 1):
    print(f"\n[{i}] {'='*50}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=15)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err and 'apport' not in err:
        print(f"STDERR: {err}")

client.close()
