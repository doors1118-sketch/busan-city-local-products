import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)

cmds = [
    "echo '=== systemd busan-api 서비스 설정 ==='",
    "cat /etc/systemd/system/busan-api.service",
    "echo ''",
    "echo '=== 환경변수 파일 ==='",
    "cat /opt/busan/.env 2>/dev/null || echo '.env 없음'",
    "echo ''",
    "echo '=== CHATBOT_DB 환경변수 확인 ==='",
    "grep -r 'CHATBOT_DB' /etc/systemd/system/ /opt/busan/.env 2>/dev/null || echo 'CHATBOT_DB 미설정'",
    "echo ''",
    "echo '=== /opt/busan/ DB 파일 목록 ==='",
    "ls -la /opt/busan/*.db",
    "echo ''",
    "echo '=== api_server.py CHATBOT_DB 기본값 ==='",
    "grep 'CHATBOT_DB' /opt/busan/api_server.py | head -5",
]

for cmd in cmds:
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode()
    err = stderr.read().decode()
    if out.strip():
        print(out.strip())
    if err.strip():
        print(f"  stderr: {err.strip()}")

ssh.close()
