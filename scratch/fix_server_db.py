import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)

cmds = [
    # staging DB를 chatbot_company.db로 복사
    "cp /opt/busan/staging_chatbot_company.db /opt/busan/chatbot_company.db",
    "chown busan-monitor:busan-monitor /opt/busan/chatbot_company.db",
    "chmod 644 /opt/busan/chatbot_company.db",
    "ls -la /opt/busan/chatbot_company.db",
    
    # api_server.py 권한도 맞춤
    "chown busan-monitor:busan-monitor /opt/busan/api_server.py",
    
    # 서비스 재시작
    "systemctl restart busan-api",
    "sleep 3",
    "systemctl is-active busan-api",
    
    # health check
    "curl -s http://127.0.0.1:8000/api/chatbot/health",
]

for cmd in cmds:
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=15)
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    print(f"$ {cmd}")
    if out: print(f"  {out}")
    if err: print(f"  stderr: {err}")
    print()

ssh.close()
