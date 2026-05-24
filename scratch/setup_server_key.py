import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"
SERVICE_KEY = "c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)

cmds = [
    # 1. 환경변수 파일 생성/갱신
    f"echo 'SHOPPING_MALL_PRDCT_SERVICE_KEY={SERVICE_KEY}' >> /opt/busan/.env",
    # 기존 키가 있으면 중복 방지
    "sort -u /opt/busan/.env -o /opt/busan/.env",
    "cat /opt/busan/.env",
    
    # 2. systemd 서비스에 EnvironmentFile 추가 (없으면)
    "grep -q 'EnvironmentFile' /etc/systemd/system/busan-api.service || sed -i '/\\[Service\\]/a EnvironmentFile=/opt/busan/.env' /etc/systemd/system/busan-api.service",
    "cat /etc/systemd/system/busan-api.service",
    
    # 3. systemd 리로드 + 서비스 재시작
    "systemctl daemon-reload",
    "systemctl restart busan-api",
    "sleep 2",
    "systemctl is-active busan-api",
    
    # 4. MAS API probe 테스트
    f"cd /opt/busan && SHOPPING_MALL_PRDCT_SERVICE_KEY={SERVICE_KEY} /opt/busan/venv/bin/python3 import_mas_product_api.py --probe",
]

for cmd in cmds:
    # 키 노출 방지 로그
    display_cmd = cmd.replace(SERVICE_KEY, "***KEY***") if SERVICE_KEY in cmd else cmd
    print(f"\n$ {display_cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=60)
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    if out: print(f"  {out}")
    if err: print(f"  stderr: {err}")

ssh.close()
