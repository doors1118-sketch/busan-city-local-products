import paramiko

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "U7$B%U5843m"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(HOST, username=USER, password=PASSWORD, timeout=10)
    
    remote_script = """
import sys; sys.path.append('/opt/busan')
import alert_check
config = alert_check.load_config()
if not config:
    print("alert_config.json not found!")
    sys.exit(1)

test_msg = \"\"\"🔔 [부산 조달 경보 시스템]
본 문자는 시스템 테스트(실제 경보 아님) 문자입니다.
현재 시스템이 매일 09:00에 정상적으로 자동 진단 및 모니터링을 수행하고 있습니다.
(금일 분석 결과: 이상 징후 0건)\"\"\"
print(f"Trying to send SMS... config has {len(config.get('ncp_sms', {}).get('recipients', []))} recipients.")
alert_check.send_ncp_sms(test_msg, config)
"""
    
    # Save the script on the remote server and run it using the virtualenv python
    command = f"""cat << 'EOF' > /tmp/test_sms_run.py
{remote_script}
EOF
cd /opt/busan && /opt/busan/venv/bin/python3 /tmp/test_sms_run.py
"""
    stdin, stdout, stderr = ssh.exec_command(command)
    print(stdout.read().decode('utf-8'))
    err = stderr.read().decode('utf-8')
    if err:
        print("STDERR:", err)
    
except Exception as e:
    print(f"Error: {e}")
finally:
    ssh.close()
