import paramiko
import os

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "U7$B%U5843m"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=10)

# SFTP로 업로드
sftp = ssh.open_sftp()
local_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'alert_check.py'))
print(f"Uploading {local_path}...")
sftp.put(local_path, '/opt/busan/alert_check.py')
sftp.close()
print("Upload complete.")

# 검증
stdin, stdout, stderr = ssh.exec_command("grep 'timeout=' /opt/busan/alert_check.py | head -5")
print("Timeout values on server:")
print(stdout.read().decode('utf-8'))

ssh.close()
