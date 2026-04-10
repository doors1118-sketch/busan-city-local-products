import paramiko
import sys

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "U7$B%U5843m"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(HOST, username=USER, password=PASSWORD, timeout=10)
    print("--- CRONTAB ---")
    stdin, stdout, stderr = ssh.exec_command("crontab -l")
    print(stdout.read().decode('utf-8'))
    
    print("--- ALERT LOG ---")
    stdin, stdout, stderr = ssh.exec_command("cat /opt/busan/alert.log 2>/dev/null | tail -n 20")
    print(stdout.read().decode('utf-8'))
    
    print("--- SYSLOG CRON ---")
    stdin, stdout, stderr = ssh.exec_command("cat /var/log/syslog 2>/dev/null | grep CRON | grep alert | tail -n 10")
    print(stdout.read().decode('utf-8'))
    
    print("--- SCRIPT EXISTENCE ---")
    stdin, stdout, stderr = ssh.exec_command("ls -la /opt/busan/send_alert.py 2>/dev/null || file /opt/busan/send_alert.py")
    print(stdout.read().decode('utf-8'))
except Exception as e:
    print(f"Error: {e}")
finally:
    ssh.close()
