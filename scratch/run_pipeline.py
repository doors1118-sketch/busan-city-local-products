import paramiko
import sys

def run_pipeline():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        print("Connecting to server...")
        client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=15)
        print("Connected! Starting daily_pipeline_sync.py 20260522...")
        
        # We need to load environment variables from .env
        command = "cd /opt/busan && export $(cat .env | xargs) && python3 daily_pipeline_sync.py 20260522"
        stdin, stdout, stderr = client.exec_command(command, get_pty=True)
        
        # Read output in real-time
        for line in stdout:
            print(line, end="")
            
        print("\n=== STDERR ===")
        print(stderr.read().decode('utf-8'))
        
    except Exception as e:
        print("Connection failed:", e)
    finally:
        client.close()

if __name__ == '__main__':
    run_pipeline()
