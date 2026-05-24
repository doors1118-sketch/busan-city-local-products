import paramiko

def check_process():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=15)
        stdin, stdout, stderr = client.exec_command("ps -ef | grep python")
        print("=== Running Python Processes ===")
        print(stdout.read().decode('utf-8'))
        
        stdin, stdout, stderr = client.exec_command("tail -n 100 /opt/busan/sync_log/daily.log")
        print("=== Tail daily.log ===")
        print(stdout.read().decode('utf-8'))
    except Exception as e:
        print("Error:", e)
    finally:
        client.close()

if __name__ == '__main__':
    check_process()
