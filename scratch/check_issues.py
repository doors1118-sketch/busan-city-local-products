import paramiko

def check_diagnostics():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=15)
        
        print("=== Checking dmesg for Killed processes ===")
        stdin, stdout, stderr = client.exec_command("dmesg -T | grep -i -E 'kill|oom' | tail -n 20")
        print(stdout.read().decode('utf-8'))

        print("=== Checking syslog for daily_pipeline_sync ===")
        stdin, stdout, stderr = client.exec_command("grep -i 'daily_pipeline' /var/log/syslog | tail -n 20")
        print(stdout.read().decode('utf-8'))

        print("=== Checking if database is locked or busy ===")
        stdin, stdout, stderr = client.exec_command("sqlite3 /opt/busan/procurement_contracts.db 'PRAGMA integrity_check;'")
        print(stdout.read().decode('utf-8'))

    except Exception as e:
        print("Error:", e)
    finally:
        client.close()

if __name__ == '__main__':
    check_diagnostics()
