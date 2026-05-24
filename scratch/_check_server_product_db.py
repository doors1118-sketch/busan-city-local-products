import paramiko

def check_sync_log():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=15)
        
        print("=== Checking sync_log table on Server DB ===")
        stdin, stdout, stderr = client.exec_command(
            "sqlite3 /opt/busan/procurement_contracts.db \"SELECT * FROM sync_log ORDER BY sync_date DESC LIMIT 30\""
        )
        print(stdout.read().decode('utf-8'))
        
        # Also let's check shopping_cntrct schema to see date columns
        print("=== Checking shopping_cntrct Schema ===")
        stdin, stdout, stderr = client.exec_command(
            "sqlite3 /opt/busan/procurement_contracts.db \"PRAGMA table_info(shopping_cntrct)\""
        )
        print(stdout.read().decode('utf-8'))

    except Exception as e:
        print("Error:", e)
    finally:
        client.close()

if __name__ == '__main__':
    check_sync_log()
