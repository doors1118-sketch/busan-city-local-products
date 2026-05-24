import paramiko

def check_dates():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=15)
        
        print("=== Checking Distinct Dates in cnstwk_cntrct (Latest 10) ===")
        stdin, stdout, stderr = client.exec_command(
            "sqlite3 /opt/busan/procurement_contracts.db \"SELECT DISTINCT SUBSTR(rgstDt, 1, 10) FROM cnstwk_cntrct ORDER BY rgstDt DESC LIMIT 10\""
        )
        print(stdout.read().decode('utf-8'))

        print("=== Checking Distinct Dates in sync_log (Latest 10) ===")
        stdin, stdout, stderr = client.exec_command(
            "sqlite3 /opt/busan/procurement_contracts.db \"SELECT sync_date FROM sync_log ORDER BY sync_date DESC LIMIT 10\""
        )
        print(stdout.read().decode('utf-8'))

    except Exception as e:
        print("Error:", e)
    finally:
        client.close()

if __name__ == '__main__':
    check_dates()
