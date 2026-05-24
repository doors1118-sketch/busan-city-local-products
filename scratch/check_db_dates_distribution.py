import paramiko

def check_distribution():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=15)
        
        tables = ['cnstwk_cntrct', 'servc_cntrct', 'thng_cntrct']
        print("=== Contract Date Distribution on Server DB ===")
        for t in tables:
            print(f"\n--- Table: {t} ---")
            cmd = f"sqlite3 /opt/busan/procurement_contracts.db \"SELECT SUBSTR(rgstDt, 1, 10) as dt, COUNT(*) FROM {t} GROUP BY dt ORDER BY dt DESC LIMIT 7\""
            stdin, stdout, stderr = client.exec_command(cmd)
            print(stdout.read().decode('utf-8'))

    except Exception as e:
        print("Error:", e)
    finally:
        client.close()

if __name__ == '__main__':
    check_distribution()
