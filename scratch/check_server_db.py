import paramiko

def check_db_contents():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=15)
        
        tables = ['cnstwk_cntrct', 'servc_cntrct', 'thng_cntrct', 'shopping_cntrct']
        print("=== Checking 2026-05-22 Contracts on Server DB ===")
        for t in tables:
            # Note: Shopping contracts might have different date column, but let's check rgstDt/dlvrReqDt
            date_col = 'dlvrReqDt' if t == 'shopping_cntrct' else 'rgstDt'
            cmd = f"sqlite3 /opt/busan/procurement_contracts.db \"SELECT COUNT(*), MIN({date_col}), MAX({date_col}) FROM {t} WHERE {date_col} LIKE '2026-05-22%'\""
            stdin, stdout, stderr = client.exec_command(cmd)
            print(f"{t}: {stdout.read().decode('utf-8').strip()}")
            err = stderr.read().decode('utf-8')
            if err:
                print(f"Error {t}:", err)

        print("\n=== Checking Max Dates in DB ===")
        for t in tables:
            date_col = 'dlvrReqDt' if t == 'shopping_cntrct' else 'rgstDt'
            cmd = f"sqlite3 /opt/busan/procurement_contracts.db \"SELECT COUNT(*), MAX({date_col}) FROM {t}\""
            stdin, stdout, stderr = client.exec_command(cmd)
            print(f"{t}: {stdout.read().decode('utf-8').strip()}")

    except Exception as e:
        print("Error:", e)
    finally:
        client.close()

if __name__ == '__main__':
    check_db_contents()
