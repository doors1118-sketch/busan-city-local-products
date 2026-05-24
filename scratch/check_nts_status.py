import paramiko

def check_formats():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=15)
        
        print("=== Samples of bid_notices_price.bidNtceNo ===")
        stdin, stdout, stderr = client.exec_command(
            "sqlite3 /opt/busan/procurement_contracts.db \"SELECT bidNtceNo FROM bid_notices_price WHERE bidNtceNo IS NOT NULL AND bidNtceNo != '' LIMIT 5\""
        )
        print(stdout.read().decode('utf-8'))

        print("=== Samples of cnstwk_cntrct.ntceNo ===")
        stdin, stdout, stderr = client.exec_command(
            "sqlite3 /opt/busan/procurement_contracts.db \"SELECT ntceNo FROM cnstwk_cntrct WHERE ntceNo IS NOT NULL AND ntceNo != '' LIMIT 5\""
        )
        print(stdout.read().decode('utf-8'))

        print("=== Count of empty rows (unclassified) ===")
        stdin, stdout, stderr = client.exec_command(
            "sqlite3 /opt/busan/procurement_contracts.db \"SELECT COUNT(*) FROM cnstwk_cntrct WHERE (cnstwkTypeLrg IS NULL OR cnstwkTypeLrg = '') AND ntceNo IS NOT NULL AND ntceNo != ''\""
        )
        print(stdout.read().decode('utf-8'))

    except Exception as e:
        print("Error:", e)
    finally:
        client.close()

if __name__ == '__main__':
    check_formats()
