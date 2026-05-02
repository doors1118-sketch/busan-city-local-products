import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

def run(cmd):
    stdin, stdout, stderr = client.exec_command(cmd)
    return stdout.read().decode('utf-8').strip()

print("DB 현황:")
print(run("sqlite3 /opt/busan/procurement_contracts.db \"SELECT COUNT(*) FROM cnstwk_cntrct WHERE cnstrtsiteRgnNm NOT LIKE '%부산%';\""))

print("\n로그 현황:")
print(run("grep -i '현장 타지역' /opt/busan/sync_log/cache_build.log | tail -n 5"))
print(run("grep -i '현장배제' /opt/busan/sync_log/cache_build.log | tail -n 5"))
