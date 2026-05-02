import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

def run(cmd):
    stdin, stdout, stderr = client.exec_command(cmd)
    return stdout.read().decode('utf-8').strip()

print("=== [서버 디스크 용량 확인] ===")
print("1. 전체 디스크 사용량")
print(run("df -h /"))

print("\n2. DB 파일 용량 (Top 5)")
print(run("ls -lh /opt/busan/*.db | awk '{print $5, $9}' | sort -hr | head -5"))

client.close()
