import paramiko

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "U7$B%U5843m"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=10)

commands = [
    "cat /etc/resolv.conf",
    "nslookup sens.apigw.ntruss.com 2>&1 || echo 'nslookup failed'",
    "dig sens.apigw.ntruss.com +short 2>&1 || echo 'dig not installed'",
    "ping -c 2 sens.apigw.ntruss.com 2>&1 || echo 'ping failed'",
    "curl -sI https://sens.apigw.ntruss.com 2>&1 | head -5 || echo 'curl failed'",
]
for cmd in commands:
    print(f"--- {cmd.split()[0]} ---")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    print(stdout.read().decode('utf-8'))
    err = stderr.read().decode('utf-8')
    if err:
        print("ERR:", err)

ssh.close()
