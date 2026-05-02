"""서버 원격 캐시 정합성 크로스 체크"""
import paramiko

def run_cmd(client, cmd, label):
    print(f"\n  [{label}]")
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode('utf-8').strip()
    if out: print(out)

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

python_script = """
import json

try:
    with open('/opt/busan/api_cache.json', 'r', encoding='utf-8') as f:
        api_data = json.load(f)
    
    with open('/opt/busan/monthly_cache.json', 'r', encoding='utf-8') as f:
        mon_data = json.load(f)

    api_total = api_data['total_rate']['발주액']
    api_local = api_data['total_rate']['수주액']
    api_rate = api_data['total_rate']['수주율']
    
    mon_months = mon_data['누계_그룹']['전체']
    last_mon = mon_months[-1]
    mon_total = last_mon['발주액']
    mon_local = last_mon['수주액']
    mon_rate = last_mon['수주율']

    print("=== [캐시 수치 비교] ===")
    print(f"1. 종합현황 (api_cache): 발주액={api_total:,} | 수주액={api_local:,} | 수주율={api_rate}%")
    print(f"2. 종합분석 (monthly_cache): 발주액={mon_total:,} | 수주액={mon_local:,} | 수주율={mon_rate}%")
    
    diff_t = api_total - mon_total
    diff_l = api_local - mon_local
    print(f"\\n차이: 발주액 {diff_t:,}원 / 수주액 {diff_l:,}원")
    if diff_t == 0 and diff_l == 0:
        print("✅ 완벽 일치 (정합성 정상)")
    else:
        print("❌ 불일치 발생 (로직 점검 필요)")
except Exception as e:
    print("Error:", e)
"""

run_cmd(client, f"python3 -c \"{python_script}\"", "정합성 비교 결과")

client.close()
