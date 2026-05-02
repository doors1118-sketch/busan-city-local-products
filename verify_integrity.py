"""캐시 정합성(Total Rate) 크로스 체크"""
import json

def check_integrity():
    # 1. API Cache 로드
    with open('/opt/busan/api_cache.json', 'r', encoding='utf-8') as f:
        api_data = json.load(f)
    
    # 2. Monthly Cache 로드
    with open('/opt/busan/monthly_cache.json', 'r', encoding='utf-8') as f:
        mon_data = json.load(f)

    # API Cache 전체 수치
    api_total = api_data['total_rate']['발주액']
    api_local = api_data['total_rate']['수주액']
    api_rate = api_data['total_rate']['수주율']
    
    # Monthly Cache 전체 수치 (누계_그룹 -> 전체 -> 마지막 달)
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
    print(f"\n차이: 발주액 {diff_t:,}원 / 수주액 {diff_l:,}원")
    if diff_t == 0 and diff_l == 0:
        print("✅ 완벽 일치 (정합성 정상)")
    else:
        print("❌ 불일치 발생 (로직 점검 필요)")

if __name__ == '__main__':
    check_integrity()
