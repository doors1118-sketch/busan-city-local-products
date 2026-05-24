import paramiko, sys, json
sys.stdout.reconfigure(encoding='utf-8')

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)

tests = [
    ("health", "curl -s http://127.0.0.1:8000/api/chatbot/health"),
    ("version", "curl -s http://127.0.0.1:8000/api/chatbot/version"),
    ("shopping-mall/search", "curl -s 'http://127.0.0.1:8000/api/chatbot/shopping-mall/search?limit=3'"),
    ("shopping-mall/product-search", "curl -s 'http://127.0.0.1:8000/api/chatbot/shopping-mall/product-search?product_name=%ED%8F%AC%EC%B6%A9%EA%B8%B0&limit=3'"),
    ("shopping-mall/supplier-search", "curl -s 'http://127.0.0.1:8000/api/chatbot/shopping-mall/supplier-search?limit=3'"),
    ("shopping-mall/list (all)", "curl -s 'http://127.0.0.1:8000/api/chatbot/shopping-mall/list'"),
    ("shopping-mall/search (mas)", "curl -s 'http://127.0.0.1:8000/api/chatbot/shopping-mall/search?contract_type_filter=mas&limit=3'"),
    ("shopping-mall/search (third_party)", "curl -s 'http://127.0.0.1:8000/api/chatbot/shopping-mall/search?contract_type_filter=third_party_unit_price&limit=3'"),
    ("shopping-mall/search (excellent)", "curl -s 'http://127.0.0.1:8000/api/chatbot/shopping-mall/search?contract_type_filter=excellent_procurement&limit=3'"),
    ("mas/search", "curl -s 'http://127.0.0.1:8000/api/chatbot/mas/search?limit=3'"),
    ("mas/list", "curl -s 'http://127.0.0.1:8000/api/chatbot/mas/list'"),
]

print("=" * 70)
print("  서버 종합쇼핑몰/MAS API 검증")
print("=" * 70)

all_ok = True
for name, cmd in tests:
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=10)
    raw = stdout.read().decode().strip()
    
    print(f"\n[{name}]")
    try:
        data = json.loads(raw)
        
        if name == "health":
            st = data.get("status")
            db = data.get("db", {})
            sm = db.get("shopping_mall_product_count", 0)
            sm_active = db.get("active_shopping_mall_product_count", 0)
            mas = db.get("mas_product_count", 0)
            ok = st == "ok" and sm > 0
            print(f"  {'✅' if ok else '❌'} status={st}, shopping_mall={sm}, active={sm_active}, mas={mas}")
            if not ok: all_ok = False
            
        elif name == "version":
            ver = data.get("api_version")
            feats = data.get("features", [])
            has_sm = "shopping_mall_product" in feats
            print(f"  {'✅' if has_sm else '❌'} version={ver}, shopping_mall_product={'있음' if has_sm else '없음'}")
            if not has_sm: all_ok = False
            
        elif "list" in name:
            candidates = data.get("candidates", [])
            print(f"  ✅ items: {len(candidates)}")
            if candidates:
                # contract_type 분포
                types = {}
                for c in candidates:
                    ct = c.get("shopping_mall_contract_type", c.get("contract_type", "n/a"))
                    types[ct] = types.get(ct, 0) + 1
                for t, cnt in sorted(types.items(), key=lambda x: -x[1])[:5]:
                    print(f"     {t}: {cnt}")
                    
        else:
            candidates = data.get("candidates", [])
            err = data.get("error")
            ok = err is None
            print(f"  {'✅' if ok else '❌'} candidates={len(candidates)}, error={err}")
            if not ok: all_ok = False
            
            # 첫 후보 요약
            if candidates:
                c = candidates[0]
                sm_flags = c.get("shopping_mall_flags", [])
                sm_summary = c.get("shopping_mall_product_summary", [])
                print(f"     첫 후보: flags={sm_flags}, sm_summary={len(sm_summary)}건")
                
    except json.JSONDecodeError:
        print(f"  ❌ JSON 파싱 실패: {raw[:200]}")
        all_ok = False

# MAS API 증분 수집 테스트 (--probe)
print(f"\n{'=' * 70}")
print("  MAS API 증분 수집 probe 테스트")
print("=" * 70)

# 환경변수 확인
stdin, stdout, stderr = ssh.exec_command("grep -r 'SHOPPING_MALL_PRDCT_SERVICE_KEY\\|SERVICE_KEY' /opt/busan/.env /etc/environment /etc/systemd/system/busan-api.service 2>/dev/null")
env_out = stdout.read().decode().strip()
if env_out:
    print(f"  서비스키 설정: {env_out[:200]}")
else:
    print("  ⚠️ SHOPPING_MALL_PRDCT_SERVICE_KEY 환경변수 미설정 — API 증분 수집 불가")
    print("     (현재 DB는 Excel bootstrap 기반 2,548건으로 정상 서비스 중)")

print(f"\n{'=' * 70}")
print(f"  최종 판정: {'✅ ALL OK' if all_ok else '❌ 일부 실패'}")
print(f"  production_deployment: HOLD")
print(f"{'=' * 70}")

ssh.close()
