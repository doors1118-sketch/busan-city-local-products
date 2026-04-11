---
description: 용역 현장지역 보강 (월 1회 조달데이터허브 임포트)
---

# 용역 현장지역 보강 워크플로우

## 배경
- 용역 계약은 현장지역 정보가 API에서 직접 제공되지 않음
- 일일 파이프라인에서 조달요청 API(`reqNo`)로 매칭하지만 커버리지 ~4.5%
- **월 1회 조달데이터허브 CSV 임포트로 94%+ 달성 가능**
- `core_calc.py`에 현장=부산 확인 시 키워드 필터 bypass 로직 적용됨

## ⚠️ 주의사항
- **절대 로컬 DB를 서버에 업로드하지 말 것** (서버 DB가 최신, 로컬은 구 버전)
- CSV에서 현장 데이터만 추출 → JSON으로 서버 전송 → **서버 DB에 직접 UPDATE**

## 현장 구분 로직 적용 순서
1. `filter_servc_by_site()`: 현장=비부산이면 사전 배제
2. `process_contract_row()`: 키워드+전화번호 필터 (단, **현장=부산이면 skip**)
3. 낙찰정보/공고 지역제한으로 bypass 가능

## 절차

### 1. 조달데이터허브에서 다운로드
- 사이트: 조달데이터허브 → `UI-ADOXCA-001R.용역 계약업체 내역`
- 검색조건:
  - 기간선택: 기준일자
  - 기준일자: 해당 연도 1월 1일 ~ 현재일
  - 수요기관소재시도: **부산광역시**
  - 나머지: 기본값
- 엑셀 다운로드 (csv)

### 2. 파일 저장
- 경로: `C:\Users\doors\OneDrive\바탕 화면\사무실 작업\` 에 저장
- 파일명 예: `2026용역 계약업체 내역(1.1~4.5).csv`
- 인코딩: UTF-16, 구분자: 탭

### 3. 임포트 (서버 DB 직접 업데이트)

**로컬에서 실행** — CSV에서 현장 데이터 추출 후 SSH로 서버 DB에 직접 반영

```python
import pandas as pd, paramiko, json, sys
sys.stdout.reconfigure(encoding='utf-8')

csv_path = r"C:\Users\doors\OneDrive\바탕 화면\사무실 작업\2026용역 계약업체 내역(1.1~4.5).csv"

# 1. CSV 로드 (UTF-16, 탭 구분)
csv_df = pd.read_csv(csv_path, encoding='utf-16', sep='\t', low_memory=False)
csv_df = csv_df.sort_values('계약납품통합변경차수', ascending=False)
csv_site = csv_df.drop_duplicates('계약납품통합번호')[['계약납품통합번호','현장지역','계약명','수요기관']].copy()
csv_site = csv_site[csv_site['현장지역'].notna() & (csv_site['현장지역'] != '')]
print(f"유니크 계약: {len(csv_site)}건")

# 2. 인덱스 생성
by_name = {}
for _, r in csv_site.iterrows():
    key = f"{str(r['수요기관']).strip()}|||{str(r['계약명']).strip()}"
    by_name[key] = str(r['현장지역']).strip()

# 3. 서버 전송 & DB UPDATE
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect('49.50.133.160', 22, 'root', 'U7$B%U5843m', timeout=10)

sftp = c.open_sftp()
with sftp.open('/tmp/_site_data.json', 'w') as f:
    f.write(json.dumps({'by_name': by_name}, ensure_ascii=False))

import_script = r'''import sqlite3, json
data = json.load(open("/tmp/_site_data.json", encoding="utf-8"))
by_name = {}
for k, v in data["by_name"].items():
    parts = k.split("|||", 1)
    if len(parts) == 2:
        by_name[(parts[0], parts[1])] = v

conn = sqlite3.connect("/opt/busan/procurement_contracts.db")
conn.execute("PRAGMA journal_mode=WAL")
rows = conn.execute("SELECT rowid, cntrctNm, dminsttCd, cnstrtsiteRgnNm FROM servc_cntrct").fetchall()
matched = 0
skip = 0
for rowid, cnm, dcd, existing in rows:
    if existing and str(existing).strip():
        skip += 1
        continue
    key = (str(dcd or "").strip(), str(cnm or "").strip())
    site = by_name.get(key)
    if site:
        conn.execute("UPDATE servc_cntrct SET cnstrtsiteRgnNm=? WHERE rowid=?", (site, rowid))
        matched += 1
conn.commit()
total = conn.execute("SELECT COUNT(*) FROM servc_cntrct").fetchone()[0]
has = conn.execute("SELECT COUNT(*) FROM servc_cntrct WHERE cnstrtsiteRgnNm IS NOT NULL AND cnstrtsiteRgnNm != ''").fetchone()[0]
busan = conn.execute("SELECT COUNT(*) FROM servc_cntrct WHERE cnstrtsiteRgnNm LIKE '%부산%'").fetchone()[0]
print(f"매칭: {matched}건, 기존유지: {skip}건")
print(f"현장파악: {has}/{total} ({has/total*100:.1f}%)")
print(f"부산: {busan}, 비부산: {has-busan}")
conn.close()
'''
with sftp.open('/tmp/_import.py', 'w') as f:
    f.write(import_script)
sftp.close()

_, o, e = c.exec_command('/opt/busan/venv/bin/python3 /tmp/_import.py', timeout=60)
print(o.read().decode().strip())
err = e.read().decode().strip()
if err: print("ERR:", err[-200:])
c.close()
```

// turbo
### 4. 캐시 재빌드
```bash
cd /opt/busan && /opt/busan/venv/bin/python3 build_api_cache.py
```

// turbo
### 5. 월별 캐시 재빌드
```bash
cd /opt/busan && /opt/busan/venv/bin/python3 build_monthly_cache.py
```

// turbo
### 6. API 재시작
```bash
systemctl restart busan-api busan-dashboard
```

### 7. 수주율 변화 확인
- 대시보드 접속하여 용역 수주율 변동 확인
- 현장=부산 건이 키워드 필터에서 복원되므로 수주율 **상승** 예상

## 매칭 방식
| 방식 | 키 | 매칭률 | 비고 |
|------|-----|-----:|------|
| ~~dcsnCntrctNo LIKE~~ | 계약번호 | ~20% | 번호 체계 불일치로 저조 |
| **계약명+수요기관코드** | (dminsttCd, cntrctNm) | **94%+** | 현재 방식 |

## 참고
- CSV 계약번호(`20161027451_5`)와 DB 계약번호(`R26TE...`)는 체계가 다름
- 수요기관코드(`B551542`)는 동일 체계 → 기관코드+계약명 조합으로 정확 매칭
- 공동수급 건은 업체별 행 분리(CSV) vs 1행(DB) — 매칭에 영향 없음
- `core_calc.py` 513행: 현장=부산이면 키워드/전화번호 필터 bypass
