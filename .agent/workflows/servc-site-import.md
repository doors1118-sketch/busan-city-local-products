---
description: 용역 현장지역 보강 (월 1회 조달데이터허브 임포트)
---

# 용역 현장지역 보강 워크플로우

## 배경
- 용역 계약은 현장지역 정보가 API에서 직접 제공되지 않음
- 일일 파이프라인에서 조달요청 API(`reqNo`)로 매칭하지만 커버리지 ~4.5%
- **월 1회 조달데이터허브 임포트로 94%+ 달성 가능**
- `core_calc.py`에 현장=부산 확인 시 키워드 필터 bypass 로직 적용됨

## ⚠️ 주의사항
- **절대 로컬 DB를 서버에 업로드하지 말 것** (서버 DB가 최신, 로컬은 구 버전)
- 엑셀에서 현장 데이터만 추출 → JSON으로 서버 전송 → **서버 DB에 직접 UPDATE**

## 현장 구분 로직 적용 순서
1. `filter_servc_by_site()`: 현장=비부산이면 사전 배제
2. `process_contract_row()`: 키워드+전화번호 필터 (단, **현장=부산이면 skip**)
3. 낙찰정보/공고 지역제한으로 bypass 가능

## 절차

### 1. 조달데이터허브에서 다운로드
- 사이트: 조달데이터허브 → `업무별 구성원별 계약 내역` (신규 형식)
- 검색조건:
  - 기간선택: 기준일자
  - 기준일자: 해당 연도 1월 1일 ~ 현재일
  - 수요기관소재시도: **부산광역시**
  - 나머지: 기본값
- 엑셀 다운로드 (xlsx)
- **헤더 0행** (첫 행이 컬럼명)

### 2. 파일 저장
- 경로: 프로젝트 폴더 또는 `C:\Users\doors\OneDrive\바탕 화면\사무실 작업\`
- 파일명은 변경될 수 있음 (내부 컬럼 형식은 동일)

### 3. 임포트 (서버 DB 직접 업데이트)

**로컬에서 실행** — 엑셀에서 현장 데이터 추출 후 SSH로 서버 DB에 직접 반영

```python
import pandas as pd, paramiko, json, sys
sys.stdout.reconfigure(encoding='utf-8')

# ★ 파일 경로 (파일명은 변경될 수 있음)
EXCEL_PATH = r"C:\dev\busan-city-local-products\업무별 구성원별 계약 내역.xlsx"

# 1. 엑셀 로드 (header=0, 첫 행이 컬럼명)
df = pd.read_excel(EXCEL_PATH)  # header=0 기본값
df['계약변경차수_n'] = pd.to_numeric(df['계약변경차수'], errors='coerce').fillna(0)
df = df.sort_values('계약변경차수_n', ascending=False)

# 2. 유니크 계약 + 현장 추출 (컬럼명: 공사현장)
csv_site = df.drop_duplicates('계약번호')[['계약번호', '공사현장', '계약명', '수요기관']].copy()
csv_site = csv_site[csv_site['공사현장'].notna() & (csv_site['공사현장'] != '')]
print(f"유니크 계약: {len(csv_site)}건")

# 3. 인덱스 생성 (수요기관+계약명)
by_name = {}
for _, r in csv_site.iterrows():
    key = f"{str(r['수요기관']).strip()}|||{str(r['계약명']).strip()}"
    by_name[key] = str(r['공사현장']).strip()

# 4. 서버 전송 & DB UPDATE
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
rows = conn.execute("SELECT rowid, cntrctNm, dminsttNm_req, cnstrtsiteRgnNm FROM servc_cntrct").fetchall()
matched = 0
skip = 0
for rowid, cnm, dnm, existing in rows:
    if existing and str(existing).strip():
        skip += 1
        continue
    key = (str(dnm or "").strip(), str(cnm or "").strip())
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
| **수요기관+계약명** | (수요기관, 계약명) | **94%+** | 현재 방식 |

## 핵심 컬럼 (신규 형식)
| 컬럼명 | 용도 |
|--------|------|
| `공사현장` | 현장 소재지 (시도/시군구) |
| `계약번호` | 유니크 키 |
| `계약명` | 매칭 키 |
| `수요기관` | 매칭 키 |
| `계약변경차수` | 최신 차수 선택용 |

## 참고
- 파일명은 변경될 수 있으나 **내부 컬럼 형식은 동일**
- 공동수급 건은 업체별 행 분리 → `drop_duplicates('계약번호')`로 유니크화
- 기존 현장정보가 있는 건은 **skip** (덮어쓰지 않음)
- `core_calc.py` 513행: 현장=부산이면 키워드/전화번호 필터 bypass
