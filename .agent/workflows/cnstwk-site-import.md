---
description: 공사 현장지역 보강 (분기 1회 조달데이터허브 임포트)
---

# 공사 현장지역 보강 워크플로우

## 배경
- 공사 계약은 경쟁입찰은 공고현장(bid_notices_raw)으로 파악 가능
- **수의계약은 공고가 없어** 현장소재지 파악 불가 → 키워드 필터만 의존
- 분기 1회 조달데이터허브 임포트로 **97%+ 현장 파악률** 달성
- `filter_cnstwk_by_site()` + `process_contract_row()` 에서 자동 활용

## ⚠️ 주의사항
- **절대 로컬 DB를 서버에 업로드하지 말 것** (서버 DB가 최신, 로컬은 구 버전)
- 엑셀에서 현장 데이터만 추출 → JSON으로 서버 전송 → **서버 DB에 직접 UPDATE**

## 절차

### 1. 조달데이터허브에서 다운로드 (2개 파일)

#### 1-1. 부산 수요기관 전체
- 사이트: 조달데이터허브 → `공사업무별 구성원별 계약 내역` (신규 형식)
- 검색조건:
  - 기간선택: 기준일자
  - 기준일자: 해당 연도 1월 1일 ~ 현재일
  - 수요기관소재시도: **부산광역시** (지역무관 선택 X)
  - 나머지: 기본값
- 엑셀 다운로드 (xlsx)
- **헤더 0행** (첫 행이 컬럼명)

#### 1-2. 해양수산부 별도 다운로드 (소재지 업데이트 전까지)
- **이유**: 해양수산부는 2025.12 부산 이전했으나, 조달데이터허브 기관 소재지가
  아직 부산으로 반영되지 않아 1-1 다운로드에서 누락됨
- 동일 메뉴에서 검색조건:
  - 기간선택: 기준일자
  - 기준일자: 해당 연도 1월 1일 ~ 현재일
  - 수요기관소재시도: **지역무관**
  - 수요기관: **해양수산부** (검색하여 선택)
  - 나머지: 기본값
- 엑셀 다운로드 (xlsx)
- **확인**: 조달데이터허브에서 해양수산부 소재지가 부산으로 변경되면 이 단계 삭제

> [!IMPORTANT]
> 매 분기 임포트 시 조달데이터허브에서 해양수산부 소재지가 부산으로 업데이트되었는지
> 확인할 것. 1-1에서 해양수산부 건이 잡히면 1-2를 제거.

> [!WARNING]
> "부산항 진해신항"은 이름에 "부산"이 있지만 **실제 현장은 경상남도 창원시 진해구**임.
> 공고 현장 데이터에서도 "경상남도"로 등록됨. 비부산 배제 대상이 맞음.

### 2. 파일 저장
- 경로: 프로젝트 폴더 또는 `C:\Users\doors\OneDrive\바탕 화면\사무실 작업\`
- 파일명은 변경될 수 있음 (내부 컬럼 형식은 동일)

### 3. 임포트 (서버 DB 직접 업데이트)

**로컬에서 실행** — 엑셀에서 현장 데이터 추출 후 SSH로 서버 DB에 직접 반영

```python
import pandas as pd, paramiko, json, sys
sys.stdout.reconfigure(encoding='utf-8')

# ★ 파일 경로 (파일명은 변경될 수 있음)
EXCEL_MAIN = r"C:\dev\busan-city-local-products\공사업무별 구성원별 계약 내역.xlsx"
EXCEL_HMS  = r"C:\dev\busan-city-local-products\공사업무별 구성원별 계약 내역_해양수산부.xlsx"

# 1. 엑셀 로드 (header=0, 첫 행이 컬럼명)
dfs = []
for path in [EXCEL_MAIN, EXCEL_HMS]:
    try:
        d = pd.read_excel(path)  # header=0 기본값
        dfs.append(d)
        print(f"  로드: {path.split(chr(92))[-1]} → {len(d)}행")
    except FileNotFoundError:
        print(f"  스킵: {path.split(chr(92))[-1]} (파일 없음)")

df = pd.concat(dfs, ignore_index=True)
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
cols = [r[1] for r in conn.execute("PRAGMA table_info(cnstwk_cntrct)").fetchall()]
if "cnstrtsiteRgnNm" not in cols:
    conn.execute("ALTER TABLE cnstwk_cntrct ADD COLUMN cnstrtsiteRgnNm TEXT")

rows = conn.execute("SELECT rowid, cnstwkNm, dminsttNm_req, cnstrtsiteRgnNm FROM cnstwk_cntrct").fetchall()
matched = 0
skip = 0
for rowid, cnm, dnm, existing in rows:
    if existing and str(existing).strip():
        skip += 1
        continue
    key = (str(dnm or "").strip(), str(cnm or "").strip())
    site = by_name.get(key)
    if site:
        conn.execute("UPDATE cnstwk_cntrct SET cnstrtsiteRgnNm=? WHERE rowid=?", (site, rowid))
        matched += 1
conn.commit()
total = conn.execute("SELECT COUNT(*) FROM cnstwk_cntrct").fetchone()[0]
has = conn.execute("SELECT COUNT(*) FROM cnstwk_cntrct WHERE cnstrtsiteRgnNm IS NOT NULL AND cnstrtsiteRgnNm != ''").fetchone()[0]
busan = conn.execute("SELECT COUNT(*) FROM cnstwk_cntrct WHERE cnstrtsiteRgnNm LIKE '%부산%'").fetchone()[0]
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
- 대시보드 접속하여 공사 수주율 확인
- 비부산 현장 배제로 발주액 감소 → 수주율 **소폭 상승** 예상

## 매칭 방식
| 우선순위 | 키 | 비고 |
|---------|-----|------|
| 1차 | 수요기관+계약명 | 현재 방식, 94%+ 매칭률 |

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
- 공동수급 업체별 행이 분리됨 → `drop_duplicates('계약번호')`로 유니크화
- 기존 현장정보가 있는 건(공고에서 가져온 것)은 **skip** (덮어쓰지 않음)
- "부산항 진해신항" = 물리적 소재지 경남 창원시 진해구 → 비부산 배제 대상

## 부산업체 소재지 교차검증 (선택)

조달데이터허브 엑셀에는 `계약시점 업체소재시군구` 컬럼이 있어 **조달청 공식 업체 소재지**를 확인할 수 있음.

```python
# 엑셀에서 부산 업체 사업자번호 추출
busan_biznos_excel = set()
for _, r in df.iterrows():
    region = str(r.get('계약시점 업체소재시군구', '')).strip()
    bno = str(r.get('사업자등록번호', '')).strip().replace('-','').replace('.0','')
    if '부산' in region and bno and len(bno) >= 10:
        busan_biznos_excel.add(bno[:10])
# 현재 biznos와 차집합 확인 → 신규 있으면 company_master에 추가
```

> [!NOTE]
> 2026.4 기준 검증 결과: 엑셀 1,869개 중 신규 3개(0.16%)만 발견.
> 현재 4중 보강 체계가 충분히 정확함을 확인.
