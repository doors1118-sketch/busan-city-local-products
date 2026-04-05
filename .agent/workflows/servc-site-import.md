---
description: 용역 현장지역 보강 (월 1회 조달데이터허브 임포트)
---

# 용역 현장지역 보강 워크플로우

## 배경
- 용역 계약은 현장지역 정보가 API에서 직접 제공되지 않음
- 일일 파이프라인에서 조달요청 API(`reqNo`)로 매칭하지만 커버리지 ~4.5%
- **월 1회 조달데이터허브 CSV 임포트로 94%+ 달성 가능**
- `core_calc.py`에 현장=부산 확인 시 키워드 필터 bypass 로직 적용됨

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
- 경로: `c:\Users\doors\OneDrive\바탕 화면\사무실 작업\` 에 저장
- 파일명 예: `2026용역 계약업체 내역(1.1~4.5).csv`
- 인코딩: UTF-16, 구분자: 탭

### 3. 임포트 (계약명+수요기관코드 매칭)
```python
import pandas as pd, sqlite3

csv_path = r"c:\Users\doors\OneDrive\바탕 화면\사무실 작업\2026용역 계약업체 내역(1.1~4.5).csv"
DB = r"c:\Users\doors\OneDrive\바탕 화면\사무실 작업\busan-city-local-products\procurement_contracts.db"

# CSV 로드 (UTF-16, 탭 구분)
csv_df = pd.read_csv(csv_path, encoding='utf-16', sep='\t', low_memory=False)

# 최신 변경차수 기준 유니크 계약
csv_df = csv_df.sort_values('계약납품통합변경차수', ascending=False)
csv_site = csv_df.drop_duplicates('계약납품통합번호')[['계약납품통합번호','현장지역','계약명','수요기관']].copy()
csv_site = csv_site[csv_site['현장지역'].notna() & (csv_site['현장지역'] != '')]

# 수요기관코드 + 계약명 인덱스
csv_index = {}
for _, r in csv_site.iterrows():
    key = (str(r['수요기관']).strip(), str(r['계약명']).strip())
    csv_index[key] = str(r['현장지역']).strip()

# DB 매칭 (현장 비어있는 건만)
conn = sqlite3.connect(DB)
conn.execute("PRAGMA journal_mode=WAL")
db_rows = conn.execute("SELECT rowid, cntrctNm, dminsttCd, cnstrtsiteRgnNm FROM servc_cntrct").fetchall()

matched = 0
for rowid, cnm, dcd, existing_site in db_rows:
    if existing_site and existing_site.strip():
        continue
    key = (str(dcd).strip(), str(cnm).strip())
    site = csv_index.get(key)
    if site:
        conn.execute("UPDATE servc_cntrct SET cnstrtsiteRgnNm=? WHERE rowid=?", (site, rowid))
        matched += 1

conn.commit()

# 검증
has_site = conn.execute("SELECT COUNT(*) FROM servc_cntrct WHERE cnstrtsiteRgnNm IS NOT NULL AND cnstrtsiteRgnNm != ''").fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM servc_cntrct").fetchone()[0]
print(f"매칭: {matched}건, 현장파악률: {has_site}/{total} ({has_site/total*100:.1f}%)")
conn.close()
```

### 4. 서버 DB 반영
```bash
# 로컬 DB를 서버에 업로드 (또는 서버에서 직접 임포트)
scp procurement_contracts.db user@server:/opt/busan/
```

// turbo
### 5. 캐시 재빌드
```bash
cd /opt/busan && /opt/busan/venv/bin/python3 build_api_cache.py
```

// turbo
### 6. API 재시작
```bash
systemctl restart busan-api
```

### 7. 수주율 변화 확인
- 대시보드 접속하여 용역 수주율 변동 확인
- 현장=부산 건이 키워드 필터에서 복원되므로 수주율 **상승** 예상

## 매칭 방식 비교
| 방식 | 키 | 매칭률 | 비고 |
|------|-----|-----:|------|
| ~~dcsnCntrctNo LIKE~~ | 계약번호 | ~20% | 번호 체계 불일치로 저조 |
| **계약명+수요기관코드** | (dminsttCd, cntrctNm) | **94%+** | 현재 방식 |

## 참고
- CSV 계약번호(`20161027451_5`)와 DB 계약번호(`R26TE...`)는 체계가 다름
- 수요기관코드(`B551542`)는 동일 체계 → 기관코드+계약명 조합으로 정확 매칭
- 공동수급 건은 업체별 행 분리(CSV) vs 1행(DB) — 매칭에 영향 없음
- `core_calc.py` 513행: 현장=부산이면 키워드/전화번호 필터 bypass
