# 부산광역시 조달 모니터링 시스템 — 운영 인수인계서

> 최종 작성: 2026-05-24

---

## 1. 시스템 개요

부산광역시 지역업체 수주율을 실시간 모니터링하는 시스템. 조달청 API에서 매일 전국 계약 데이터를 수집하고, 부산 관련 계약만 필터링하여 수주율·유출품목·보호제도 적용 현황을 분석한다.

### 핵심 구성요소

```
[NCP 서버: 49.50.133.160]
├── daily_pipeline_sync.py  ← 매일 03:00 크론 (데이터 수집)
├── build_api_cache.py      ← 매일 04:00 크론 (캐시 생성)
├── alert_check.py          ← 평일 09:00 크론 (이상감지 SMS)
├── api_server.py           ← 상시 실행 (FastAPI, systemd)
├── core_calc.py            ← 수주율 계산 공통 모듈 (★ 핵심)
└── .env                    ← SERVICE_KEY (조달청 API 인증키)
```

### 기술 스택
- **서버**: NCP Ubuntu 24.04 (s2-g3), 공인IP `49.50.133.160`
- **언어**: Python 3 + FastAPI
- **DB**: SQLite (procurement_contracts.db 외 5개)
- **배포**: GitHub → 서버 수동 `git pull`
- **알림**: NCP SENS SMS (발신: 051-888-7694)

---

## 2. 서버 접속 정보

| 항목 | 값 |
|------|------|
| SSH | `root@49.50.133.160:22`, PW: `back9900@@` |
| API | http://49.50.133.160:8000/docs (Swagger) |
| GitHub | https://github.com/doors1118-sketch/busan-city-local-products |
| systemd 서비스 | `busan-api.service` |
| 서비스 유저 | `busan-monitor` |
| 프로젝트 경로 | `/opt/busan/` |
| 가상환경 | `/opt/busan/venv/` |

> ⚠️ 사무실 네트워크에서 SSH 차단됨 — NCP 웹 콘솔 또는 집에서 paramiko로 접속

### paramiko 접속 패턴 (Windows)
```python
import paramiko
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)
stdin, stdout, stderr = client.exec_command('cd /opt/busan && git pull origin main --ff-only')
print(stdout.read().decode('utf-8'))
client.close()
```

---

## 3. 데이터베이스 구조

### 주요 DB 파일

| DB | 용도 | 크기(참고) |
|----|------|-----------|
| `procurement_contracts.db` | 계약 대장 (메인) | ~450MB |
| `busan_agencies_master.db` | 수요기관 마스터 (4,885건) | ~1.3MB |
| `busan_companies_master.db` | 부산 업체 마스터 | ~21MB |
| `servc_site.db` | 용역 현장 소재지 | ~18MB |
| `staging_chatbot_company.db` | 챗봇용 업체 DB | ~51MB |
| `chatbot_company.db` | 챗봇 운영 DB | ~300KB |

### procurement_contracts.db 주요 테이블

| 테이블 | 분야 | 설명 |
|--------|------|------|
| `cnstwk_cntrct` | 공사 | 중앙+자체조달 공사 계약 |
| `servc_cntrct` | 용역 | 중앙+자체조달 용역 계약 |
| `thng_cntrct` | 물품 | 중앙+자체조달 물품 계약 |
| `shopping_cntrct` | 쇼핑몰 | 종합쇼핑몰 계약 |
| `bid_notices_raw` | 입찰공고 | 공사 현장 소재지 (108만건) |
| `bid_notices_price` | 공고가격 | 추정가격+의무공동수급여부 |
| `busan_award_*` | 낙찰정보 | 부산 지역제한 확정 정보 |
| `sync_log` | 동기화 | 일일 수집 성공 기록 |

### 핵심 컬럼 설명

| 컬럼 | 설명 |
|------|------|
| `untyCntrctNo` | 통합계약번호 (중복 제거 PK) |
| `dcsnCntrctNo` | 확정계약번호 (앞8자리=기관+공종, 끝2자리=차수) |
| `dminsttCd` | 수요기관코드 (★ 매칭 기준, cntrctInsttCd 아님!) |
| `dminsttList` | 수요기관 목록 원본 (`[1^코드^이름^...]`) |
| `corpList` | 계약업체 목록 (`[순번^역할^...^지분율^...^사업자번호]`) |
| `thtmCntrctAmt` | 당해계약금액 (★ 금액 기준, 장기계속계약 금년도분) |
| `rgstDt` | 등록일시 |

---

## 4. 일일 파이프라인 (`daily_pipeline_sync.py`)

### 실행 방식
- **자동**: 크론 매일 03:00 실행
- **수동**: `python3 daily_pipeline_sync.py 20260522` (특정 날짜 보충)
- **자동 보충**: 마지막 성공일(`sync_log`) 이후 빠진 날짜 자동 수집

### 실행 순서 (10단계)

```
[사전] API 헬스체크 → 실패 시 건너뜀 (catch-up 보존)
[사전] SERVICE_KEY 빈 값 체크 → 실패 시 즉시 중단

Step 1.0  수요기관 마스터 API 동기화  (ao/UsrInfoService02)
Step 1.5  조달업체 마스터 동기화      (ao/UsrInfoService02)
Step 1.6  조달업체 업종정보 동기화    (ao/UsrInfoService02)
Step 1.7  공사 입찰공고 동기화        (부산 현장 필터)
Step 1.8  용역 조달요청 현장 동기화   (servc_site.db)
Step 1.9  낙찰정보 브릿지 동기화      (부산 지역제한 3분야)
Step 2.0  입찰공고 추정가격 동기화
Step 2    전국 계약 데이터 수집 (7종)
          ├ 공사_중앙/자체 (getCntrctInfoListCnstwkPPSSrch / getCntrctInfoListCnstwk)
          ├ 용역_중앙/자체
          ├ 물품_중앙/자체
          └ 쇼핑몰
Step 3    DB 저장 (부산 필터 → APPEND, 중복 선삭제)
Step 3.5  dminsttList → dminsttCd 자동 파싱
Step 3.6  용역 현장 매칭 (servc_site.db → servc_cntrct)
Step 3.7  사전규격 수집
Step 3.5b 챗봇 DB 동기화 (migrate_chatbot_db.py)
Step 4    API 캐시 재생성 (build_api_cache.py)
Step 5    경보 체크 → 크론에서 별도 실행
Step 6    DB 백업 (NCP Object Storage, 7일 보관)
```

### API 파라미터 규칙 (2026-05-24 확인)

| 항목 | 값 | 비고 |
|------|------|------|
| 기본 URL | `https://apis.data.go.kr/1230000/ao/CntrctInfoService/` | `ao/` prefix 필수 |
| `inqryDiv` | **반드시 `1`** | 등록일시 기준. `2`는 통합계약번호(날짜X) |
| 날짜 형식 | `YYYYMMDDHHMM` (12자리) | 예: `202605220000` ~ `202605222359` |
| 인증키 | `.env`의 `SERVICE_KEY` | 코드 자체에 자동 로딩 로직 있음 |

### 에러 대응

| 에러 | 원인 | 조치 |
|------|------|------|
| `HTTP 401` | SERVICE_KEY 없거나 잘못됨 | `.env` 파일 확인 |
| `resultCode: 08` | 필수 파라미터 누락 | `inqryDiv`, 날짜 형식 확인 |
| `HTTP 500` | API 서버 점검/장애 | 조달청 공지 확인, 자동 보충 대기 |
| 로그에 `🔑 SERVICE_KEY가 비어있습니다!` | `.env` 파일 비었음 | `.env`에 키 재입력 |

---

## 5. 크론탭 구성

```bash
# === 모니터링 핵심 (busan-monitor 유저) ===
# 일일 데이터 수집
0 3 * * * cd /opt/busan && . /opt/busan/.env && mkdir -p sync_log && python3 daily_pipeline_sync.py >> sync_log/daily.log 2>&1

# API 캐시 재빌드 + 서버 재시작
0 4 * * * cd /opt/busan && . /opt/busan/.env && cp api_cache.json api_cache_prev.json && python3 build_api_cache.py >> sync_log/cache_build.log 2>&1 && python3 build_monthly_cache.py >> sync_log/monthly_build.log 2>&1 && sudo systemctl restart busan-api

# 이상감지 경보 (평일)
0 9 * * 1-5 cd /opt/busan && . /opt/busan/.env && python3 alert_check.py >> alert_log/alert.log 2>&1

# === 챗봇 관련 ===
0 5 * * *   챗봇 DB 마스터 이관 (bootstrap_master_data.py)
15 5 * * *  기술개발제품 인증 API 수집 (import_certified_product_api.py)
30 5 * * *  혁신장터 API 수집 (import_innovation_product_api.py)
45 5 * * *  종합쇼핑몰 API 수집 (import_mas_product_api.py)
35 5 * * *  직접생산 인증 API (import_direct_production_cert_api.py)
0 6 * * 0   국세청 사업자등록상태 배치 (주 1회 일요일, nts_batch_sync.py)
20 6 * * 0  조달물품분류 동기화 (주 1회 일요일)
```

> ⚠️ 모든 크론에 `. /opt/busan/.env &&`가 있어야 함 (환경변수 로딩)

---

## 6. 캐시 생성 (`build_api_cache.py`)

### 동작 원리
1. `procurement_contracts.db`에서 전체 계약 데이터 로드
2. `core_calc.py`의 11개 필터 규칙 적용 (현장, 키워드, 전화번호, 낙찰정보 등)
3. 수주율·유출품목·보호제도·기관순위 등 20개+ 분석 항목 계산
4. **`api_cache.json`** (~11MB) 생성
5. `api_server.py`가 이 파일을 읽어서 REST API로 서빙

### 캐시 키 구조 (주요)
| 키 | 내용 |
|----|------|
| `1_수주율_전체` | 전체/분야별 수주율 |
| `2_수주율_그룹별` | 부산시/국가 그룹별 수주율 |
| `3_기관순위_*` | 상위/하위 10개 기관 |
| `5_유출품목_*` | 분야별 유출품목 TOP |
| `6_유출계약_*` | 대형 유출계약 목록 |
| `8_보호제도_*` | 지역제한/의무공동수급 현황 |
| `weekly_history` | 주간 수주율 추이 |

### 수동 캐시 재빌드
```bash
cd /opt/busan
python3 build_api_cache.py    # api_cache.json 생성
python3 build_monthly_cache.py # monthly_cache.json 생성
systemctl restart busan-api    # 서버에 반영
```

> ⚠️ **로컬에서 빌드하면 로컬 DB 기준**으로 생성됨. 반드시 **서버에서 실행**할 것.

---

## 7. API 서버 (`api_server.py`)

### systemd 서비스
```ini
# /etc/systemd/system/busan-api.service
[Service]
EnvironmentFile=/opt/busan/.env
User=busan-monitor
WorkingDirectory=/opt/busan
ExecStart=/opt/busan/venv/bin/python3 /opt/busan/api_server.py
Restart=always
```

### 주요 명령어
```bash
systemctl status busan-api      # 상태 확인
systemctl restart busan-api     # 재시작
journalctl -u busan-api -n 50   # 로그 확인
```

### 주요 API 엔드포인트

| 엔드포인트 | 용도 |
|-----------|------|
| `/api/summary` | 종합 수주율 |
| `/api/ranking` | 기관별 순위 |
| `/api/leakage` | 유출품목/계약 |
| `/api/protection` | 보호제도 현황 |
| `/api/private-contract` | 수의계약 분석 |
| `/api/local-companies` | 지역업체 현황 |
| `/api/economic-impact` | 경제효과 |
| `/api/agency/search?q=` | 기관 검색 |
| `/api/chatbot/*` | 챗봇 전용 API |
| `/docs` | Swagger 문서 |

---

## 8. 핵심 계산 모듈 (`core_calc.py`)

**⚠️ 수주율 계산은 반드시 이 모듈을 import해서 사용. 직접 코드 작성 금지.**

### 11개 규칙 요약

| # | 규칙 | 설명 |
|---|------|------|
| 1 | 지분율 파싱 | corpList에서 사업자번호+지분율 추출 |
| 2 | dminsttCd 우선 매칭 | 수요기관 기준 부산 판별 |
| 3 | 공사현장 필터 | bid_notices_raw 현장 소재지 |
| 3b | 용역현장 필터 | 정부기관만 적용, 부산시는 스킵 |
| 3c | 쇼핑몰 공사자재 필터 | 기관별 공고 현장 분석 |
| 4 | 전화번호 필터 | 051/070/010 외 배제 |
| 5 | 키워드 필터 | 124개 타지역 지명 |
| 6 | 낙찰정보 브릿지 | 부산 지역제한이면 복원 |
| 7 | 입찰공고 rgnLmtInfo | 추가 복원 |
| 8 | compare_unit | 기관 통합 집계 |
| 11 | dcsnCntrctNo 차수 | 최신 차수만 |

---

## 9. 이상감지 경보 (`alert_check.py`)

### 3단계 모니터링
| 단계 | 시점 | 감지 |
|------|------|------|
| A-0 | 수집 확인 | sync_log 전일 수집 완료 여부 (CRITICAL) |
| A | 캐시 비교 | 수주율 급변, 대형유출 신규, 발주액 이상 |
| B-1 | 공고 단계 | 지역제한/의무공동도급 미적용 사전 경보 |
| B-2 | 계약 단계 | 외지업체 지분 60% 초과 |

### SMS 알림 설정
- 설정 파일: `/opt/busan/alert_config.json` (git 미추적)
- 발신: `051-888-7694` (부산광역시)
- 수신: 3명 (alert_config.json의 recipients)
- NCP SENS API 사용 (HMAC-SHA256 서명)

---

## 10. 배포 절차

### 일반 코드 배포
```bash
# 1. 로컬에서 커밋/푸시
git add . && git commit -m "설명" && git push origin main

# 2. 서버에서 풀 (SSH 또는 paramiko)
cd /opt/busan && git pull origin main --ff-only

# 3. API 서버 재시작 (코드 변경 시)
systemctl restart busan-api
```

### DB/캐시 변경 시
```bash
# 서버에서 캐시 재빌드
cd /opt/busan
python3 build_api_cache.py
python3 build_monthly_cache.py
systemctl restart busan-api
```

### 긴급 롤백
```bash
cd /opt/busan && git reset --hard origin/main && systemctl restart busan-api
```

---

## 11. 주요 설정 파일 (git 미추적)

| 파일 | 위치 | 내용 | 비고 |
|------|------|------|------|
| `.env` | `/opt/busan/.env` | `SERVICE_KEY=조달청API키` | 코드 자동 로딩 |
| `alert_config.json` | `/opt/busan/alert_config.json` | SMS/텔레그램/이메일 설정 | NCP SENS 키 포함 |
| `*.db` | `/opt/busan/*.db` | 모든 SQLite DB | 크기 때문에 git 제외 |
| `api_cache.json` | `/opt/busan/api_cache.json` | 캐시 데이터 | 서버에서만 생성 |

---

## 12. 정기 수동 작업

### 월 1회: 용역 현장 데이터 보강
- **워크플로우**: `.agent/workflows/servc-site-import.md`
- **방법**: 조달데이터허브에서 `UI-ADOXCA-001R.용역 계약업체 내역` 다운로드 → DB 반영
- **효과**: 용역 현장 파악률 향상 (API만으로는 4.5%)

### 분기 1회: 공사 현장 데이터 보강
- **워크플로우**: `.agent/workflows/cnstwk-site-import.md`
- **방법**: 조달데이터허브 공사 계약 엑셀 → DB 반영

### 수기 계약 반영 (요청 시)
- **스크립트**: `import_manual_contracts.py`
- **마커**: `bsnsDivNm='수기입력'`, `untyCntrctNo=MANUAL_*`
- **롤백**: `python import_manual_contracts.py --rollback`

---

## 13. 트러블슈팅

### 파이프라인 수집 실패 시
```bash
# 1. 로그 확인
tail -50 /opt/busan/sync_log/daily.log

# 2. sync_log 마지막 성공일 확인
python3 -c "import sqlite3; c=sqlite3.connect('procurement_contracts.db'); print(c.execute('SELECT MAX(sync_date) FROM sync_log').fetchone())"

# 3. 수동 보충 실행
cd /opt/busan && . .env && python3 -u daily_pipeline_sync.py 20260522 >> sync_log/daily.log 2>&1

# 4. 캐시 재빌드 + 서버 재시작
python3 build_api_cache.py && systemctl restart busan-api
```

### API 서버 장애 시
```bash
systemctl status busan-api          # 상태 확인
journalctl -u busan-api -n 100      # 에러 로그
systemctl restart busan-api          # 재시작
```

### 조달청 API 키 갱신
1. 공공데이터포털(data.go.kr) 로그인
2. 마이페이지 → API 키 확인/갱신
3. `/opt/busan/.env`에 `SERVICE_KEY=새키` 입력
4. `systemctl restart busan-api`

### 디스크 부족 시
```bash
# DB 백업 파일 정리
ls -la /opt/busan/backup/
# 오래된 로그 삭제
find /opt/busan/sync_log -mtime +30 -delete
```

---

## 14. 데이터 흐름도

```
조달청 OpenAPI (apis.data.go.kr)
    │
    │  daily_pipeline_sync.py (매일 03:00)
    │  7개 API 호출 (공사/용역/물품 × 중앙/자체 + 쇼핑몰)
    │
    ▼
procurement_contracts.db (전국 데이터)
  + busan_agencies_master.db (부산 기관 4,885개)
  + busan_companies_master.db (부산 업체)
  + servc_site.db (용역 현장 소재지)
  + bid_notices_raw (공사 입찰공고 108만건)
  + busan_award_* (낙찰정보)
    │
    │  build_api_cache.py (매일 04:00)
    │  core_calc.py 11개 필터 적용
    │
    ▼
api_cache.json (~11MB)
    │
    │  api_server.py (FastAPI, 상시 실행)
    │
    ▼
REST API (http://49.50.133.160:8000)
    │
    ├── 대시보드 업체 (시각화)
    ├── Streamlit 담당자 조회
    └── 챗봇 (조달 자문)

별도 경로:
alert_check.py (평일 09:00)
  → api_cache.json vs api_cache_prev.json 비교
  → 이상 감지 시 NCP SMS 발송
```

---

## 15. 과거 장애 이력

### 2026-05-22~24 파이프라인 장애
- **원인**: 크론에서 `.env` 미로딩 + `.env` 파일 내용 소실
- **조치**: 코드에 `.env` 자동 로더 추가, 크론에 `. .env` 추가, `inqryDiv=2→1` 수정
- **교훈**: 환경변수 전달 경로를 이중화할 것

### 2026-03-27 조달청 점검
- **원인**: 조달청 네트워크 장비 교체 (19:00~익일 21:00)
- **조치**: `check_api_health()` 추가로 점검 시 자동 스킵, catch-up 메커니즘으로 복구

---

## 16. 연락처 및 외부 서비스

| 서비스 | 용도 | 계정/URL |
|--------|------|----------|
| 공공데이터포털 | 조달청 API 키 발급 | data.go.kr |
| 조달데이터허브 | 월간 수동 데이터 보강 | hub.g2b.go.kr |
| NCP 콘솔 | 서버 관리, SMS 관리 | console.ncloud.com |
| NCP Object Storage | DB 자동 백업 | backup_db.py |
| GitHub | 소스코드 관리 | doors1118-sketch/busan-city-local-products |

---

## 17. 핵심 운영 규칙 (반드시 준수)

1. **수주율 계산은 반드시 `core_calc.py`를 import** — 필터 누락 방지
2. **캐시는 서버에서만 빌드** — 로컬 빌드 시 로컬 DB 기준이 됨
3. **`.env`는 git에 포함되지 않음** — 서버에서 직접 관리
4. **`api_cache.json`도 git에 포함되지 않음** — 서버에서 생성
5. **배포 후 `systemctl restart busan-api`** 필수
6. **test_integrity.py로 데이터 정합성 검증** — `python3 test_integrity.py`
7. **파이프라인 실패 시 sync_log 미기록 → 다음 실행 시 자동 보충**
