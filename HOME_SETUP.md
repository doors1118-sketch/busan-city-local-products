# 🏠 집에서 할 일 (2026-03-14 업데이트)

## ⚠️ 긴급: 서버 코드 리셋 (1회만)

사무실에서 Git 히스토리를 정리하고 force push 했으므로, 서버 코드를 1회 리셋해야 합니다.

```bash
# SSH 접속 후
cd /opt/busan
git fetch origin
git reset --hard origin/main
systemctl restart busan-api
systemctl status busan-api   # active (running) 확인
```

> DB 파일, 크론잡, venv는 건드릴 필요 없음 (Git과 무관)

---

## 집 PC 코드 동기화

```bash
cd [프로젝트 폴더]
git fetch origin
git reset --hard origin/main
```

---

## 🎨 대시보드 디자인 수정 (로컬에서 빠르게)

### 최초 1회 설정 (집 PC)

```bash
# 1. 코드 받기
git clone https://github.com/doors1118-sketch/busan-city-local-products.git
cd busan-city-local-products

# 2. Python 가상환경 만들기
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Mac/Linux

# 3. 패키지 설치
pip install streamlit requests

# 4. DB 파일 다운로드 (NCP Object Storage)
curl -O https://kr.object.ncloudstorage.com/busan-deploy/procurement_contracts.db.zip
curl -O https://kr.object.ncloudstorage.com/busan-deploy/busan_agencies_master.db
curl -O https://kr.object.ncloudstorage.com/busan-deploy/busan_companies_master.db
curl -O https://kr.object.ncloudstorage.com/busan-deploy/servc_site.db

# 5. 압축 해제
tar -xf procurement_contracts.db.zip
del procurement_contracts.db.zip
```

### 대시보드 실행

```bash
streamlit run dashboard.py
```
→ http://localhost:8501 자동 오픈

### 수정 → 즉시 반영

```
dashboard.py 수정 → 저장(Ctrl+S) → 브라우저가 자동 새로고침 (1초)
```

> 💡 Streamlit은 파일 변경을 감지해서 자동 리로드합니다.

### 수정 완료 후 서버 반영

```bash
git add dashboard.py
git commit -m "대시보드 디자인 수정"
git push
```
→ 5분 후 서버(http://49.50.133.160:8501)에 자동 반영

---

## 서버 정보

| 항목 | 값 |
|------|------|
| IP | `49.50.133.160` |
| API | http://49.50.133.160:8000/docs |
| 대시보드 | http://49.50.133.160:8501 |
| SSH | `ssh root@49.50.133.160` (키: busan-key.pem) |
| 작업 디렉토리 | `/opt/busan/` |
| GitHub | https://github.com/doors1118-sketch/busan-city-local-products |
| NCP Object Storage | `busan-deploy` 버킷 |

## 크론잡 (이미 등록됨)

| 시간 | 작업 |
|------|------|
| 매일 새벽 2시 | `daily_pipeline_sync.py` (나라장터 API → DB 갱신) |
| 매일 새벽 4시 | `build_api_cache.py` (캐시 재생성 → API 재시작) |
| 5분마다 | GitHub pull + 서비스 재시작 |
