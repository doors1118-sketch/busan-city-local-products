# 🏠 집에서 할 일 (2026-03-13)

## 1단계: GitHub에 코드 Push

```bash
cd [프로젝트 폴더]
git push origin main --force
```

> ⚠️ `--force` 필수 — 히스토리를 재작성했으므로 강제 푸시 필요

---

## 2단계: 서버에 DB 파일 내려받기

SSH 접속 후 (또는 NCP 웹 콘솔에서):

```bash
cd /opt/busan

# DB 파일 다운로드 (NCP Object Storage → 서버)
wget https://kr.object.ncloudstorage.com/busan-deploy/procurement_contracts.db.zip
wget https://kr.object.ncloudstorage.com/busan-deploy/busan_agencies_master.db
wget https://kr.object.ncloudstorage.com/busan-deploy/busan_companies_master.db
wget https://kr.object.ncloudstorage.com/busan-deploy/servc_site.db

# 압축 해제
unzip procurement_contracts.db.zip
rm procurement_contracts.db.zip
```

---

## 3단계: 서버 Git 리셋 + 코드 동기화

```bash
cd /opt/busan
git fetch origin
git reset --hard origin/main
```

---

## 4단계: 필요 패키지 설치 + 서비스 재시작

```bash
# 필요 패키지 설치
/opt/busan/venv/bin/pip install requests streamlit

# API 서버 재시작
systemctl restart busan-api
systemctl status busan-api
```

---

## 5단계: 대시보드 배포

```bash
# systemd 서비스 등록
cat > /etc/systemd/system/busan-dashboard.service << 'EOF'
[Unit]
Description=Busan Dashboard (Streamlit)
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/busan
ExecStart=/opt/busan/venv/bin/streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable busan-dashboard
systemctl start busan-dashboard
systemctl status busan-dashboard
```

→ http://49.50.133.160:8501 에서 대시보드 확인

---

## 6단계 (선택): 일일 파이프라인 cron 등록

```bash
crontab -e

# 매일 새벽 2시에 파이프라인 실행, 완료 후 API 서버 재시작
0 2 * * * cd /opt/busan && /opt/busan/venv/bin/python daily_pipeline_sync.py >> /var/log/busan-sync.log 2>&1 && systemctl restart busan-api
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

> 💡 Streamlit은 파일 변경을 감지해서 자동 리로드합니다. 수정할 때마다 바로 결과 확인!

### 수정 완료 후 서버 반영

```bash
git add dashboard.py
git commit -m "대시보드 디자인 수정"
git push
```
→ 5분 후 서버(http://49.50.133.160:8501)에 자동 반영

즉시 반영하고 싶으면 SSH로:

```bash
cd /opt/busan && git pull origin main && systemctl restart busan-dashboard
```

---

## 확인 사항

- [ ] `git push --force` 완료
- [ ] 서버에 DB 4개 파일 존재 확인
- [ ] http://49.50.133.160:8000/docs API 접속 확인
- [ ] http://49.50.133.160:8501 대시보드 접속 확인
- [ ] 대시보드 수정 → push → 반영 확인

## 서버 정보

| 항목 | 값 |
|------|------|
| IP | `49.50.133.160` |
| API | http://49.50.133.160:8000/docs |
| 대시보드 | http://49.50.133.160:8501 |
| SSH | `ssh root@49.50.133.160` (키: busan-key.pem) |
| 작업 디렉토리 | `/opt/busan/` |
