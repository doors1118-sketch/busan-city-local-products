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
# daily_pipeline_sync.py에 필요한 패키지 추가 설치
/opt/busan/venv/bin/pip install requests

# API 서버 재시작
systemctl restart busan-api
systemctl status busan-api
```

---

## 5단계 (선택): 일일 파이프라인 cron 등록

```bash
# crontab 편집
crontab -e

# 매일 새벽 2시에 파이프라인 실행, 완료 후 API 서버 재시작
0 2 * * * cd /opt/busan && /opt/busan/venv/bin/python daily_pipeline_sync.py >> /var/log/busan-sync.log 2>&1 && systemctl restart busan-api
```

---

## 확인 사항

- [ ] `git push --force` 완료
- [ ] 서버에 DB 4개 파일 존재 확인
- [ ] `http://49.50.133.160:8000/docs` 접속 확인
- [ ] API 응답 데이터 정상 확인

## 서버 정보

| 항목 | 값 |
|------|------|
| IP | `49.50.133.160` |
| API | http://49.50.133.160:8000/docs |
| SSH | `ssh root@49.50.133.160` (키: busan-key.pem) |
| 작업 디렉토리 | `/opt/busan/` |
