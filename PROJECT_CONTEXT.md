# 부산시 지역 특산물 조달 모니터링 프로젝트

## 프로젝트 개요
부산시청 구청 담당자용 지역 업체 수주율/현황 모니터링 시스템

## 서버 정보

| 항목 | 값 |
|------|-----|
| 서버명 | busanlocalproducts |
| 플랫폼 | 네이버클라우드 (NCP) |
| OS | Ubuntu 24.04 (s2-g3, vCPU 2, 8GB RAM) |
| 공인 IP | 49.50.133.160 |
| VPC/Subnet | busan-vpc / busan-subnet |
| 로그인 키 | busan-key (PEM 파일 사무실 PC에 있을 수 있음) |

### 열린 포트 (ACG)
- 22 (SSH), 443 (HTTPS), 8000 (API), 8501 (Streamlit), 3389 (RDP)

## 현재 서버 상태 (2026-03-13)

### 설치된 환경
- Python 3.12 + venv (`/opt/busan/venv`)
- FastAPI + Uvicorn
- Git

### 실행 중인 서비스
- **busan-api** (systemd): `active (running)`
- API Swagger UI: http://49.50.133.160:8000/docs
- 서비스 파일: `/etc/systemd/system/busan-api.service`
- 작업 디렉토리: `/opt/busan/`
- 실행 파일: `/opt/busan/venv/bin/python api_server.py`

### 서버 파일 구조
```
/opt/busan/
├── api_server.py       # FastAPI 메인 서버
├── api_cache.json      # API 캐시 데이터
├── venv/               # Python 가상환경
└── .git/               # Git 저장소 (이 GitHub 레포와 연결됨)
```

## 다음 작업
1. 사무실 코드 GitHub에 push
2. 서버에 코드 동기화
3. 구청 담당자용 대시보드 개발 (Streamlit, 포트 8501)
4. 데이터 전달 스케줄링
