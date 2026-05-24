# 부산 조달 모니터링 시스템 — 서버·서비스·DB 정보

> 최종 업데이트: 2026-05-08

## 인프라 개요

| 구분 | 항목 | 값 | 비고 |
|:---:|------|------|------|
| **서버** | 서버명 | `busanlocalproducts` | NCP (네이버 클라우드) |
| | OS | Ubuntu 24.04 (s2-g3, vCPU 2, 8GB) | |
| | 공인 IP | `49.50.133.160` | |
| | SSH | `root@49.50.133.160:22` | PW: `back9900@@` |
| | GitHub | https://github.com/doors1118-sketch/busan-city-local-products | Public |
| **서비스** | `busan-api` | FastAPI · 포트 **8000** · `/opt/busan/` | http://49.50.133.160:8000/docs |
| | `busan-dashboard` | Streamlit · 포트 **8501** · `/opt/busan/` | http://49.50.133.160:8501 |
| | `busan-advisor-pilot` | Streamlit · `/opt/advisor/` | 조달 법령 자문 챗봇 |
| **DB** | `procurement_contracts.db` | 계약 대장 (공사/용역/물품/쇼핑몰 83만건) | `/opt/busan/` |
| | `busan_agencies_master.db` | 수요기관 마스터 (4,885건) | `/opt/busan/` |
| | `busan_companies_master.db` | 부산 지역업체 마스터 | `/opt/busan/` |
| | `servc_site.db` | 용역 현장 소재지 | `/opt/busan/` |
| | `chatbot_company.db` | 챗봇용 업체 DB (업체/면허/인증/MAS/쇼핑몰) | `/opt/busan/` |
| | `api_cache.json` | 대시보드 API 캐시 | `/opt/busan/` |
| | `monthly_cache.json` | 월별 추이 캐시 | `/opt/busan/` |
