# 원격 유지보수

운영 기준 경로는 `/opt/busan`, 배포 브랜치는 `main`이다. GitHub Actions의 수동 배포는 운영 작업트리가 깨끗하고 현재 커밋에서 `main` 최신 커밋으로 fast-forward할 수 있을 때만 실행된다.

2026-08-15에 기존 운영 브랜치의 검증된 변경을 `main`에 통합했다. 이후 일상 개발과 배포는 `main`만 사용한다. 운영 서버에서 직접 코드를 수정하지 않고 GitHub에 커밋한 뒤 Actions의 `운영 서버 수동 배포`를 실행한다.

새 PC 설정과 공통 배포 절차는 신용보증 저장소의 `docs/REMOTE_MAINTENANCE.md` 및 `scripts/setup-new-pc.ps1`을 기준으로 한다.

## 공공데이터 API 자동 복구

사전규격 API가 최종 재시도까지 실패하면 `api_recovery_queue`에 대상 날짜를 기록한다. `retry_public_api_failures.py`는 대기·실패 상태인 날짜를 다시 수집하며, `prespec_monitor`의 기본키 기반 `INSERT OR REPLACE`를 사용하므로 중복 적재하지 않는다.

운영 예약은 매일 08:10에 실행하고 09:00 경보보다 먼저 끝나도록 한다.

```cron
10 8 * * * cd /opt/busan && . /opt/busan/.env && /opt/busan/venv/bin/python3 retry_public_api_failures.py --limit 3 >> /opt/busan/sync_log/public_api_recovery.log 2>&1
```

복구에 성공한 사전규격 실패 기록은 감사 이력으로 보존하되 문자 경보에서는 제외한다. 복구가 다시 실패하면 큐에 남아 다음 날 재시도하고 기존 경보도 유지한다. 계약·DB 손상·디스크 경보처럼 자동 변경 위험이 큰 항목은 이 큐에서 실행하지 않는다.
