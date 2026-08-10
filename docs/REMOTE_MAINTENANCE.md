# 원격 유지보수

운영 기준 경로는 `/opt/busan`, 배포 브랜치는 `codex/monitoring-server-sync-20260611`이다. GitHub Actions의 수동 배포는 운영 작업트리가 깨끗하고 현재 커밋에서 지정 브랜치 최신 커밋으로 fast-forward할 수 있을 때만 실행된다.

현재 운영 서버에는 GitHub에 아직 정리되지 않은 커밋과 미커밋 변경이 있으므로 자동 배포가 의도적으로 차단된다. 운영 변경을 별도 백업하고 검토된 커밋으로 GitHub에 반영한 뒤 차단을 해제한다. `git reset --hard`, 강제 체크아웃, 운영 디렉터리 전체 복사로 차이를 제거하지 않는다.

새 PC 설정과 공통 배포 절차는 신용보증 저장소의 `docs/REMOTE_MAINTENANCE.md` 및 `scripts/setup-new-pc.ps1`을 기준으로 한다.
