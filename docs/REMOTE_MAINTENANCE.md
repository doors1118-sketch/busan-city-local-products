# 원격 유지보수

운영 기준 경로는 `/opt/busan`, 배포 브랜치는 `main`이다. GitHub Actions의 수동 배포는 운영 작업트리가 깨끗하고 현재 커밋에서 `main` 최신 커밋으로 fast-forward할 수 있을 때만 실행된다.

2026-08-15에 기존 운영 브랜치의 검증된 변경을 `main`에 통합했다. 이후 일상 개발과 배포는 `main`만 사용한다. 운영 서버에서 직접 코드를 수정하지 않고 GitHub에 커밋한 뒤 Actions의 `운영 서버 수동 배포`를 실행한다.

새 PC 설정과 공통 배포 절차는 신용보증 저장소의 `docs/REMOTE_MAINTENANCE.md` 및 `scripts/setup-new-pc.ps1`을 기준으로 한다.
