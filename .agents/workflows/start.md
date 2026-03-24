---
description: 새 대화 시작 시 자동 적용되는 작업 지침
---

# 작업 시작 규칙

1. **반드시 `PROJECT_STATUS.md`를 먼저 읽을 것**
   - 프로젝트 규칙, 핵심 파일, 필터 로직, 검증 기준값 확인

2. **수주율 관련 코드 작성 시 `core_calc.py`를 import하여 사용할 것**
   - 필터/지분계산 코드를 새로 작성하지 말 것
   - 임시 분석 스크립트에서도 반드시 core_calc 사용

3. **작업 완료 후 `PROJECT_STATUS.md`를 업데이트할 것**
   - 새 규칙/스크립트 추가 시 반드시 문서에 반영

4. **캐시 재생성 후 자동 검증 테스트 실행**
    ```
    python test_integrity.py
    ```
    - 전체 통과 확인 후 작업 완료 처리

5. **집에서 서버 SSH 접속 시 비밀번호 방식 사용**
    - Windows SSH 클라이언트(ssh.exe)는 stdin 비밀번호 입력이 안 됨
    - Python `paramiko` 라이브러리로 접속: `pip install paramiko`
    - 접속 정보: `root@49.50.133.160:22`, 비밀번호 인증
    - 배포 명령: `cd /opt/busan && git pull origin main --ff-only && python3 build_api_cache.py && systemctl restart busan-api`
