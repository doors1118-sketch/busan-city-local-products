#!/bin/bash
# 파이프라인 진단 + 자동 복구 스크립트
# 사용법: cd /opt/busan && git pull && bash fix_pipeline.sh

echo "==============================="
echo " 파이프라인 진단 시작"
echo "==============================="

echo ""
echo "=== 1. CRON 확인 ==="
crontab -l 2>/dev/null || echo "cron 없음!"

echo ""
echo "=== 2. 캐시 파일 날짜 ==="
ls -la /opt/busan/api_cache.json 2>/dev/null

echo ""
echo "=== 3. 파이프라인 로그 (최근 30줄) ==="
tail -30 /opt/busan/pipeline.log 2>/dev/null || echo "로그 파일 없음"

echo ""
echo "=== 4. 디스크 용량 ==="
df -h /opt/busan/

echo ""
echo "=== 5. DB 파일 날짜 ==="
ls -la /opt/busan/*.db 2>/dev/null | tail -5

echo ""
echo "=== 6. 서비스 상태 ==="
systemctl status busan-api --no-pager 2>/dev/null | head -10

echo ""
echo "==============================="
echo " 진단 완료. 아래 결과를 공유해주세요."
echo " 자동 복구를 원하면: bash fix_pipeline.sh fix"
echo "==============================="

# fix 인자가 있으면 자동 복구
if [ "$1" = "fix" ]; then
    echo ""
    echo "=== 자동 복구 시작 ==="
    
    # cron 재등록
    echo ">> cron 재등록..."
    (crontab -l 2>/dev/null | grep -v daily_pipeline_sync; echo "0 3 * * * cd /opt/busan && python3 daily_pipeline_sync.py >> /opt/busan/pipeline.log 2>&1") | crontab -
    echo "   cron 등록 완료:"
    crontab -l

    # 파이프라인 수동 실행
    echo ""
    echo ">> 파이프라인 수동 실행 (시간 소요)..."
    cd /opt/busan && python3 daily_pipeline_sync.py >> /opt/busan/pipeline.log 2>&1
    
    echo ""
    echo ">> 캐시 갱신..."
    cd /opt/busan && python3 build_api_cache.py
    
    echo ""
    echo ">> API 서버 재시작..."
    systemctl restart busan-api
    
    echo ""
    echo "=== 복구 완료! ==="
    echo "캐시 날짜 확인:"
    head -c 100 /opt/busan/api_cache.json
fi
