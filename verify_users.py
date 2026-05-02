"""챗봇 서비스 실패 원인 확인 + 모니터링 최종 검증"""
import paramiko

def run_cmd(client, cmd, label, timeout=15):
    print(f"\n  [{label}]")
    try:
        stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode('utf-8', errors='replace').strip()
        err = stderr.read().decode('utf-8', errors='replace').strip()
        if out: print(out)
        if err: print(f"  [STDERR] {err}")
        if not out and not err: print("  (OK)")
    except Exception as e:
        print(f"  [ERROR] {e}")

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 1. 챗봇 에러 로그
run_cmd(client, "journalctl -u law-chatbot --since '5 min ago' --no-pager | tail -20", "챗봇 에러 로그")

# 2. busan-chatbot 사용자로 직접 실행 테스트
run_cmd(client, "su -s /bin/bash busan-chatbot -c 'cd /opt/advisor && /usr/local/bin/streamlit --version' 2>&1", "챗봇 streamlit 접근 테스트")

# 3. 모니터링 시스템 최종 상태
print("\n" + "=" * 60)
print("  최종 상태 요약")
print("=" * 60)
run_cmd(client, """
echo "=== 사용자 ==="
id busan-monitor
id busan-chatbot

echo ""
echo "=== 모니터링 서비스 ==="
systemctl is-active busan-api && echo "  busan-api: OK"
systemctl is-active busan-dashboard && echo "  busan-dashboard: OK"

echo ""
echo "=== cron ==="
echo "[busan-monitor]"
crontab -u busan-monitor -l
echo ""
echo "[busan-chatbot]"
crontab -u busan-chatbot -l
echo ""
echo "[root]"
crontab -l

echo ""
echo "=== 교차 접근 차단 ==="
su -s /bin/bash busan-chatbot -c 'cat /opt/busan/alert_config.json 2>&1 | head -1' || echo "  ✅ 차단됨: busan-chatbot → alert_config.json 읽기 불가"
su -s /bin/bash busan-chatbot -c 'ls /opt/busan/*.db 2>&1 | head -1' || echo "  ✅ 차단됨: busan-chatbot → DB 접근 불가"
""", "최종 상태 요약")

client.close()
print("\n=== 검증 완료 ===")
