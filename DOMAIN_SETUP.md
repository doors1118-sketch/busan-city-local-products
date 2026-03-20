# busanproduct.co.kr 도메인 설정 가이드

> 생성일: 2026-03-20

## 1단계: 가비아 DNS 설정 (사무실 PC에서 가능)

1. https://www.gabia.com 로그인
2. **My가비아** → **서비스 관리** → **도메인** 클릭
3. `busanproduct.co.kr` 옆 **관리** 클릭
4. **DNS 설정** (또는 **DNS 레코드 관리**) 클릭
5. 아래 2개 레코드 추가:

| 호스트 | 타입 | 값 | TTL |
|--------|------|-----|-----|
| `@` | A | `49.50.133.160` | 3600 |
| `www` | A | `49.50.133.160` | 3600 |

6. **저장** 클릭

> ⏳ DNS 반영까지 최대 1시간 소요 (보통 5~10분)

---

## 2단계: 서버 설정 (NCP 웹 콘솔에서 실행)

### NCP 방화벽 (ACG) 설정
NCP 콘솔 → 서버 → ACG → **80, 443 포트 인바운드 허용** 추가:

| 프로토콜 | 포트 | 접근소스 |
|---------|------|---------|
| TCP | 80 | 0.0.0.0/0 |
| TCP | 443 | 0.0.0.0/0 |

### 서버에서 아래 명령어 순서대로 실행

```bash
# 1. Nginx 설치
sudo apt update && sudo apt install nginx -y

# 2. Nginx 설정 파일 생성
sudo tee /etc/nginx/sites-available/busan-api << 'EOF'
server {
    listen 80;
    server_name busanproduct.co.kr www.busanproduct.co.kr;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
        proxy_connect_timeout 75s;
    }
}
EOF

# 3. 사이트 활성화 + default 비활성화
sudo ln -sf /etc/nginx/sites-available/busan-api /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default

# 4. 설정 검증 + Nginx 시작
sudo nginx -t && sudo systemctl restart nginx && sudo systemctl enable nginx

# 5. SSL 인증서 설치 (HTTPS)
sudo apt install certbot python3-certbot-nginx -y
sudo certbot --nginx -d busanproduct.co.kr -d www.busanproduct.co.kr --non-interactive --agree-tos --email doors1118@gmail.com

# 6. SSL 자동 갱신 확인
sudo certbot renew --dry-run
```

---

## 3단계: 접속 확인

설정 완료 후 아래 URL로 접속 테스트:
- http://busanproduct.co.kr → 자동으로 https로 리다이렉트
- https://busanproduct.co.kr → API 서버 응답
- https://busanproduct.co.kr/docs → Swagger 문서

---

## 참고: 기존 IP 접속도 유지

기존 `http://49.50.133.160:8000` 접속도 그대로 유지됩니다.
대시보드 업체에 새 도메인 URL 전달 후 안정화되면 8000 포트 비활성화 가능.

---

## 문제 해결

| 증상 | 원인 | 해결 |
|------|------|------|
| 도메인 접속 안됨 | DNS 미반영 | `nslookup busanproduct.co.kr` 확인, 최대 1시간 대기 |
| 502 Bad Gateway | FastAPI 미실행 | `sudo systemctl status busan-api` 확인 |
| SSL 오류 | 인증서 미발급 | DNS 반영 후 certbot 재실행 |
| NCP 접속 안됨 | ACG 방화벽 | 80/443 포트 열었는지 확인 |
