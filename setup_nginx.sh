#!/bin/bash
# busanproduct.co.kr Nginx setup script
# Root(/) → Streamlit dashboard (8501)
# /api/* → FastAPI (8000)

# 1. Create Nginx reverse proxy config
cat > /etc/nginx/sites-available/busan-api << 'CONF'
server {
    listen 80;
    server_name busanproduct.co.kr www.busanproduct.co.kr;

    # Streamlit dashboard (root)
    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }

    # Streamlit websocket
    location /_stcore/stream {
        proxy_pass http://127.0.0.1:8501/_stcore/stream;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
    }

    # FastAPI endpoints
    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # FastAPI docs
    location /docs {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
    }
    location /openapi.json {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
    }
}
CONF

# 2. Enable site
ln -sf /etc/nginx/sites-available/busan-api /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default

# 3. Test and restart Nginx
nginx -t && systemctl restart nginx
echo "=== Nginx configured ==="

# 4. Re-run certbot for SSL (will update existing cert config)
certbot --nginx -d busanproduct.co.kr --non-interactive --agree-tos --email doors1118@gmail.com 2>/dev/null || echo "SSL already configured or certbot skipped"

# 5. Restart nginx after certbot
systemctl restart nginx

echo "=== Setup complete ==="
echo "busanproduct.co.kr → Streamlit dashboard (8501)"
echo "busanproduct.co.kr/api/* → FastAPI (8000)"
systemctl status nginx --no-pager | head -5
