#!/bin/bash
# busanproduct.co.kr Nginx + SSL setup script

# 1. Create Nginx reverse proxy config
cat > /etc/nginx/sites-available/busan-api << 'CONF'
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
CONF

# 2. Enable site + remove default
ln -sf /etc/nginx/sites-available/busan-api /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default

# 3. Test and restart Nginx
nginx -t && systemctl restart nginx
echo "=== Nginx configured ==="

# 4. Install certbot and get SSL certificate
apt install certbot python3-certbot-nginx -y
certbot --nginx -d busanproduct.co.kr -d www.busanproduct.co.kr \
    --non-interactive --agree-tos --email doors1118@gmail.com

# 5. Verify
echo "=== Setup complete ==="
systemctl status nginx --no-pager
echo ""
echo "Test: curl -I http://busanproduct.co.kr"
curl -sI http://busanproduct.co.kr | head -5
