1. Generate a self-signed SSL certificate:

```bash
sudo mkdir -p /etc/nginx/ssl
sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/nginx/ssl/nginx.key -out /etc/nginx/ssl/nginx.crt
```

2. Create a password file for basic authentication:

```bash
sudo sh -c "echo -n 'prh:' >> /etc/nginx/.htpasswd"
sudo sh -c "openssl passwd -apr1 <PASSWORD_HERE> >> /etc/nginx/.htpasswd"
```

3. Create an Nginx configuration file:

```bash
sudo nano /etc/nginx/sites-available/prefect
```

4. Add the following configuration to the file:

```nginx
server {
    listen 443 ssl;
    server_name 5.78.119.241;

    ssl_certificate /etc/nginx/ssl/nginx.crt;
    ssl_certificate_key /etc/nginx/ssl/nginx.key;

    location ~ ^/(?:api)?$ {
        auth_basic "Restricted Access";
        auth_basic_user_file /etc/nginx/.htpasswd;

        proxy_pass http://127.0.0.1:4200;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # WebSocket support required in Prefect 3: https://github.com/PrefectHQ/prefect/issues/15274
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

5. Enable the site:

```bash
sudo ln -s /etc/nginx/sites-available/prefect /etc/nginx/sites-enabled/
```

6. Test the Nginx configuration:

```bash
sudo nginx -t
```

7. If the test is successful, restart Nginx:

```bash
sudo systemctl restart nginx
```

Now prefect should be accessible via HTTPS on port 443 with basic authentication.