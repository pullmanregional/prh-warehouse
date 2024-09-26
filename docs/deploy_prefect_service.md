This setup will run Prefect as a systemd service under the `deploy` user, automatically restart it if it crashes, and send you email notifications if there are any issues. The check script runs every 5 minutes to ensure Prefect is running properly.


0. Make sure that python is installed via pyenv, and prefect is installed via pip.

1. Create a systemd service file for Prefect:

```bash
sudo vim /etc/systemd/system/prefect.service
```

2. Add the following content to the file:

```ini
[Unit]
Description=Prefect Server
After=network.target

[Service]
Type=simple
User=deploy
ExecStart=/home/deploy/.pyenv/shims/prefect server start --host 0.0.0.0
Restart=always
RestartSec=1

[Install]
WantedBy=multi-user.target

```

4. Reload the systemd manager to read the new service file:

```bash
sudo systemctl daemon-reload
```

5. Start the Prefect service:

```bash
sudo systemctl start prefect
```

6. Enable the service to start on boot:

```bash
sudo systemctl enable prefect
```

7. Check the status of the service:

```bash
sudo systemctl status prefect
```

8. Mail notifications. Install necessary packages:

```bash
sudo apt-get update
sudo apt-get install -y mailutils
```

9. Create a script to check Prefect status and send notifications:

```bash
nano /home/deploy/check_prefect.sh
```

10. Add the following content to the script:

```bash
#!/bin/bash

# File to store the previous state
STATE_FILE="/home/deploy/.prefect_state"

# Check Prefect service status
CURRENT_STATUS=$(systemctl is-active prefect)

# Read the previous state
if [ -f "$STATE_FILE" ]; then
    PREVIOUS_STATUS=$(cat "$STATE_FILE")
else
    PREVIOUS_STATUS="unknown"
fi

# Function to send email
send_email() {
    echo "Prefect service status is $CURRENT_STATUS" | mail -s "Prefect Service Alert" jonjlee@gmail.com
}

# Check if status changed from active to inactive
if [ "$CURRENT_STATUS" != "active" ] && [ "$PREVIOUS_STATUS" == "active" ]; then
    send_email
    sudo systemctl restart prefect
fi

# Save the current status
echo "$CURRENT_STATUS" > "$STATE_FILE"
```

11. Make the script executable:

```bash
chmod +x /home/deploy/check_prefect.sh
```

12. Set up a cron job to run the script every 5 minutes:

```bash
(crontab -l 2>/dev/null; echo "*/5 * * * * /home/deploy/check_prefect.sh") | crontab -
```

13. Configure the system's email settings. For a simple setup using Gmail, you can use `ssmtp`. This doesn't require a local SMTP server process:

```bash
sudo apt-get install -y ssmtp
sudo nano /etc/ssmtp/ssmtp.conf
```

14. Add the following to the `ssmtp.conf` file (replace with your actual Gmail credentials):

```
root=jlee.peds@gmail.com
mailhub=smtp.gmail.com:587
AuthUser=jlee.peds@gmail.com
AuthPass=your_app_password
UseSTARTTLS=YES
FromLineOverride=YES
```

Note: For security, use an app-specific password instead of your main Gmail password. To set this up, 2FA must be enabled. Then go to https://myaccount.google.com/u/1/apppasswords.

15. Set appropriate permissions for the ssmtp configuration:

```bash
sudo chmod 640 /etc/ssmtp/ssmtp.conf
```

16. Test the notification system:

```bash
echo "Test email" | mail -s "Test Subject" jonjlee@gmail.com
```

Here are all the commands:

```bash
sudo nano /etc/systemd/system/prefect.service
# (Add content from the prefect-systemd-service artifact)
sudo systemctl daemon-reload
sudo systemctl start prefect
sudo systemctl enable prefect
sudo systemctl status prefect
sudo apt-get update
sudo apt-get install -y mailutils
nano /home/deploy/check_prefect.sh
# (Add content from the prefect-check-script artifact)
chmod +x /home/deploy/check_prefect.sh
(crontab -l 2>/dev/null; echo "*/5 * * * * /home/deploy/check_prefect.sh") | crontab -
sudo apt-get install -y ssmtp
sudo nano /etc/ssmtp/ssmtp.conf
# (Add email configuration)
sudo chmod 640 /etc/ssmtp/ssmtp.conf
echo "Test email" | mail -s "Test Subject" your_email@example.com
```

