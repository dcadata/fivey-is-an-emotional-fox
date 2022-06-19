from email.mime.text import MIMEText
from os import environ
from smtplib import SMTP_SSL


def send_email(subject: str, body: str) -> None:
    sender = environ['EMAIL_SENDER']
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = environ['EMAIL_RECIPIENT']

    server = SMTP_SSL(host='smtp.gmail.com', port=465)
    server.login(sender, environ['EMAIL_PASSWORD'])
    server.send_message(msg)
    server.quit()
