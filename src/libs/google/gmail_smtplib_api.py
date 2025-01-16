# Create password application: https://myaccount.google.com/apppasswords
from typing import Dict
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

# adding libraries folder to the system path
from libs import (
    api as f_api,
)

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


class GmailSmtplibAPI(f_api.API):
    SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
    HOST_GMAIL = "smtp.gmail.com"
    PORT_GMAIL = 587

    def __init__(self, config: Dict[str, str]):
        super().__init__()

        # configuration
        self.config = config

    def send_email(self, email_to, subject, image_paths, html_body):
        config = self.config["email"]
        message = MIMEMultipart()
        message["From"] = config["email_from"]
        message["To"] = email_to
        message["Subject"] = subject

        # Attach the body with the msg instance
        # message.attach(MIMEText(body, "plain"))
        # message.attach(MIMEText(body, "html"))

        # Attach images and update HTML body to reference the images
        if image_paths:
            for i, image_path in enumerate(image_paths):
                with open(image_path, "rb") as img:
                    img = MIMEImage(img.read())
                    img.add_header("Content-ID", f"<image{i}>")
                    message.attach(img)

        # Load file with the body content
        message.attach(MIMEText(html_body, "html"))

        # Create server
        server = smtplib.SMTP(self.HOST_GMAIL, self.PORT_GMAIL)
        server.starttls()

        # Login Credentials for sending the mail
        server.login(user=config["email_from"], password=config["password"])

        # Send the email
        # server.sendmail(self.email_from, email_to, message.as_string())
        server.send_message(msg=message)

        # Disconnect from the server
        server.quit()
