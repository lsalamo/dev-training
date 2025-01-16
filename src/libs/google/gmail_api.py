import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

import base64
from email.message import EmailMessage

# adding libraries folder to the system path
from libs import (
    api as f_api,
)

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


class GmailAPI(f_api.API):
    SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

    def __init__(self, file):
        super().__init__(file)

        # configuration
        self.config = self.load_config()

        # authentication
        creds = self.__authentication()

        # client
        self.email_from = self.config["email"]["email_from"]
        delegated_credentials = creds.with_subject(self.email_from)
        self.client = build("gmail", "v1", credentials=delegated_credentials)

    def __authentication(self):
        file_creds = self.config["google"]["credentials"]["path_service_account"]
        creds = Credentials.from_service_account_file(file_creds, scopes=self.SCOPES)
        return creds

    def send_email(self, email_to, subject, body, image_paths):
        message = EmailMessage()
        message["From"] = self.email_from
        message["To"] = email_to
        message["Subject"] = subject
        message.set_content(body)

        # add images as attachments
        if image_paths:
            for image_path in image_paths:
                with open(image_path, "rb") as img:
                    image_data = img.read()
                    image_name = image_path.split("/")[-1]
                    message.add_attachment(image_data, maintype="image", subtype="jpeg", filename=image_name)

        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {"raw": encoded_message}
        # pylint: disable=E1101
        send_message = self.client.users().messages().send(userId="me", body=create_message).execute()
        print(f'Message Id: {send_message["id"]}')

        # html = f"<html><body>{body}"
        # for i, image_path in enumerate(image_paths):
        #     html += f'<p><img src="cid:image{i}"></p>'
        # html += "</body></html>"

        # message.attach(MIMEText(html, "html"))

        # for i, image_path in enumerate(image_paths):
        #     with open(image_path, "rb") as img:
        #         msg_img = MIMEImage(img.read())
        #         msg_img.add_header("Content-ID", f"<image{i}>")
        #         message.attach(msg_img)
