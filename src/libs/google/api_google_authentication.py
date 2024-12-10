import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2 import service_account
from googleapiclient.discovery import build

class GoogleAuthentication:
    @staticmethod
    def service_account(file_credentials):
        # export GOOGLE_APPLICATION_CREDENTIALS = / path / to / credentials.json
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = file_credentials   

    @staticmethod
    def oauth2(file_credentials):
        TOKEN_FILE = 'token.json'
        SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
        creds = None
        # Check if token file exists
        if os.path.exists(TOKEN_FILE):
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        # If credentials are invalid or missing, run the OAuth flow
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(file_credentials, SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for future use
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())       
        return creds    