import os
import pandas as pd
from typing import List, Dict

# adding libraries folder to the system path
from libs import (
    api as f_api,
)

# importing Gsheet API class
import gspread
from google.oauth2.service_account import Credentials


class GsheetAPI(f_api.API):
    SPREADSHEET_ID = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    RANGE = "Class Data!A2:E"
    SCOPES = scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

    def __init__(self, config):
        # configuration
        self.config = config["google"]
        self.site = self.config["site"]
        # self.site_id = self.SITES[self.site]["id"]
        # self.platform = self.config["platform"]

        # authentication
        creds = self.__authentication()

        # client
        client = gspread.authorize(creds)
        self.client = client.open(self.SPREADSHEET_ID)
        print(self.client.sheet1.get("A1"))

        # initialization constructor api
        file = self.config["__file__"]
        super().__init__(file)

    def __authentication(self):
        file_creds = self.config["credentials"]["path_service_account"]
        creds = Credentials.from_service_account_file(file_creds, scopes=self.SCOPES)
        return creds
