import pandas as pd

# adding libraries folder to the system path
from libs import (
    api as f_api,
    datetime_formatter as f_datetime_formatter,
)

# importing Gsheet API class
import gspread  # https://docs.gspread.org/en/latest/user-guide.html
from google.oauth2.service_account import Credentials


class GsheetAPI(f_api.API):
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

    def __init__(self, file: str):
        super().__init__(file)

        # configuration
        self.config = self.load_config()["google"]

        # authentication
        creds = self.__authentication()

        # client
        self.client = gspread.authorize(creds)

    def __authentication(self):
        file_creds = self.config["credentials"]["path_service_account"]
        creds = Credentials.from_service_account_file(file_creds, scopes=self.SCOPES)
        return creds

    def delete_and_create_spreadsheet(self, title, locale="es_ES"):
        try:
            # Try to open the spreadsheet by title
            spreadsheet = self.open_spreadsheet_bytitle(title=title)
            # If found, delete the spreadsheet
            self.delete_spreadsheet(spreadsheet)
        except gspread.SpreadsheetNotFound:
            pass
        # create a new spreadsheet
        return self.create_spreadsheet(title=title)

    def create_spreadsheet(self, title: str, locale="es_ES") -> gspread.Spreadsheet:
        spreadsheet = self.client.create(title=title)
        spreadsheet.update_locale(locale)
        return spreadsheet

    def open_spreadsheet_bytitle(self, title: str) -> gspread.Spreadsheet:
        return self.client.open(title=title)

    def open_spreadsheet_bykey(self, key: str):
        return self.client.open_by_key(key == key)

    def share_spreadsheet(self, spreadsheet: gspread.Spreadsheet, email):
        spreadsheet.share(email, perm_type="user", role="writer")

    def delete_spreadsheet(self, spreadsheet: gspread.Spreadsheet):
        self.client.del_spreadsheet(spreadsheet.id)

    def delete_all_tabs(self, spreadsheet: gspread.Spreadsheet):
        worksheets = spreadsheet.worksheets()
        for worksheet in worksheets:
            spreadsheet.del_worksheet(worksheet)

    def update_worksheet(self, spreadsheet: gspread.Spreadsheet, df: pd.DataFrame, title: str, color_code: str):
        # Delete existing worksheet and create a new one
        worksheet = spreadsheet.add_worksheet(title=title.upper(), rows=df.shape[0], cols=df.shape[1])

        # Set tab color to red
        worksheet.update_tab_color(color_code)

        # Save data to the sheet
        df["date"] = f_datetime_formatter.DatetimeFormatter.datetime_to_str(df["date"].dt, "%Y-%m-%d")
        headers = df.columns.values.tolist()
        data = df.values.tolist()
        rows_to_append = [headers] + data
        worksheet.append_rows(rows_to_append, value_input_option="RAW")

        # Make header row bold
        header_range = f"A1:{chr(65 + len(headers) - 1)}1"
        worksheet.format(header_range, {"textFormat": {"bold": True}})

        # Format range B2:G9 as integers with a dot
        worksheet.format("B2:G9", {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})

        # Delete worksheet named 'sheet1' if it exists
        try:
            spreadsheet.del_worksheet(spreadsheet.sheet1)
        except gspread.WorksheetNotFound:
            pass

        self.log.print("GsheetAPI.update_worksheet", f"url: {worksheet.url}")
        self.log.print("GsheetAPI.update_worksheet", "completed")
