import pandas as pd
from typing import List, Dict, Union

from libs import (
    api as f_api,
)

import gspread
from google.oauth2.service_account import Credentials


class GspreadAPI(f_api.API):
    """
    A class to interact with Google Sheets using the gspread library.

    Documentation:
        - gspread: https://docs.gspread.org/en/latest/index.html
        - batchUpdate: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/batchUpdate
        - Cells Format: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellFormat
    """

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    FOLDER_ID_SHARED_SERVICE_ACCOUNT = "1lPvmkAGl_u9zFcrXWZSTtag4vsaFUTDq"

    def __init__(self, config: Dict[str, str]):
        super().__init__()

        # configuration
        self.config = config

        # properties
        self._spreadsheet = gspread.Spreadsheet
        self._worksheets = []
        self._last_active_worksheet: Union[gspread.worksheet.Worksheet, None] = None

        # authentication
        creds = self.__authentication()

        # client gsheet
        self.client: gspread.client.Client = gspread.authorize(creds)

    @property
    def spreadsheet(self) -> gspread.Spreadsheet:
        return self._spreadsheet

    @property
    def worksheets(self) -> List[gspread.Worksheet]:
        return self._worksheets

    @property
    def last_active_worksheet(self) -> Union[gspread.Worksheet, None]:
        return self._last_active_worksheet

    def __authentication(self):
        file_creds = self.config["google"]["credentials"]["path_service_account"]
        creds = Credentials.from_service_account_file(file_creds, scopes=self.SCOPES)
        return creds

    # def create_spreadsheet(self, title: str, locale: str = "es_ES") -> gspread.Spreadsheet:
    def create_spreadsheet(self, title: str, locale: str = "es_ES"):
        try:
            spreadsheet = self.client.create(title=title, folder_id=self.FOLDER_ID_SHARED_SERVICE_ACCOUNT)
            spreadsheet.update_locale(locale)
            self._spreadsheet = spreadsheet
            return spreadsheet
        except Exception as error:
            print(f"An error occurred: {error}")
        return error

    def open_spreadsheet_bytitle(self, title: str) -> gspread.Spreadsheet:
        try:
            return self.client.open(title=title)
        except gspread.SpreadsheetNotFound:
            raise Exception(f"Spreadsheet '{title}' not found")

    def open_spreadsheet_bykey(self, key: str) -> gspread.Spreadsheet:
        try:
            return self.client.open_by_key(key == key)
        except gspread.SpreadsheetNotFound:
            raise Exception(f"Spreadsheet with key '{key}' not found")

    def share_spreadsheet(self, spreadsheet: gspread.Spreadsheet, email):
        spreadsheet.share(email, perm_type="user", role="writer")

    def delete_spreadsheet(self, spreadsheet: gspread.Spreadsheet):
        self.client.del_spreadsheet(spreadsheet.id)

    def delete_worksheet(self, spreadsheet: gspread.Spreadsheet, worksheet: gspread.Worksheet):
        try:
            spreadsheet.del_worksheet(worksheet)
        except gspread.WorksheetNotFound:
            pass

    def delete_worksheet_by_title(self, spreadsheet: gspread.Spreadsheet, title: str):
        try:
            spreadsheet.del_worksheet(spreadsheet.worksheet(title))
        except gspread.WorksheetNotFound:
            pass

    def delete_worksheet_sheet1(self, spreadsheet: gspread.Spreadsheet):
        try:
            if self._spreadsheet.sheet1.title == "Sheet1":
                spreadsheet.del_worksheet(spreadsheet.sheet1)
        except gspread.WorksheetNotFound:
            pass

    def delete_all_tabs(self, spreadsheet: gspread.Spreadsheet):
        worksheets = spreadsheet.worksheets()
        for worksheet in worksheets:
            spreadsheet.del_worksheet(worksheet)

    def add_worksheet(self, spreadsheet: gspread.Spreadsheet, df: pd.DataFrame, title: str, tab_color: str = None):
        # add worksheet
        worksheet = spreadsheet.add_worksheet(title=title.upper(), rows=df.shape[0], cols=df.shape[1])

        # Set tab color to red
        worksheet.update_tab_color(tab_color)

        # Save data to the sheet
        headers = df.columns.values.tolist()
        data = df.values.tolist()
        rows_to_append = [headers] + data
        worksheet.append_rows(rows_to_append, value_input_option="RAW")

        # Add worksheet to the list and update the last active worksheet
        self._worksheets.append(worksheet)
        self._last_active_worksheet = worksheet

        # Delete worksheet named 'sheet1'
        self.delete_worksheet_sheet1(self._spreadsheet)

    def set_worksheet_formatting(
        self, worksheet: gspread.Worksheet, ranges: Union[List[str], str], format: gspread.worksheet.JSONResponse
    ):
        # Make header row bold and set background color
        worksheet.format(ranges=ranges, format=format)

    def update_cells(self, worksheet: gspread.Worksheet, row: int, col: int, value: Union[int, float, str]):
        worksheet.update_cell(row, col, value)

    def format_worksheet(
        self,
        range: str,
        bg_color: Dict[str, Union[int, float]] = None,
        fontsize: int = None,
        bold: bool = None,
        number_pattern: str = None,
    ):
        fields = []
        if bg_color is not None:
            fields.append("backgroundColor")
        if fontsize is not None or bold is not None:
            fields.append("textFormat")
        if number_pattern is not None:
            fields.append("numberFormat")
        fields_str = f"userEnteredFormat({','.join(fields)})"

        background_color = {}
        text_format = {}
        number_format = {}
        if bg_color is not None:
            background_color = bg_color
        if fontsize is not None:
            text_format["fontSize"] = fontsize
        if bold is not None:
            text_format["bold"] = bold
        if number_pattern is not None:
            number_format["type"] = "NUMBER"
            number_format["pattern"] = number_pattern
        return {
            "repeatCell": {
                "range": self._parse_range(range),
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": background_color,
                        "textFormat": text_format,
                        "numberFormat": number_format,
                    }
                },
                "fields": fields_str,
            }
        }

    def format_worksheet_borders(
        self,
        range: str,
        style: str = "SOLID",
        width: int = 1,
        color: Dict[str, Union[int, float]] = {"red": 0, "green": 0, "blue": 0},
    ):
        border = {"style": style, "width": width, "color": color}
        return {
            "updateBorders": {
                "range": self._parse_range(range),
                "top": border,
                "bottom": border,
                "left": border,
                "right": border,
            }
        }

    def format_worksheet_frozen(
        self,
        rows: int = None,
        cols: int = None,
    ):
        fields = []
        if rows is not None:
            fields.append("frozenRowCount")
        if cols is not None:
            fields.append("frozenColumnCount")
        fields_str = f"gridProperties({','.join(fields)})"

        grid_properties = {}
        if rows is not None:
            grid_properties["frozenRowCount"] = rows
        if cols is not None:
            grid_properties["frozenColumnCount"] = cols
        return {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": self.last_active_worksheet.id,
                    "gridProperties": grid_properties,
                },
                "fields": fields_str,
            }
        }

    def _parse_range(self, range_str: str):
        start_cell, end_cell = range_str.split(":")
        start_row = int("".join(filter(str.isdigit, start_cell)))
        start_col = ord(start_cell[0].upper()) - ord("A")
        end_row = int("".join(filter(str.isdigit, end_cell)))
        end_col = ord(end_cell[0].upper()) - ord("A")

        return {
            "sheetId": self.last_active_worksheet.id,
            "startRowIndex": start_row - 1,
            "endRowIndex": end_row,
            "startColumnIndex": start_col,
            "endColumnIndex": end_col + 1,
        }
