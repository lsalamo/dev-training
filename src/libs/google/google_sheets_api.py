import pandas as pd
from typing import List, Dict, Union

from libs import (
    api as f_api,
)

from libs.google import google_drive_api as gdrive_api

from googleapiclient.discovery import Resource
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials


class GoogleSheetsAPI(f_api.API):
    """
    A class to interact with Google Sheets API.

    Documentation:
        - URL: https://developers.google.com/sheets/api/reference/rest
        - batchUpdate: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/batchUpdate
        - Request: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request
        - Cells Format: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellFormat
    """

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    FOLDER_ID_SHARED_SERVICE_ACCOUNT = "1lPvmkAGl_u9zFcrXWZSTtag4vsaFUTDq"
    SHEET1_ID = 0

    def __init__(self, config: Dict[str, str]):
        super().__init__()

        # configuration
        self.config = config

        # properties
        self._spreadsheet: Dict[str, str] = {}
        self._sheets: List[Dict[str, Union[str, int]]] = []

        # authentication
        creds = self.__authentication()

        # Initialize the Drive API client
        self.client: Resource = build("sheets", "v4", credentials=creds)

    @property
    def spreadsheet(self) -> Dict[str, str]:
        return self._spreadsheet

    @property
    def sheets(self) -> List[Dict[str, Union[str, int]]]:
        return self._sheets

    def __authentication(self):
        file_creds = self.config["google"]["credentials"]["path_service_account"]
        creds = Credentials.from_service_account_file(file_creds, scopes=self.SCOPES)
        return creds

    def create_spreadsheet(self, title: str, locale: str = "es_ES"):
        try:
            body = {"properties": {"title": title, "locale": locale}}
            spreadsheet = (
                self.client.spreadsheets()
                .create(body=body, fields="spreadsheetId,properties,sheets,spreadsheetUrl")
                .execute()
            )
            self._set_spreadsheet(spreadsheet)

            # Move the spreadsheet to the shared folder
            gdrive = gdrive_api.GoogleDriveAPI(config=self.config, creds=self.client._http.credentials)
            gdrive.move_file_to_folder(
                file_id=self._spreadsheet["id"], folder_id=[self.FOLDER_ID_SHARED_SERVICE_ACCOUNT], parents=["root"]
            )
        except HttpError as error:
            self.log.print_error("GoogleSheetAPI:create_spreadsheet", f"An error occurred: {str(error)}")

    def get_spreadsheet_by_title(self, title: str):
        try:
            spreadsheet = self.client.spreadsheets().get(title=title).execute()
            self._set_spreadsheet(spreadsheet)
        except HttpError as error:
            print(f"An error occurred: {error}")

    def get_spreadsheet_by_id(self, spreadsheet_id: str):
        try:
            spreadsheet = self.client.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            self._set_spreadsheet(spreadsheet)
        except HttpError as error:
            print(f"An error occurred: {error}")

    def _set_spreadsheet(self, spreadsheet):
        self._spreadsheet["id"] = spreadsheet.get("spreadsheetId")
        self._spreadsheet["url"] = spreadsheet.get("spreadsheetUrl")
        self._spreadsheet["title"] = spreadsheet.get("properties", {}).get("title", "")
        for sheet in spreadsheet.get("sheets", []):
            new_sheet = {
                "id": sheet.get("properties", {}).get("sheetId", ""),
                "title": sheet.get("properties", {}).get("title", ""),
                "index": sheet.get("properties", {}).get("index", 0),
            }
            self._sheets.append(new_sheet)

    def get_sheet_by_title(self, spreadsheet_id: str, title: str):
        sheet_metadata = self.get_spreadsheet_by_id(spreadsheet_id)
        for sheet in sheet_metadata.get("sheets", []):
            if sheet.get("properties", {}).get("title", "") == title:
                return sheet.get("properties", {})
        return None

    def _update_values(self, spreadsheet_id: str, range: str, df: pd.DataFrame):
        values = [df.columns.values.tolist()] + df.values.tolist()
        data = {"range": range, "majorDimension": "ROWS", "values": values}
        self.client.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range, valueInputOption="RAW", body=data
        ).execute()

    def share_spreadsheet(self, spreadsheet_id: str, email: str):
        self.client.permissions().create(
            fileId=spreadsheet_id,
            body={"type": "user", "role": "writer", "emailAddress": email},
            fields="id",
        ).execute()

    def delete_spreadsheet(self, spreadsheet_id: str):
        self.client.spreadsheets().delete(spreadsheetId=spreadsheet_id).execute()

    def delete_sheet_by_title(self, spreadsheet_id: str, title: str):
        for sheet in self._sheets:
            if sheet["title"] == title:
                self.client.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": [{"deleteSheet": {"sheetId": sheet["id"]}}]},
                ).execute()
                self._sheets.remove(sheet)
                print(f"Sheet '{title}' deleted successfully.")
                return

    def delete_sheet1(self, spreadsheet_id: str):
        for sheet in self._sheets:
            if sheet["title"] in ["Sheet1", "Hoja 1"]:
                self.client.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": [{"deleteSheet": {"sheetId": sheet["id"]}}]},
                ).execute()
                self._sheets.remove(sheet)
                self.log.print("GoogleSheetAPI:delete_sheet1", f"Sheet 'Sheet1' deleted successfully.")
                return

    def add_sheet(self, spreadsheet_id: str, df: pd.DataFrame, title: str, tab_color: str = None):
        try:
            requests = [
                {
                    "addSheet": {
                        "properties": {
                            "title": title.upper(),
                            "gridProperties": {
                                "rowCount": df.shape[0] + 1,  # +1 for header row
                                "columnCount": df.shape[1],
                            },
                            "tabColor": self.format_color_hex_to_rgb(tab_color) if tab_color else None,
                        }
                    }
                }
            ]

            # add sheet
            response = self.batch_update(spreadsheet_id=spreadsheet_id, requests=requests)
            response = response.get("replies")[0].get("addSheet", {}).get("properties", {})
            new_sheet = {
                "id": response.get("sheetId", ""),
                "title": response.get("title", ""),
                "index": response.get("index", ""),
            }
            self._sheets.append(new_sheet)

            # Update the worksheet with the DataFrame data
            self._update_values(spreadsheet_id, f"{title.upper()}!A1", df)

            self.log.print("GoogleSheetAPI:add_sheet", f"Sheet '{title.upper()}' added successfully.")
            return new_sheet.get("id", "")
        except Exception as e:
            self.log.print_error("GoogleSheetAPI:add_sheet", f"An error occurred: {str(e)}")

    def format_color_hex_to_rgb(self, hex_color: str):
        hex_color = hex_color.lstrip("#")
        return {
            "red": int(hex_color[0:2], 16) / 255.0,
            "green": int(hex_color[2:4], 16) / 255.0,
            "blue": int(hex_color[4:6], 16) / 255.0,
        }

    def format_worksheet(
        self,
        sheet_id: str,
        range: str,
        bg_color: str = None,
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
            background_color = self.format_color_hex_to_rgb(bg_color) if bg_color else None
        if fontsize is not None:
            text_format["fontSize"] = fontsize
        if bold is not None:
            text_format["bold"] = bold
        if number_pattern is not None:
            number_format["type"] = "NUMBER"
            number_format["pattern"] = number_pattern
        return {
            "repeatCell": {
                "range": self._parse_range(sheet_id, range),
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
        sheet_id: str,
        range: str,
        style: str = "SOLID",
        width: int = 1,
        color: Dict[str, Union[int, float]] = {"red": 0, "green": 0, "blue": 0},
    ):
        border = {"style": style, "width": width, "color": color}
        return {
            "updateBorders": {
                "range": self._parse_range(sheet_id, range),
                "top": border,
                "bottom": border,
                "left": border,
                "right": border,
            }
        }

    def format_worksheet_frozen(
        self,
        sheet_id: str,
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
                    "sheetId": sheet_id,
                    "gridProperties": grid_properties,
                },
                "fields": fields_str,
            }
        }

    def _parse_range(self, sheet_id: str, range_str: str):
        start_cell, end_cell = range_str.split(":")
        start_row = int("".join(filter(str.isdigit, start_cell)))
        start_col = ord(start_cell[0].upper()) - ord("A")
        end_row = int("".join(filter(str.isdigit, end_cell)))
        end_col = ord(end_cell[0].upper()) - ord("A")

        return {
            "sheetId": sheet_id,
            "startRowIndex": start_row - 1,
            "endRowIndex": end_row,
            "startColumnIndex": start_col,
            "endColumnIndex": end_col + 1,
        }

    def batch_update(self, spreadsheet_id: str, requests: Dict):
        body = {"requests": requests}
        return self.client.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
