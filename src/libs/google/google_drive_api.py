# API REST GOOGLE : https://developers.google.com/drive/api/reference/rest/v3
import pandas as pd
from typing import List, Dict, Union

from libs import (
    api as f_api,
)

from googleapiclient.discovery import Resource
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials


class GoogleDriveAPI(f_api.API):
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    FOLDER_ID_SHARED_SERVICE_ACCOUNT = "1qdctMBegkhxKjFgjeRBLiHhRVnDhKF7f"
    QUERY_FILES = "mimeType!='application/vnd.google-apps.folder' and trashed=false"
    QUERY_FOLDERS = "mimeType='application/vnd.google-apps.folder' and trashed=false"
    QUERY_FILES_IN_MY_DRIVE = "'root' in parents and trashed=false"
    QUERY_FILES_IN_FOLDER = "'{folder_id}' in parents and trashed=false"
    QUERY_FILES_WITH_NAME = "name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"

    def __init__(self, config: Dict[str, str], creds: Credentials = None):
        super().__init__()

        # configuration
        self.config = config

        # authentication
        if creds is None:
            creds = self.__authentication()

        # client google drive
        self.client: Resource = build("drive", "v3", credentials=creds)

    def __authentication(self):
        file_creds = self.config["google"]["credentials"]["path_service_account"]
        creds = Credentials.from_service_account_file(file_creds, scopes=self.SCOPES)
        return creds

    def get_file(self, file_id):
        try:
            file = self.client.files().get(fileId=file_id).execute()
            return file
        except HttpError as error:
            print(f"An error occurred while getting the file: {error}")
            return None

    def get_file_parent(self, file_id):
        try:
            file = self.client.files().get(fileId=file_id, fields="parents").execute()
            return file.get("parents")
        except HttpError as error:
            print(f"An error occurred while getting the file: {error}")
            return None

    def list_files(self) -> List[Dict[str, str]]:
        return self._list_files(query=self.QUERY_FILES.format(folder_id=folder_id))

    def list_folders(self) -> List[Dict[str, str]]:
        return self._list_files(query=self.QUERY_FOLDERS.format(folder_id=folder_id))

    def list_files_with_name(self, name: str) -> List[Dict[str, str]]:
        return self._list_files(query=self.QUERY_FILES_WITH_NAME.format(name=name))

    def list_files_in_my_drive(self) -> List[Dict[str, str]]:
        return self._list_files(query=self.QUERY_FILES_IN_MY_DRIVE)

    def list_files_in_folder(self, folder_id: str) -> List[Dict[str, str]]:
        return self._list_files(query=self.QUERY_FILES_IN_FOLDER.format(folder_id=folder_id))

    def _list_files(self, query: str) -> List[Dict[str, str]]:
        try:
            results = self.client.files().list(q=query, fields="files(id, name, mimeType)").execute()
            items = results.get("files", [])
            if not items:
                print("No files or folders found.")
            else:
                for item in items:
                    print(f"Name: {item['name']} - ID: {item['id']} - Type: {item['mimeType']}")
            return items
        except HttpError as error:
            print(f"An error occurred while listing files: {error}")
            return []

    def del_file(self, file_id: str):
        try:
            self.client.files().delete(fileId=file_id).execute()
            print(f"File with id {file_id} has been deleted.")
        except HttpError as error:
            print(f"An error occurred while deleting the file: {error}")

    def del_all_files_from_folder(self, folder_id: str):
        try:
            # Query to list all files in the specified folder
            query = f"'{folder_id}' in parents and trashed=false"
            results = self.client.files().list(q=query, fields="files(id, name)").execute()
            items = results.get("files", [])

            if not items:
                print(f"No files found in the folder with id: {folder_id}")
            else:
                for item in items:
                    file_id = item["id"]
                    self.del_file(file_id)
        except HttpError as error:
            print(f"An error occurred while listing or deleting files: {error}")

    def move_file_to_folder(self, file_id: str, folder_id: List[str], parents: List[str]):
        try:
            parents = ",".join(parents)
            folder_id = ",".join(folder_id)
            results = (
                self.client.files()
                .update(fileId=file_id, addParents=folder_id, removeParents=parents, fields="id, parents")
                .execute()
            )
            return results
        except HttpError as error:
            print(f"An error occurred while updating the folder: {error}")
            return None
