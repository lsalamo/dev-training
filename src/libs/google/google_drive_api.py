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
    """
    A class to interact with Google Drive API.

    Documentation:
        - URL: https://developers.google.com/drive/api/reference/rest
        - Search Files and Folders: https://developers.google.com/drive/api/guides/search-files
    """

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    FOLDER_ID_ROOT = "root"
    FOLDER_ID_SHARED_SERVICE_ACCOUNT = "1lPvmkAGl_u9zFcrXWZSTtag4vsaFUTDq"
    QUERY_FILES = "mimeType!='application/vnd.google-apps.folder' and trashed=false"
    QUERY_FOLDERS = "mimeType='application/vnd.google-apps.folder' and trashed=false"
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

    def get_file(self, file_id: str):
        try:
            file = self.client.files().get(fileId=file_id).execute()
            return file
        except HttpError as error:
            print(f"An error occurred while getting the file: {error}")
            return None

    def get_file_parents(self, parents: List[str]):
        try:
            return self.client.files().get(fileId=parents[0]).execute()
        except HttpError as error:
            print(f"An error occurred while getting the file: {error}")
            return None

    def list_type_files(self, folder_id: str = FOLDER_ID_ROOT, add_parent_name: bool = False) -> List[Dict[str, str]]:
        return self._list_files(query=self.QUERY_FILES.format(folder_id=folder_id), add_parent_name=add_parent_name)

    def list_type_folders(self, folder_id: str = FOLDER_ID_ROOT, add_parent_name: bool = False) -> List[Dict[str, str]]:
        return self._list_files(query=self.QUERY_FOLDERS.format(folder_id=folder_id), add_parent_name=add_parent_name)

    def list_files_in_folder(
        self, folder_id: str = FOLDER_ID_ROOT, add_parent_name: bool = False
    ) -> List[Dict[str, str]]:
        return self._list_files(
            query=self.QUERY_FILES_IN_FOLDER.format(folder_id=folder_id), add_parent_name=add_parent_name
        )

    def list_files_in_shared_folder(self, add_parent_name: bool = False) -> List[Dict[str, str]]:
        return self._list_files(
            query=self.QUERY_FILES_IN_FOLDER.format(folder_id=self.FOLDER_ID_SHARED_SERVICE_ACCOUNT),
            add_parent_name=add_parent_name,
        )

    def list_files_in_my_drive(self, add_parent_name: bool = False) -> List[Dict[str, str]]:
        return self._list_files(
            query=self.QUERY_FILES_IN_FOLDER.format(folder_id=self.FOLDER_ID_ROOT),
            add_parent_name=add_parent_name,
        )

    def list_files_with_name(self, name: str, add_parent_name: bool = False) -> List[Dict[str, str]]:
        return self._list_files(query=self.QUERY_FILES_WITH_NAME.format(name=name), add_parent_name=add_parent_name)

    def _list_files(self, query: str, add_parent_name: bool) -> List[Dict[str, str]]:
        try:
            results = []
            page_token = None
            while True:
                response = (
                    self.client.files()
                    .list(
                        q=query,
                        spaces="drive",
                        fields="nextPageToken, files(id, name, mimeType, parents)",
                        pageToken=page_token,
                    )
                    .execute()
                )
                files = response.get("files", [])
                if not files:
                    self.log.print("GoogleDriveAPI:_list_files", "No files or folders found.")
                    break
                else:
                    if add_parent_name:
                        for file in files:
                            # get parent name
                            parents = self.get_file_parents(parents=file.get("parents"))
                            file["parents_name"] = parents.get("name", "unknown")
                            # print(
                            #     f"Name: {file['name']} - ID: {file['id']} - Type: {file['mimeType']} - Parent Name: {file['parents_name']}"
                            # )
                    results.extend(files)
                    page_token = response.get("nextPageToken", None)
                    if page_token is None:
                        break

            return results
        except HttpError as error:
            self.log.print_error("GoogleSheetAPI:_list_files", f"An error occurred: {str(error)}")
            return []

    def del_file(self, file_id: str):
        try:
            self.client.files().delete(fileId=file_id).execute()
            print(f"File with id {file_id} has been deleted.")
        except HttpError as error:
            print(f"An error occurred while deleting the file: {error}")

    def del_all_files_in_folder(self, folder_id: str):
        self._del_all_files(folder_id=folder_id)

    def del_all_files_in_my_drive(self):
        self._del_all_files(folder_id=self.FOLDER_ID_ROOT)

    def del_all_files_in_shared_folder(self):
        self._del_all_files(folder_id=self.FOLDER_ID_SHARED_SERVICE_ACCOUNT)

    def _del_all_files(self, folder_id: str):
        try:
            files = self.list_files_in_folder(folder_id=folder_id)
            for file in files:
                if file.get("id", None) is not None:
                    self.del_file(file["id"])
        except HttpError as error:
            self.log.print_error("GoogleSheetAPI:_del_all_files", f"An error occurred: {str(error)}")

    def move_file_to_shared_folder(self, file_id: str, parents: List[str] = [FOLDER_ID_ROOT]):
        return self.move_file_to_folder(file_id, [self.FOLDER_ID_SHARED_SERVICE_ACCOUNT], parents)

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
