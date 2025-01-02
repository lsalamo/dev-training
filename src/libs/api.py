import os
import requests
import pandas as pd
from typing import Dict
from abc import ABC, abstractmethod

# adding libraries folder to the system path
from libs import (
    log as f_log,
    csv as f_csv,
    json as f_json,
)


class API:
    def __init__(self, file: str):
        # file
        self._file = file

        # log
        self._log = f_log.Log()

        # csv
        self._csv = f_csv.CSV(self.file)

    def request(self, method, url, headers, payload):
        response = requests.request(method, url, headers=headers, data=payload)
        if response.status_code != 200:
            self._log.print_and_exit("API.request", str(response.status_code) + " > " + response.text)
        else:
            return response.json()

    @property
    def file(self):
        return self._file

    @file.setter
    def file(self, value):
        self._file = value

    @property
    def log(self):
        return self._log

    @log.setter
    def log(self, value):
        self._log = value

    @abstractmethod
    def __authentication(self):
        pass

    def load_config(self) -> Dict[str, str]:
        config = {}
        if type(self).__name__ == "AdobeAPI":
            file_config = os.path.join(os.getcwd(), "src/adobe/_credentials/bearer_token/config.json")
            config = f_json.JSON.load_json(file_config)
            config = config["adobe"]
        elif type(self).__name__ == "DataAPI":
            file_config = os.path.join(os.getcwd(), "src/google/_credentials/config.json")
            config = f_json.JSON.load_json(file_config)
            config = config["google"]
        return config

    def save_csv(self, df: pd.DataFrame):
        """
        Save a pandas DataFrame to a CSV file.

        Parameters:
        df (pd.DataFrame): The pandas DataFrame containing the data to be saved.
        """
        self._csv.dataframe_to_csv(df)
        self._log.print("API.save_csv", f"file {self._csv.csv_name} saved")
