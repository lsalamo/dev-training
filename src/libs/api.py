import os
import requests
import pandas as pd
from abc import ABC, abstractmethod

# adding libraries folder to the system path
from libs import (
    log as f_log,
    csv as f_csv,
)


class API:
    def __init__(self, file: str):
        # file
        self._file = file

        # log
        self.log = f_log.Log()

        # csv
        self.csv = f_csv.CSV(self.file)

    def request(self, method, url, headers, payload):
        response = requests.request(method, url, headers=headers, data=payload)
        if response.status_code != 200:
            self.log.print_and_exit("API.request", str(response.status_code) + " > " + response.text)
        else:
            return response.json()

    @property
    def file(self):
        return self._file

    @file.setter
    def file(self, value):
        self._file = value

    @abstractmethod
    def __authentication(self):
        pass

    def save_csv(self, df: pd.DataFrame):
        """
        Save a pandas DataFrame to a CSV file.

        Parameters:
        df (pd.DataFrame): The pandas DataFrame containing the data to be saved.
        """
        self.csv.dataframe_to_csv(df)
        self.log.print("API.save_csv", f"file {self.csv.csv_name} saved")
