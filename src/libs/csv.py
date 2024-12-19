import os
import pandas as pd

from libs import (
    files as f_files,
)


class CSV:
    def __init__(self, file: str):
        csv_path = f_files.Directory.get_directory(os.path.realpath(file))
        csv_path = os.path.join(csv_path, "csv")
        f_files.Directory.create_directory(csv_path)
        self.csv_path = csv_path
        self.csv_name = os.path.basename(file).replace(".py", ".csv")

    def csv_to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame()
        file = os.path.join(self.csv_path, self.csv_name)
        if f_files.File.exists_file(file):
            try:
                df = pd.read_csv(file, header=0)
            except Exception as e:
                raise Exception(f"CSV.csv_to_dataframe:{str(e)}")
        else:
            raise FileNotFoundError(f"CSV file not found: {file}")
        return df

    @staticmethod
    def csv_to_dataframe(file: str) -> pd.DataFrame:
        df = pd.DataFrame()
        if f_files.File.exists_file(file):
            try:
                df = pd.read_csv(file, header=0)
            except Exception as e:
                raise Exception(f"CSV.csv_to_dataframe:{str(e)}")
        else:
            raise FileNotFoundError(f"CSV file not found: {file}")
        return df

    def dataframe_to_csv(self, df: pd.DataFrame):
        if f_files.Directory.exists_directory(self.csv_path):
            file = os.path.join(self.csv_path, self.csv_name)
            df.to_csv(file, index=False, header=True)
        else:
            raise Exception(f"CSV.dataframe_to_csv:Directory csv does not exist")
