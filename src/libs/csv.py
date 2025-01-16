import os
import pandas as pd

from libs import (
    files as f_files,
)


class CSV:
    def __init__(self):
        pass

    @staticmethod
    def csv_to_dataframe(file_path: str) -> pd.DataFrame:
        try:
            df = pd.DataFrame()
            if f_files.File.exists_file(file_path):
                df = pd.read_csv(file_path, header=0)
            return df
        except FileNotFoundError as e:
            raise FileNotFoundError(f"CSV.csv_to_dataframe:file not found: {file_path}")
        except Exception as e:
            raise Exception(f"CSV.csv_to_dataframe:{str(e)}")

    @staticmethod
    def dataframe_to_csv(file_path: str, df: pd.DataFrame):
        try:
            file_directory = f_files.Directory.get_directory(os.path.realpath(file_path))
            if os.path.basename(file_directory) == "csv":
                f_files.Directory.create_directory(file_directory)
                if f_files.Directory.exists_directory(file_directory):
                    df.to_csv(file_path, index=False, header=True)
                else:
                    raise Exception(f"CSV.dataframe_to_csv:Directory csv does not exist")
            else:
                raise Exception(f"CSV.dataframe_to_csv:Parent directory is not csv")
        except Exception as e:
            raise Exception(f"CSV.dataframe_to_csv:{str(e)}")
