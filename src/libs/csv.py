import os
import pandas as pd

from libs import (
    files as f_files,
)

class CSV:
    def __init__(self, file:str):
        path_csv = os.path.dirname(os.path.realpath(file))
        path_csv = os.path.join(path_csv, 'csv')
        f_files.Directory.create_directory(path_csv)         
        self.path = path_csv

    def csv_to_dataframe(self, file:str):
        df = pd.DataFrame()
        file = os.path.join(self.path, file)
        file = f_files.File(file)
        if file.exists_file():
            try:
                df = pd.read_csv(file.path_file, header=None)
            except BaseException as e:
                raise Exception(f'CSV.csv_to_dataframe:{e}')

        return df

    def dataframe_to_csv(self, df:pd.DataFrame, file:str):
        if f_files.Directory.exists_directory(self.path):
            file = os.path.join(self.path, file)
            df.to_csv(file, index=False)
        else:
            raise Exception(f'CSV.dataframe_to_csv:Directory csv does not exist')

