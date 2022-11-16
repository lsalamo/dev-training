import os
import shutil
import sys
import pandas as pd
import logging


class CSV:
    @staticmethod
    def csv_to_dataframe(file):
        df = pd.DataFrame()
        file = File(file)
        if file.exists_file():
            try:
                df = pd.read_csv(file.path_file, header=None)
            except BaseException as e:
                Log.print_and_exit('CSV.csv_to_dataframe', format(e))
        return df

    @staticmethod
    def dataframe_to_file(df, file):
        directory = os.path.join(Directory.get_working_directory(), 'csv/')
        if Directory.exists_directory(directory):
            file = os.path.join(directory, file)
            df.to_csv(file, index=False)
        else:
            Log.print_and_exit('CSV.dataframe_to_file', 'Directory csv does not exist')


class Log:
    @staticmethod
    def print(method, info):
        print('> ' + method + '() - ' + info)

    @staticmethod
    def print_error(method, info):
        print('> ERROR - ' + method + '() - ' + info)

    @staticmethod
    def print_and_exit(method, info):
        sys.exit('> ERROR - ' + method + '() - ' + info)


class Directory:
    @staticmethod
    def change_working_directory(directory):
        os.chdir(directory)

    @staticmethod
    def get_working_directory():
        return os.getcwd()

    @staticmethod
    def create_directory(directory):
        directory = os.path.join(Directory.get_working_directory(), directory)
        # if os.path.isdir(directory):
        #    shutil.rmtree(directory)
        if not os.path.isdir(directory):
            os.makedirs(directory)

    @staticmethod
    def exists_directory(directory):
        return os.path.isdir(directory)


class File:
    def __init__(self, file):
        self.working_directory = Directory.get_working_directory()
        self.file = file
        self.path_file = os.path.join(self.working_directory, self.file)

    def exists_file(self):
        if os.path.isfile(self.path_file):
            return True
        else:
            return False

    def read_file(self):
        result = ''
        if self.exists_file():
            file = open(self.path_file)
            result = file.read()
            file.close()
        else:
            print('> ERROR > File.read_file() - File not found')
        return result
