import os
import shutil
import requests
import sys
import pandas as pd


class API:
    def __init__(self, method, url, headers, payload):
        self.method = method
        self.url = url
        self.headers = headers
        self.payload = payload

    def request(self):
        df = pd.DataFrame()
        response = requests.request(self.method, self.url, headers=self.headers, data=self.payload)
        if response.status_code != 200:
            Log.print_and_exit('API.request', str(response.status_code) + ' > ' + response.text)
        else:
            response = response.json()
            total_records = response['numberOfElements']
            if total_records > 0:
                df = pd.DataFrame.from_dict(response['rows'])
        return df


class Adobe_Report_API(API):
    def __init__(self, token, payload):
        url = 'https://analytics.adobe.io/api/schibs1/reports'
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token,
            'x-api-key': '5e9fd55fa92c4a0a82b3f2a74c088e60',
            'x-proxy-global-company-id': 'schibs1'
        }
        super().__init__('POST', url, headers, payload)


class CSV:

    @staticmethod
    def csv_to_dataframe(file):
        df = pd.DataFrame()
        file = File(file)
        if file.exists_file():
            df = pd.read_csv(file.path_file, header=None)
        return df

    @staticmethod
    def dataframe_to_file(df, file):
        dir = os.path.join(Directory.get_working_directory(), 'csv/')
        if Directory.exists_directory(dir):
            file = os.path.join(dir, file)
            df.to_csv(file)
        else:
            Log.print_and_exit('CSV.dataframe_to_file', 'Directory csv does not exist')


class Log:
    @staticmethod
    def print(method, info):
        print('> ' + method + '() - ' + info)

    @staticmethod
    def print_and_exit(method, info):
        sys.exit('> ERROR - ' + method + '() - ' + info)


class Directory:

    @staticmethod
    def change_working_directory(dir):
        os.chdir(dir)

    @staticmethod
    def get_working_directory():
        return os.getcwd()

    @staticmethod
    def create_directory(dir):
        dir = os.path.join(Directory.get_working_directory(), dir)
        if os.path.isdir(dir):
            shutil.rmtree(dir)
        os.makedirs(dir)

    @staticmethod
    def exists_directory(dir):
        if os.path.isdir(dir):
            return True
        else:
            return False


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
