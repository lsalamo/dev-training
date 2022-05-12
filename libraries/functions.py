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
            sys.exit('ERROR ' + str(response.status_code) + ' > ' + response.text)
        else:
            response = response.json()
            total_records = response['numberOfElements']
            if total_records > 0:
                df = pd.DataFrame.from_dict(response['rows'])
        return df


class Log:
    @staticmethod
    def print(method, info):
        print('> ' + method + '() - ' + info)


class Directory:
    def __init__(self, dir):
        self.working_directory = dir
        # Change working directory
        os.chdir(dir)

    def create_directory(self, dir):
        dir = os.path.join(self.working_directory, dir)
        if os.path.isdir(dir):
            shutil.rmtree(dir)
        os.makedirs(dir)


class File:
    def __init__(self, file):
        self.working_directory = os.getcwd()
        self.file = file

    def exists_file(self):
        file = os.path.join(self.working_directory, self.file)
        if os.path.isfile(file):
            return True
        else:
            return False

    def read_file(self):
        result = ''
        if self.exists_file():
            file = open(os.path.join(self.working_directory, self.file))
            result = file.read()
            file.close()
        else:
            print('> ERROR > File.read_file() - File not found')
        return result
