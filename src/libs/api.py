import requests
from abc import ABC, abstractmethod

# adding libraries folder to the system path
from libs import (
    log as f_log,
)

class API:
    def __init__(self, method, url, headers, payload):
        self.method = method
        self.url = url
        self.headers = headers
        self.payload = payload

    def request(self):
        response = requests.request(self.method, self.url, headers=self.headers, data=self.payload)
        if response.status_code != 200:
            f_log.Log.print_and_exit('API.request', str(response.status_code) + ' > ' + response.text)
        else:
            return response.json()
        
    @abstractmethod
    def __authentication(self):
        pass        

