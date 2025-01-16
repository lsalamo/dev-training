import requests
from abc import ABC, abstractmethod

# adding libraries folder to the system path
from libs import (
    log as f_log,
)


class API:
    def __init__(self):
        self._log = f_log.Log()

    @property
    def log(self):
        return self._log

    @log.setter
    def log(self, value):
        self._log = value

    def request(self, method, url, headers, payload):
        response = requests.request(method, url, headers=headers, data=payload)
        if response.status_code != 200:
            self.log.print_and_exit("API.request", str(response.status_code) + " > " + response.text)
        else:
            return response.json()

    @abstractmethod
    def __authentication(self):
        pass
