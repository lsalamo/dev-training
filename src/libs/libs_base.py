import os
import pandas as pd
from typing import Dict
import locale

# adding libraries folder to the system path
from libs import (
    files as f_files,
    log as f_log,
    json as f_json,
)


class LibsBase:
    def __init__(self, file: str):
        # Set the locale to Spanish
        locale.setlocale(locale.LC_ALL, "es_ES.UTF-8")

        # file
        self._file = file

        # log
        self._log = f_log.Log()

    @property
    def file(self):
        return self._file

    @file.setter
    def file(self, value):
        self._file = value

    @property
    def file_directory(self):
        return f_files.Directory.get_directory(os.path.realpath(self._file))

    @property
    def file_name(self):
        return os.path.basename(self._file)

    @property
    def log(self):
        return self._log

    @log.setter
    def log(self, value):
        self._log = value

    def load_config(self) -> Dict[str, str]:
        config = os.path.join(os.getcwd(), "src/libs/_credentials/config.json")
        config = f_json.JSON.load_json(config)
        config["file"] = self.file
        config["file_directory"] = self.file_directory
        config["file_name"] = self.file_name
        return config
