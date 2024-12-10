import os
import pandas as pd
import json

from libs import (
    files as f_files,
)

class JSON:
    def load_json(file:str):
        if f_files.File.exists_file(file):
            with open(file, 'r') as f:
                result = json.load(f)
            f.close()
        else:
            print('> ERROR > JSON.load_json() - File not found')
        return result

            

