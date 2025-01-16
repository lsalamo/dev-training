import os
import matplotlib.pyplot as plt

from libs import (
    files as f_files,
)


class Plot:
    def __init__(self):
        pass

    @staticmethod
    def plot_to_file(file_path: str, plot: plt):
        try:
            file_directory = f_files.Directory.get_directory(os.path.realpath(file_path))
            if os.path.basename(file_directory) == "img":
                f_files.Directory.create_directory(file_directory)
                if f_files.Directory.exists_directory(file_directory):
                    plot.savefig(file_path)
                else:
                    raise Exception(f"Plot.plot_to_file:Directory img does not exist")
            else:
                raise Exception(f"Plot.plot_to_file:Parent directory is not img")
        except Exception as e:
            raise Exception(f"Plot.plot_to_file:{str(e)}")
