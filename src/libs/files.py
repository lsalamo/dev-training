import os

class Directory:
    @staticmethod
    def change_working_directory(directory):
        os.chdir(directory)

    @staticmethod
    def set_working_directory(directory):
        return os.chdir(directory)

    @staticmethod
    def get_working_directory():
        return os.getcwd()

    @staticmethod
    def get_directory(path):
        return os.path.dirname(path)

    @staticmethod
    def create_directory(directory):
        # if os.path.isdir(directory):
        #    shutil.rmtree(directory)
        if not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)

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
