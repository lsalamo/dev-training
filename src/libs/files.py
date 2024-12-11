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
    @staticmethod
    def exists_file(file:str):
        # file = os.path.join(Directory.get_working_directory(), file)
        if os.path.isfile(file):
            return True
        else:
            return False
    
    @staticmethod
    def read_file(file):
        if File.exists_file(file):
            with open(file, 'r') as f:
                result = f.read()
            f.close()
        else:
            print('> ERROR > File.read_file() - File not found')
        return result
    
