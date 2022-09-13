import logging
import sys


class Logging:
    def __init__(self):
        logging.basicConfig(level="INFO")
        self.logger = logging.getLogger()

    def print(self, method, info):
        self.logger.info("Method: {} - {}".format(method, info))

    @staticmethod
    def print_error(method, info):
        print('> ERROR - ' + method + '() - ' + info)

    @staticmethod
    def print_and_exit(method, info):
        sys.exit('> ERROR - ' + method + '() - ' + info)