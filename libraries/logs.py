import logging
import sys


class Logging:
    def __init__(self):
        logging.basicConfig(level="INFO")
        self.logger = logging.getLogger()

    def print(self, method, info):
        # self.logger.info("Method: {} - {}".format(method, info))
        self.logger.info(f'Method:{method}()::{info}')

    def print_error(self, info):
        self.logger.error(f'###########################################')
        self.logger.error(f'{info}')
        self.logger.error(f'###########################################')

    @staticmethod
    def print_and_exit(method, info):
        sys.exit('> ERROR - ' + method + '() - ' + info)