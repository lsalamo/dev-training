import logging
import sys


class Log:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)

    def print(self, method, info):
        self.logger.info(f"Method:{method}():{info}")

    def print_error(self, info):
        self.logger.error(f"###########################################")
        self.logger.error(f"{info}")
        self.logger.error(f"###########################################")

    @staticmethod
    def print_and_exit(method, info):
        sys.exit("> ERROR - " + method + "() - " + info)
