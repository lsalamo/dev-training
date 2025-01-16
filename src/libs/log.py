import logging
from colorama import init, Fore, Style  # https://patorjk.com/software/taag/#p=display&f=RubiFont&t=Adobe


class Log:
    def __init__(self):
        init(autoreset=True)
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger()
        logging.getLogger("matplotlib.font_manager").disabled = True
        # self.logger.setLevel(logging.DEBUG)
        # logging.basicConfig(format="%(levelname)s::%(message)s", level=logging.DEBUG)
        logging.basicConfig(format="%(message)s", level=logging.DEBUG)

    def print_header(self, value):
        self.logger.info(f"{Fore.BLUE}{value}")

    def print_line(self):
        self.logger.info(f"\n{Fore.LIGHTBLUE_EX}{'='*50}\n")

    def print(self, method, value):
        self.logger.info(f"{Fore.LIGHTBLUE_EX}{method}:{Fore.WHITE}{value}")

    def print_error(self, value):
        self.logger.error(f"{Fore.RED}{Fore.WHITE}{value}")

    @staticmethod
    def print_and_exit(method, info):
        # sys.exit("> ERROR - " + method + "() - " + info)
        pass
