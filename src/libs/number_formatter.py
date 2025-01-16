import numpy as np


class NumberFormatter:
    def __init__(self, decimal_places=2):
        pass

    @staticmethod
    def format_number(number):
        if isinstance(number, (int, np.int64)):
            return f"{number:,}".replace(",", ".")
        elif isinstance(number, (float, np.float64)):
            return f"{int(number):,}".replace(",", ".")
        else:
            return str(number)

    @staticmethod
    def format_float(number, decimal_places=2):
        if isinstance(number, (float, np.float64)):
            return f"{number:,.{decimal_places}f}".replace(",", ".")
        else:
            return str(number)

    @staticmethod
    def format_percentage(number, decimal_places=2):
        return f"{number:.{decimal_places}f}%"

    @staticmethod
    def format_currency(number, currency_symbol="€"):
        return f"{currency_symbol}{NumberFormatter.format_number(number)}"
