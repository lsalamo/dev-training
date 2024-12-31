import numpy as np


class NumberFormatter:
    def __init__(self, decimal_places=2):
        self.decimal_places = decimal_places

    def format_number(self, number):
        if isinstance(number, (int, np.int64)):
            return f"{number:,}".replace(",", ".")
        elif isinstance(number, (float, np.float64)):
            return f"{int(number):,}".replace(",", ".")
        else:
            return str(number)

    def format_float(self, number):
        if isinstance(number, (float, np.float64)):
            return f"{number:,.{self.decimal_places}f}".replace(",", ".")
        else:
            return str(number)

    def format_percentage(self, number):
        return f"{number:.{self.decimal_places}f}%"

    def format_currency(self, number, currency_symbol="€"):
        return f"{currency_symbol}{self.format_number(number)}"
