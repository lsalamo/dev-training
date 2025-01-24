import numpy as np


class NumberFormatter:
    def __init__(self, decimal_places=2):
        pass

    @staticmethod
    def to_number(value) -> int:
        return int(value)

    @staticmethod
    def to_float(value, decimal_places: int = 2) -> int:
        return round(float(decimal_places), 2)

    @staticmethod
    def format_number(value, large_number_formatter: bool = False) -> str:
        value = int(value)
        if large_number_formatter:
            if value >= 1e9:
                value = f"{value/1e9:,.2f}B"
            elif value >= 1e6:
                value = f"{value/1e6:,.1f}M"
            elif value >= 1e3:
                value = f"{value/1e3:,.0f}K"
            else:
                value = f"{value:,.0f}"
        else:
            value = f"{value:,.0f}"
        return value.replace(",", ".")

    @staticmethod
    def format_float(value, decimal_places=2) -> str:
        if isinstance(value, (float, np.float64)):
            # return f"{value:,.{decimal_places}f}".replace(",", ".")
            return f"{value:.{decimal_places}f}"
        else:
            return str(value)

    @staticmethod
    def format_percentage(value, decimal_places=2) -> str:
        return f"{value:.{decimal_places}f}%"

    @staticmethod
    def format_currency(value, currency_symbol="€") -> str:
        return f"{currency_symbol}{NumberFormatter.format_number(value)}"
