import locale as lc
from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta


# https://docs.python.org/3.8/library/datetime.html
class DatetimeFormatter:

    @staticmethod
    def get_current_date() -> datetime:
        """Current date without time -> datetime.datetime(2025, 1, 23, 0, 0)"""
        today = date.today()
        return datetime(today.year, today.month, today.day)

    @staticmethod
    def get_current_datetime() -> datetime:
        """Current date with time -> datetime.datetime(2025, 1, 23, 1, 42, 11)"""
        return datetime.now()

    @staticmethod
    def today_to_str(pattern: str = "%Y%m%d") -> datetime:
        return datetime.now().strftime(pattern)

    @staticmethod
    def get_current_day():
        return datetime.now().day

    @staticmethod
    def get_current_month():
        return datetime.now().month

    @staticmethod
    def get_current_year():
        return datetime.now().year

    @staticmethod
    def get_day(value: datetime):
        return value.day

    @staticmethod
    def get_month(value: datetime):
        return value.month

    @staticmethod
    def get_year(value: datetime):
        return value.year

    @staticmethod
    def str_to_datetime(value: str, pattern: str = "%Y%m%d") -> datetime:
        if value == "today":
            return DatetimeFormatter.get_current_date()
        elif value == "yesterday":
            return DatetimeFormatter.datetime_add_days(days=-1)
        elif value == "7daysAgo":
            return DatetimeFormatter.datetime_add_days(days=-7)
        elif value == "3MonthsAgo":
            return DatetimeFormatter.datetime_add_months(months=-3)
        else:
            return datetime.strptime(value, pattern)

    @staticmethod
    def datetime_to_str(value: datetime, pattern: str = "%Y%m%d") -> str:
        return value.strftime(pattern)

    @staticmethod
    def datetime_to_day_month_year(value: datetime, locale="en_US") -> str:
        """Format datetime to 'day month year' (e.g., '5 January 2023')"""
        lc.setlocale(lc.LC_TIME, locale)
        return value.strftime("%d %B %Y")

    @staticmethod
    def datetime_to_day(value: datetime) -> str:
        """Format datetime to literal day (e.g., 'Monday')"""
        return value.strftime("%A")

    @staticmethod
    def datetime_to_month(value: datetime) -> str:
        """Format datetime to literal month (e.g., 'January')"""
        return value.strftime("%B")

    @staticmethod
    def datetime_add_days(value: datetime = get_current_date(), days: int = 0) -> datetime:
        """CALL > Datetime.datetime_add_days(dt, 3|-3)"""
        return value + timedelta(days=days)

    @staticmethod
    def datetime_add_months(value: datetime = get_current_date(), months: int = 0) -> datetime:
        """CALL > Datetime.datetime_add_months(dt, -3)"""
        return value + relativedelta(months=months)

    @staticmethod
    def datetime_add_years(value: datetime = get_current_date(), years: int = 0) -> datetime:
        """CALL > Datetime.datetime_add_years(dt, -3)"""
        return value + relativedelta(years=years)

    @staticmethod
    def diff_days(from_date: datetime, to_date: datetime) -> int:
        """Calculate the difference in days between two dates"""
        return (to_date - from_date).days
