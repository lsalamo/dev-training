from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta


# https://docs.python.org/3.8/library/datetime.html
class DatetimeFormatter:
    @staticmethod
    def get_current_datetime() -> datetime:
        # date.today()
        return datetime.now()

    def today_to_str() -> datetime:
        # date.today().strftime("%Y%m%d")
        return datetime.now().strftime("%Y%m%d")

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
        """CALL > Datetime.str_to_datetime('2022-05-01', '%Y-%m-%d')"""
        return value.day

    @staticmethod
    def get_month(value: datetime):
        """CALL > Datetime.str_to_datetime('2022-05-01', '%Y-%m-%d')"""
        return value.day

    @staticmethod
    def get_year(value: datetime):
        """CALL > Datetime.str_to_datetime('2022-05-01', '%Y-%m-%d')"""
        return value.year

    @staticmethod
    def str_to_datetime(value: str, pattern: str) -> datetime:
        """CALL > Datetime.str_to_datetime('2022-05-01', '%Y-%m-%d')"""
        return datetime.strptime(value, pattern)

    @staticmethod
    def datetime_to_str(value: datetime, pattern: str) -> str:
        """CALL > Datetime.datetime_to_str(dt, '%Y-%m-%dT00:00:000')"""
        return value.strftime(pattern)

    @staticmethod
    def datetime_add_days(value: datetime = get_current_datetime(), days: int = 0) -> datetime:
        """CALL > Datetime.datetime_add_days(dt, 3|-3)"""
        return value + timedelta(days=days)

    @staticmethod
    def datetime_add_months(value: datetime = get_current_datetime(), years: int = 0) -> datetime:
        """CALL > Datetime.datetime_add_months(dt, -3)"""
        return value + relativedelta(months=years)

    @staticmethod
    def datetime_add_years(value: datetime = get_current_datetime(), years: int = 0) -> datetime:
        """CALL > Datetime.datetime_add_years(dt, -3)"""
        return value + relativedelta(years=years)
