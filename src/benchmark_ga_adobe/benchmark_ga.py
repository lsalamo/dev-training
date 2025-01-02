import sys
import os
from typing import Dict

# adding libraries folder to the system path
from libs import (
    log as f_log,
    dataframe as f_df,
)
from libs.google import data_api as api_google


class App:
    def __init__(self):
        # arguments
        self.args = self.__parse_arguments()

        # logging
        self.__log_init_info()

        # client
        self.google = api_google.DataAPI(self.args["file"])

    def __parse_arguments(self) -> Dict[str, str]:
        if len(sys.argv) < 4:
            raise ValueError("Not enough arguments provided")

        result = {
            "file": __file__,
            "site": sys.argv[1],
            "platform": sys.argv[2],
            "from_date": sys.argv[3],
            "to_date": sys.argv[4],
        }
        return result

    def __log_init_info(self):
        log.print_header(
            """
 ▗▄▄▖ ▗▄▖  ▗▄▖  ▗▄▄▖▗▖   ▗▄▄▄▖
▐▌   ▐▌ ▐▌▐▌ ▐▌▐▌   ▐▌   ▐▌   
▐▌▝▜▌▐▌ ▐▌▐▌ ▐▌▐▌▝▜▌▐▌   ▐▛▀▀▘
▝▚▄▞▘▝▚▄▞▘▝▚▄▞▘▝▚▄▞▘▐▙▄▄▖▐▙▄▄▖
        """
        )

        # log.print("init", f"Current working directory: {os.getcwd()}")
        # log.print("init", f"Python search paths: {sys.path}")
        log.print_params("Name of python script:", sys.argv[0])
        # log.print("init", f"Total arguments passed: {len(sys.argv)}")
        log.print_params("Arguments:", "")
        arguments = sys.argv[1:]
        if arguments:
            for i, arg in enumerate(arguments, start=1):
                log.print_params(f"  {i}.", f"{arg}")
        else:
            log.print_error(f"No se proporcionaron parámetros.")
        log.print_line()

    def __set_columns(self, df):
        columns = f"date,{self.args["platform"]}-visits,{self.args["platform"]}-visitors,{self.args["platform"]}-views"
        df.columns = columns.split(",")

    def request(self):
        # dimensions, metrics and data_ranges
        dimensions = "date"
        metrics = "sessions,totalUsers,screenPageViews"
        date_ranges = {
            "start_date": self.args["from_date"],
            "to_date": self.args["to_date"],
        }
        # dimension filter
        dimesion_filter = [{"dimension": "platform", "value": self.args["platform"]}]
        # if app_version:
        #     dimesion_filter.append({"dimension": "appVersion", "value": app_version})
        # order_bys
        order_bys = {"type": "dimension", "value": "date", "desc": True}

        df = self.google.request(self.args["site"], dimensions, metrics, date_ranges, dimesion_filter, order_bys)
        if not f_df.Dataframe.is_empty(df):
            self.__set_columns(df)
            f_df.Dataframe.Cast.columns_to_datetime(df, "date", "%Y%m%d")
            df = f_df.Dataframe.Sort.sort_by_columns(df, columns="date", ascending=False)
            self.google.save_csv(df)

        # log
        log.print("App.request", "completed")
        return df


if __name__ == "__main__":
    try:
        log = f_log.Log()
        app = App()
        df = app.request()
        log.print("App.main", "completed")
    except Exception as e:
        log.print("App.main", f"An error occurred: {str(e)}")
        sys.exit(1)
