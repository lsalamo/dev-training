import sys
import os
from typing import Dict

# adding libraries folder to the system path
from libs import (
    log as f_log,
    dataframe as f_df,
    json as f_json,
)
from libs.google import data_api as api_google


class App:
    def __init__(self, site, platform, from_date, to_date):
        # logging
        self._log_init_info()

        # configuration
        self.config = self._load_config(site, platform, from_date, to_date)

        # client
        self.google = api_google.DataAPI(self.config)

    def _log_init_info(self):
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

    def _load_config(self, site: str, platform: str, from_date: str, to_date: str) -> Dict[str, str]:
        file_config = os.path.join(os.getcwd(), "src/google/_credentials/config.json")
        config = f_json.JSON.load_json(file_config)
        config["google"].update(
            {"__file__": __file__, "site": site, "platform": platform, "from_date": from_date, "to_date": to_date}
        )
        return config

    def _set_columns(self, df):
        platform = self.config["google"]["platform"]
        columns = f"date,{platform}-visits,{platform}-visitors,{platform}-views"
        df.columns = columns.split(",")

    def request(self):
        # dimensions, metrics and data_ranges
        self.dimensions = "date"
        self.metrics = "sessions,totalUsers,screenPageViews"
        self.date_ranges = {
            "start_date": self.config["google"]["from_date"],
            "to_date": self.config["google"]["to_date"],
        }
        # dimension filter
        dimesion_filter = [{"dimension": "platform", "value": self.config["google"]["platform"]}]
        # if app_version:
        #     dimesion_filter.append({"dimension": "appVersion", "value": app_version})
        self.dimension_filter = dimesion_filter
        # order_bys
        self.order_bys = {"type": "dimension", "value": "date", "desc": True}

        df = self.google.request(self.dimensions, self.metrics, self.date_ranges, self.dimension_filter, self.order_bys)
        if not f_df.Dataframe.is_empty(df):
            self._set_columns(df)
            df = f_df.Dataframe.Cast.columns_to_datetime(df, "date", "%Y%m%d")
            self.google.save_csv(df)

        # log
        log.print("App.request", "completed")
        return df


def parse_arguments() -> Dict[str, str]:
    if len(sys.argv) < 4:
        raise ValueError("Not enough arguments provided")

    result = {
        "site": sys.argv[1],
        "platform": sys.argv[2],
        "from_date": sys.argv[3],
        "to_date": sys.argv[4],
    }
    return result


if __name__ == "__main__":
    try:
        log = f_log.Log()
        args = parse_arguments()
        app = App(**args)
        df = app.request()
        log.print("App.main", "completed")
    except Exception as e:
        log.print("App.main", f"An error occurred: {str(e)}")
        sys.exit(1)
