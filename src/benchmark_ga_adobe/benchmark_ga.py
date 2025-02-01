import sys
import os
import pandas as pd
from typing import Dict

# adding libraries folder to the system path
from libs import log as f_log, dataframe as f_df, csv as f_csv, libs_base
from libs.google import data_api as api_google


class App(libs_base.LibsBase):
    def __init__(self):
        super().__init__(__file__)

        # arguments
        self.args = self._parse_arguments()

        # configuration
        self.config = self.load_config()

        # logging
        self._log_init_info()

        # client
        self.google = api_google.DataAPI(self.config)

    def _parse_arguments(self) -> Dict[str, str]:
        if len(sys.argv) < 4:
            raise ValueError("Not enough arguments provided")
        return {
            "site": sys.argv[1],
            "from_date": sys.argv[2],
            "to_date": sys.argv[3],
            "app_version": sys.argv[4] if len(sys.argv) > 4 else "",
        }

    def _log_init_info(self):
        # print header
        self.log.print_header(
            """
 ▗▄▄▖ ▗▄▖  ▗▄▖  ▗▄▄▖▗▖   ▗▄▄▄▖
▐▌   ▐▌ ▐▌▐▌ ▐▌▐▌   ▐▌   ▐▌   
▐▌▝▜▌▐▌ ▐▌▐▌ ▐▌▐▌▝▜▌▐▌   ▐▛▀▀▘
▝▚▄▞▘▝▚▄▞▘▝▚▄▞▘▝▚▄▞▘▐▙▄▄▖▐▙▄▄▖
        """
        )

        # print name script and arguments
        script_name = sys.argv[0]
        arguments = sys.argv[1:]
        self.log.print("Name of python script:", script_name)
        self.log.print("Arguments:", "")
        if arguments:
            for i, arg in enumerate(arguments, start=1):
                self.log.print(f"  {i}.", f"{arg}")
        else:
            self.log.print_error(f"No se proporcionaron parámetros.")

    def _set_columns(self, df, platform):
        # Change platform value from "android" to "and"
        if platform == "android":
            platform = "and"
        columns = f"date,{platform}_visits,{platform}_visitors,{platform}_views"
        df.columns = columns.split(",")

    def request(self):
        df_combined = pd.DataFrame()
        platforms = ["web", "android", "ios"]
        # dimensions, metrics and data_ranges
        dimensions = "date"
        metrics = "sessions,totalUsers,screenPageViews"
        date_ranges = {
            "start_date": self.args["from_date"],
            "to_date": self.args["to_date"],
        }
        # order_bys
        order_bys = {"type": "dimension", "value": "date", "desc": True}

        for platform in platforms:
            # dimension filter
            dimesion_filter = [{"dimension": "platform", "value": platform}]
            # if app_version:
            #     dimesion_filter.append({"dimension": "appVersion", "value": app_version})

            df = self.google.reports(self.args["site"], dimensions, metrics, date_ranges, dimesion_filter, order_bys)
            if not f_df.Dataframe.is_empty(df):
                self._set_columns(df, platform)
                df_combined = f_df.Dataframe.Columns.join_two_frames_by_columns(df_combined, df, "date", "outer")
        f_df.Dataframe.Cast.columns_to_datetime(df_combined, "date", "%Y%m%d")
        df_combined = f_df.Dataframe.Sort.sort_by_columns(df_combined, columns="date", ascending=False)

        # log
        self.log.print("App.request", "request completed!")
        return df_combined

    def save_csv(self, df: pd.DataFrame):
        file_path = os.path.join(self.file_directory, f"csv/benchmark_ga.csv")
        f_csv.CSV.dataframe_to_csv(file_path, df)
        self.log.print("App.save_csv", f"CSV file saved to {file_path}")

    def print_end(self):
        self.log.print("App.main", "application finished!")
        self.log.print_line()


if __name__ == "__main__":
    try:
        app = App()
        df = app.request()
        app.save_csv(df)
        app.print_end()
    except Exception as e:
        print(f"App.main: An error occurred: {str(e)}")
        sys.exit(1)
