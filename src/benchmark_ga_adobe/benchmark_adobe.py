import sys
import os
import pandas as pd
from typing import Dict

# adding libraries folder to the system path
from libs import libs_base, dataframe as f_df, csv as f_csv
from libs.adobe import adobe_analytics_api as api_adobe


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
        self.adobe = api_adobe.AdobeAPI(self.config)

    def _parse_arguments(self) -> Dict[str, str]:
        if len(sys.argv) < 4:
            raise ValueError("Not enough arguments provided")

        result = {
            "site": sys.argv[1],
            "from_date": sys.argv[2],
            "to_date": sys.argv[3],
            "app_version": sys.argv[4],
        }
        return result

    def _log_init_info(self):
        # print header
        self.log.print_header(
            """
 ▗▄▖ ▗▄▄▄  ▗▄▖ ▗▄▄▖ ▗▄▄▄▖
▐▌ ▐▌▐▌  █▐▌ ▐▌▐▌ ▐▌▐▌   
▐▛▀▜▌▐▌  █▐▌ ▐▌▐▛▀▚▖▐▛▀▀▘
▐▌ ▐▌▐▙▄▄▀▝▚▄▞▘▐▙▄▞▘▐▙▄▄▖
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

    def _set_columns(self, df):
        columns = f"date,web_visits,web_visitors,web_views,and_visits,and_visitors,and_views,ios_visits,ios_visitors,ios_views"
        df.columns = columns.split(",")

    def request(self):
        df = self.adobe.reports("adobe/request.json", self.args["site"], self.args["from_date"], self.args["to_date"])
        if not f_df.Dataframe.is_empty(df):
            f_df.Dataframe.Columns.drop_to_index(df, 2)
            self._set_columns(df)
            f_df.Dataframe.Cast.columns_to_datetime(df, "date", "%b %d, %Y")
            f_df.Dataframe.Cast.columns_regex_to_int64(df, "^(web-|android-|ios-)")
            df = f_df.Dataframe.Sort.sort_by_columns(df, columns="date", ascending=False)

        # log
        self.log.print("App.request", "request completed!")
        return df

    def save_csv(self, df: pd.DataFrame):
        file_path = os.path.join(self.file_directory, f"csv/benchmark_adobe.csv")
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
