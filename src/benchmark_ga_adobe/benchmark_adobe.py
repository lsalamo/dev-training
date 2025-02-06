import sys
import os
import pandas as pd
from typing import Dict

# adding libraries folder to the system path
from libs import libs_base, dataframe as f_df, csv as f_csv
from libs.adobe import adobe_analytics_api as api_adobe


class App(libs_base.LibsBase):
    """
    A class to manage Adobe Analytics

    Documentation:
        - QA Adobe: https://experience.adobe.com/#/@schibstedspain/so:schibs1/analytics/spa/#/workspace/edit/67a49b4e195ca75a9ca8ff96
    """

    PLATFORMS = ["web", "and", "ios"]
    FILTER_PLATFORM = {
        "web": "s2165_5ce2aa77c0fdcb1990ad2326",
        "and": "s2165_5ce2a951f0f34a6c7c320948",
        "ios": "s2165_5ce2a75f5d3c0b0671203160",
    }

    FILTER_APP_VERSION = {
        "and": "s2165_67a49c827d7e5d1e9a5a3130",
        "ios": "s2165_67a49d17c707a75937ca3bae",
    }

    def __init__(self):
        super().__init__(__file__)

        self.args = self._parse_arguments()
        self.config = self.load_config()
        self.app_version = self._parse_app_version()
        self._log_init_info()

        self.adobe = api_adobe.AdobeAPI(self.config)

    def _parse_arguments(self) -> Dict[str, str]:
        if len(sys.argv) < 4:
            raise ValueError("Not enough arguments provided")
        return {
            "site": sys.argv[1],
            "from_date": sys.argv[2],
            "to_date": sys.argv[3],
            "app_version": sys.argv[4] if sys.argv[4] not in ("None", "") else None,
        }

    def _parse_app_version(self) -> Dict[str, str]:
        if not self.args.get("app_version"):
            return None
        result = {}
        app_versions = self.args["app_version"].split(":")
        for version in app_versions:
            platform, ver = version.split("-", 1)
            if platform in ("and", "ios"):
                result[platform] = ver
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

    def _set_columns(self, df: pd.DataFrame, platform: str):
        columns = f"date,{platform}_visits,{platform}_visitors,{platform}_views"
        df.columns = columns.split(",")

    def request(self):
        def cast_columns(df: pd.DataFrame) -> pd.DataFrame:
            f_df.Dataframe.Cast.columns_to_datetime(df, "date", "%b %d, %Y")
            f_df.Dataframe.Cast.columns_regex_to_int64(df, "^(web_|android_|ios_)")
            return df

        df_combined = pd.DataFrame()
        for platform in self.PLATFORMS:
            app_version = self.app_version.get(platform, None)
            file_request = "request/request_app_version.json" if app_version else "request/request.json"

            payload = self.adobe.get_payload(
                file_request, self.args["site"], self.args["from_date"], self.args["to_date"]
            )
            payload = payload.replace("{{filter_platform}}", self.FILTER_PLATFORM.get(platform))
            if app_version:
                payload = payload.replace("{{filter_app_version}}", self.FILTER_APP_VERSION.get(platform))

            platform_str = f"{platform.upper()} ({app_version})" if app_version else platform.upper()
            df = self.adobe.reports(payload)
            if not f_df.Dataframe.is_empty(df):
                df = f_df.Dataframe.Columns.drop(df, ["data", "itemId"])
                self._set_columns(df, platform)
                df_combined = f_df.Dataframe.Columns.join_two_frames_by_columns(df_combined, df, "date", "outer")
            else:
                self.log.print_error("App.request", f"No data found for platform: {platform_str})", exit=True)

            self.log.print("App.request", f"report requested for platform: {platform_str}")

        df_combined = (
            df_combined.pipe(f_df.Dataframe.Fill.nan, value=0)
            .pipe(cast_columns)
            .pipe(f_df.Dataframe.Sort.sort_by_columns, columns="date", ascending=False)
        )

        return df_combined

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
