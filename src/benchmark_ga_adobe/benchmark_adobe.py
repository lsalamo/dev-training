import sys
import os

# adding libraries folder to the system path
from libs import (
    log as f_log,
    dataframe as f_df,
    json as f_json,
)
from libs.adobe import adobe_analytics_api as api_adobe


class App:
    def __init__(self, site, from_date, to_date, platform, app_version):
        # configuration
        self.site = site
        self.platform = platform
        self.app_version = app_version
        file_config = os.path.join(os.getcwd(), "src/adobe/config.json")
        self.config = f_json.JSON.load_json(file_config)
        self.config["adobe"]["__file__"] = __file__
        self.config["adobe"]["site"] = site
        self.config["adobe"]["platform"] = platform
        self.config["adobe"]["from_date"] = from_date
        self.config["adobe"]["to_date"] = to_date

        # client
        self.adobe = api_adobe.AdobeAPI(self.config)

    def set_columns(self, df):
        # columns = "date,platform,version," if self.app_version else "date,platform,"
        columns = f"date,platform,{self.platform}-visits,{self.platform}-visitors,{self.platform}-views"
        df.columns = columns.split(",")

    def request(self):
        df = self.adobe.reports("adobe/request.json")
        if not f_df.Dataframe.is_empty(df):
            df["platform"] = self.platform
            df = df[["value", "platform", 0, 1, 2]]
            self.set_columns(df)
            df = f_df.Dataframe.Cast.columns_to_datetime(df, "date", "%b %d, %Y")
            df = f_df.Dataframe.Cast.columns_regex_to_int64(df, "^(web-|android-|ios-)")
            df = f_df.Dataframe.Sort.sort_by_columns(df, columns="date", ascending=False)
            self.adobe.save_csv(df)

        # log
        log.print("App.request", "completed")
        return df

    def load_csv(self):
        df = self.adobe.load_csv()
        log.print("App.load_csv", "completed")
        return df


if __name__ == "__main__":
    # Logging
    log = f_log.Log()
    log.print("init", f"Current working directory: {os.getcwd()}")
    log.print("init", f"Python search paths: {sys.path}")
    log.print("init", f"Name of Python script: {sys.argv[0]}")
    log.print("init", f"Total arguments passed: {str(len(sys.argv))}")
    for i in range(1, len(sys.argv)):
        log.print("init", f"Argument: {sys.argv[i]}")

    # result
    result = {}
    result["site"] = sys.argv[1]
    result["from_date"] = sys.argv[2]
    result["to_date"] = sys.argv[3]
    result["platform"] = sys.argv[4]
    result["app_version"] = sys.argv[5] if result["platform"] != "web" else None

    app = App(
        site=result["site"],
        from_date=result["from_date"],
        to_date=result["to_date"],
        platform=result["platform"],
        app_version=result["app_version"],
    )
    result["df_ga"] = app.request()
    result["df_ga_csv"] = app.load_csv()
