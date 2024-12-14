import sys
import os

# adding libraries folder to the system path
from libs import (
    log as f_log,
    dataframe as f_df,
    csv as f_csv,
    json as f_json,
)
from libs.google import data_api as api_google


class Google:
    def __init__(self, property, from_date, end_date, platform, app_version):
        # configuration
        self.property = property
        self.platform = platform
        self.app_version = app_version
        file_config = os.path.join(os.getcwd(), "src/google/config.json")
        self.config = f_json.JSON.load_json(file_config)
        self.config["google"]["__file__"] = __file__
        self.config["google"]["property"] = property
        self.config["google"]["platform"] = platform

        # dimensions, metrics and data_ranges
        self.dimensions = "date,platform,appVersion" if app_version else "date,platform"
        self.metrics = "sessions,totalUsers,screenPageViews"
        self.date_ranges = {"start_date": from_date, "end_date": end_date}
        # dimension filter
        dimesion_filter = [{"dimension": "platform", "value": platform}]
        if app_version:
            dimesion_filter.append({"dimension": "appVersion", "value": app_version})
        self.dimension_filter = dimesion_filter
        # order_bys
        self.order_bys = {"type": "dimension", "value": "date", "desc": True}

    def set_columns(self, df):
        columns = "date,platform,version," if self.app_version else "date,platform,"
        columns += f"{self.platform}-visits,{self.platform}-visitors,{self.platform}-views"
        return columns.split(",")

    def get_google(self):
        google = api_google.DataAPI(self.config)
        df = google.request(self.dimensions, self.metrics, self.date_ranges, self.dimension_filter, self.order_bys)
        if not f_df.Dataframe.is_empty(df):
            self.set_columns(df)
            df = f_df.Dataframe.Cast.columns_to_datetime(df, "date", "%Y%m%d")
            # csv
            google.save_csv(df)

        # log
        log.print("google.get_google", "dataframe loaded")
        return df

    def get_google_csv(self):
        df = csv.csv_to_dataframe("google.csv")
        # f_df.Dataframe.Columns.drop_from_index(df, 4, True)
        # self.get_columns(df)

        # log
        log.print("google.get_google_csv", "dataframe loaded")

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

    # csv
    csv = f_csv.CSV(__file__)

    # result
    result = {}
    result["site"] = sys.argv[1]
    result["from_date"] = sys.argv[2]
    result["to_date"] = sys.argv[3]
    result["platform"] = sys.argv[4]
    result["app_version"] = sys.argv[5] if result["platform"] != "web" else None

    google = Google(
        property=result["site"],
        from_date=result["from_date"],
        end_date=result["to_date"],
        platform=result["platform"],
        app_version=result["app_version"],
    )
    result["df_ga"] = google.get_google()
    result["df_ga_csv"] = google.get_google_csv()
