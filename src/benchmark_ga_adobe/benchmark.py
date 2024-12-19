import sys
import os

# adding libraries folder to the system path
from libs import (
    log as f_log,
    dataframe as f_df,
    csv as f_csv,
)


class App:
    def __init__(self, platform):
        self.platform = platform

    def load_csv(self, file):
        file_path = os.path.join(os.getcwd(), file)
        df = f_csv.CSV.csv_to_dataframe(file_path)
        log.print("App.load_csv", f"dataframe loaded from file {file_path}")
        return df

    def merge_adobe_google(self, df_aa, df_ga):
        # merge
        df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_aa, df_ga, "date", "outer", ("-aa", "-ga"))
        # transform
        df = df.fillna(0)
        df = f_df.Dataframe.Cast.columns_regex_to_int64(df, "^(web-|android-|ios-)")

        # columns
        columns = f"date,{self.platform}-visits-aa,{self.platform}-visits-ga,{self.platform}-visitors-aa,{self.platform}-visitors-ga,{self.platform}-views-aa,{self.platform}-views-ga"
        df.columns = columns.split(",")

        # log
        log.print("App.merge_adobe_google", "completed")
        return df


if __name__ == "__main__":
    # Logging
    log = f_log.Log()

    platform = sys.argv[4]

    app = App(platform)
    df_ga = app.load_csv("src/benchmark_ga_adobe/csv/benchmark_ga.csv")
    df_aa = app.load_csv("src/benchmark_ga_adobe/csv/benchmark_adobe.csv")
    df = app.merge_adobe_google(df_ga, df_aa)
    df = f_df.Dataframe.Sort.sort_by_columns(df, columns="date", ascending=False)

    # csv
    csv = f_csv.CSV(__file__)
    csv.dataframe_to_csv(df)

    print("fin")
