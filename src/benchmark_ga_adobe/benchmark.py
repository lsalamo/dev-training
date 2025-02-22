import sys
import os
from typing import List, Dict
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.axes import Axes


# adding libraries folder to the system path
from libs import (
    dataframe as f_df,
    csv as f_csv,
    plot as f_plot,
    number_formatter as nf,
    datetime_formatter as dtf,
    files as f_files,
    libs_base,
)
from libs.google import gmail_smtplib_api, google_sheets_api as gsheet_api


class App(libs_base.LibsBase):
    PLATFORMS = ["web", "and", "ios"]
    COLS = ["visits", "visitors", "views"]

    def __init__(self):
        super().__init__(__file__)

        self.args = self._parse_arguments()
        self.config = self.load_config()
        self.app_version = self._parse_app_version()

        self.site_title = self._get_site_name(self.args["site"]).upper()
        self.from_date = dtf.DatetimeFormatter.str_to_datetime(self.args["from_date"], "%Y-%m-%d")
        self.to_date = dtf.DatetimeFormatter.str_to_datetime(self.args["to_date"], "%Y-%m-%d")
        self.date_diff_days = dtf.DatetimeFormatter.diff_days(self.from_date, self.to_date)
        self.title = (
            f"Comparison Adobe vs Google {self.site_title} - {dtf.DatetimeFormatter.today_to_str(pattern="%Y/%m/%d")}"
        )

        self._log_init_info()

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

    def _get_site_name(self, site: str) -> str:
        site_names = {
            "cnet": "Coches",
            "mnet": "Motos",
            "cnet_pro": "Coches PRO",
            "ma": "Milanuncios",
            "ijes": "Infojobs ES",
            "ijit": "Infojobs IT",
            "ij_epreselec": "Infojobs Epreselec",
            "fc": "Fotocasa",
            "hab": "Habitaclia",
            "fc_pro": "Fotocasa PRO",
        }
        return site_names.get(site, "Unknown site")

    def _log_init_info(self):
        # print header
        self.log.print_header(
            """
▗▄▄▖ ▗▄▄▄▖▗▖  ▗▖ ▗▄▄▖▗▖ ▗▖▗▖  ▗▖ ▗▄▖ ▗▄▄▖ ▗▖ ▗▖
▐▌ ▐▌▐▌   ▐▛▚▖▐▌▐▌   ▐▌ ▐▌▐▛▚▞▜▌▐▌ ▐▌▐▌ ▐▌▐▌▗▞▘
▐▛▀▚▖▐▛▀▀▘▐▌ ▝▜▌▐▌   ▐▛▀▜▌▐▌  ▐▌▐▛▀▜▌▐▛▀▚▖▐▛▚▖ 
▐▙▄▞▘▐▙▄▄▖▐▌  ▐▌▝▚▄▄▖▐▌ ▐▌▐▌  ▐▌▐▌ ▐▌▐▌ ▐▌▐▌ ▐▌
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

    def merge_adobe_google(self) -> pd.DataFrame:
        def load_csv(filename: str) -> pd.DataFrame:
            file_path = os.path.join(self.file_directory, "csv", filename)
            df = f_csv.CSV.csv_to_dataframe(file_path)
            self.log.print("App.merge_adobe_google", f"dataframe loaded from file {file_path}")
            return df

        def cast_columns(df: pd.DataFrame) -> pd.DataFrame:
            f_df.Dataframe.Cast.columns_regex_to_int64(df, "^(web_|and_|ios_)(?!.*_percentage_diff$)")
            f_df.Dataframe.Cast.columns_regex_to_float64(df, "^(web_|and_|ios_).*(?<=_percentage_diff)$")
            f_df.Dataframe.Cast.columns_to_datetime(df, "date", "%Y-%m-%d")
            return df

        def get_columns() -> List[str]:
            columns = ["date"]
            for platform in App.PLATFORMS:
                for metric in App.COLS:
                    columns.extend(
                        [f"{platform}_{metric}_aa", f"{platform}_{metric}_ga", f"{platform}_{metric}_percentage_diff"]
                    )
            return columns

        df_ga = load_csv("benchmark_ga.csv")
        df_aa = load_csv("benchmark_adobe.csv")

        df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_aa, df_ga, "date", "outer", ("_aa", "_ga"))
        df = df.pipe(f_df.Dataframe.Fill.nan, value=0)

        for platform in self.PLATFORMS:
            for metric in self.COLS:
                col = f"{platform}_{metric}"
                aa_col, ga_col = f"{col}_aa", f"{col}_ga"
                df[f"{col}_diff"] = df[aa_col] - df[ga_col]
                df[f"{col}_percentage_diff"] = (df[aa_col] - df[ga_col]) / df[ga_col] * 100

        df = (
            df.pipe(f_df.Dataframe.Fill.nan, value=0)
            .pipe(f_df.Dataframe.Fill.infinity, value=100)
            .pipe(cast_columns)
            .pipe(f_df.Dataframe.Sort.sort_by_columns, columns="date", ascending=False)
        )

        self.log.print("App.merge_adobe_google", "dataframes merged successfully!")

        return df[get_columns()]

    def save_csv(self):
        file_path = os.path.join(self.file_directory, "csv", "benchmark.csv")
        f_csv.CSV.dataframe_to_csv(file_path, df)
        self.log.print("App.save_csv", f"CSV file saved to {file_path}")

    def save_spreadsheet(self) -> str:
        def format_sheet(rows: int, cols: int) -> List:
            requests = []

            # headers
            requests.append(
                gsheet.format_worksheet(
                    sheet_id=sheet_id,
                    range="A1:J1",
                    bg_color="#BDBDBD",
                    fontsize=10,
                    bold=True,
                )
            )

            # format Numbers
            for range in [f"B2:C{rows}", f"E2:F{rows}", f"H2:I{rows}"]:
                requests.append(
                    gsheet.format_worksheet(
                        sheet_id=sheet_id,
                        range=range,
                        number_pattern="#,##0",
                    )
                )

            # borders
            for range in [f"B1:D{rows}", f"E1:G{rows}", f"H1:J{rows}"]:
                requests.append(gsheet.format_worksheet_borders(sheet_id=sheet_id, range=range))

            # frozen
            requests.append(gsheet.format_worksheet_frozen(sheet_id=sheet_id, rows=1, cols=1))

            # add formatting
            gsheet.batch_update(spreadsheet_id=gsheet.spreadsheet["id"], requests=requests)

        gsheet = gsheet_api.GoogleSheetsAPI(self.config)
        gsheet.create_spreadsheet(self.title)

        # Define color code based on site
        color_codes = {
            "cnet": "#ff0000",  # Red
            "ma": "#00ff00",  # Green
            "fc": "#0000ff",  # Blue
            "ij": "#3d85c6",  # Blue lighter
        }
        color_code = color_codes.get(self.args["site"], "#ffffff")  # Default to white if site not recognized

        # Define column templates
        columns = "date,{{platform}}_visits_aa,{{platform}}_visits_ga,{{platform}}_visits_percentage_diff,{{platform}}_visitors_aa,{{platform}}_visitors_ga,{{platform}}_visitors_percentage_diff,{{platform}}_views_aa,{{platform}}_views_ga,{{platform}}_views_percentage_diff"
        columns_min = "date,visits_adobe,visits_google,visits_gap,visitors_adobe,visitors_google,visitors_gap,views_adobe,views_google,views_gap"

        # Create worksheets for each platform
        date_str = dtf.DatetimeFormatter.datetime_to_str(df["date"].dt, "%Y-%m-%d")
        for platform in self.PLATFORMS:
            platform_columns = columns.replace("{{platform}}", platform).split(",")
            # create a copy of the platform (df._is_view = False)
            df_platform = df[platform_columns].copy()
            df_platform.columns = columns_min.split(",")
            df_platform["date"] = date_str

            # add sheet to the spreadsheet
            app_version = self.app_version.get(platform, None)
            title = f"{platform.upper()} ({app_version})" if app_version else platform.upper()
            sheet_id = gsheet.add_sheet(gsheet.spreadsheet["id"], df=df_platform, title=title, tab_color=color_code)
            format_sheet(rows=df_platform.shape[0] + 1, cols=df_platform.shape[1])

        # Delete sheet1
        gsheet.delete_sheet1(gsheet.spreadsheet["id"])

        self.log.print("App.save_spreadsheet", f"url: {gsheet.spreadsheet["url"]}")
        self.log.print("App.save_spreadsheet", "spreadsheet saved succesfully!")
        return gsheet.spreadsheet["url"]

    def load_df_agg(self) -> pd.DataFrame:
        def aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
            df_combined = pd.DataFrame()
            for platform in self.PLATFORMS:
                for metric in self.COLS:
                    col = f"{platform}_{metric}"
                    aa_col, ga_col = f"{col}_aa", f"{col}_ga"
                    stats_summary = df[[aa_col, ga_col, f"{col}_percentage_diff"]].agg(
                        ["sum", "min", "max", "mean", "std"]
                    )
                    stats_traspose = stats_summary.T
                    f_df.Dataframe.Columns.update_column_by_row_index(
                        df=stats_traspose,
                        row_index=f"{col}_percentage_diff",
                        col_name="sum",
                        value=(df[aa_col].sum() - df[ga_col].sum()) / df[ga_col].sum() * 100,
                    )
                    df_combined = f_df.Dataframe.Rows.concat_frames(
                        frames=[df_combined, stats_traspose], ignore_index=False
                    )
            return df_combined

        df_agg_last_day = df_agg_last_7_days = df_agg_last_month = None

        # Filter last day
        df_last_day = df[df["date"] == self.to_date]
        if not f_df.Dataframe.is_empty(df_last_day):
            df_agg_last_day = aggregate_data(df_last_day)

        # filter last 7 days
        df_last_7_days = df[df["date"] >= dtf.DatetimeFormatter.datetime_add_days(value=self.to_date, days=-7)]
        if not f_df.Dataframe.is_empty(df_last_7_days):
            df_agg_last_7_days = aggregate_data(df_last_7_days)

        # filter last month
        df_last_month = df[df["date"] >= dtf.DatetimeFormatter.datetime_add_months(value=self.to_date, months=-1)]
        if not f_df.Dataframe.is_empty(df_last_month):
            df_agg_last_month = aggregate_data(df_last_month)

        if df_agg_last_day is None or df_agg_last_7_days is None or df_last_month is None:
            self.log.print_error("App.load_df_agg", "Dataframe agrregation is empty")
        return df_agg_last_day, df_agg_last_7_days, df_agg_last_month

    def save_plot(self):
        for platform in self.PLATFORMS:
            fig, axes = plt.subplots(1, 3, figsize=(15, 5))
            for i, metric in enumerate(self.COLS):
                # add plot
                col = f"{platform}_{metric}"
                ax: Axes = axes[i]
                # axes[i].scatter(
                #     df["date"], df[f"{col}_aa"], color="blue", label=f"Adobe {metric.capitalize()}", marker="o"
                # )
                ax.plot(
                    df["date"],
                    df[f"{col}_aa"],
                    color="blue",
                    linewidth=1,
                    label=f"Adobe {metric.capitalize()}",
                )
                # axes[i].scatter(df["date"], df[f"{col}_ga"], color="red", label=f"GA {metric.capitalize()}", marker="x")
                ax.plot(
                    df["date"],
                    df[f"{col}_ga"],
                    color="red",
                    linewidth=1,
                    label=f"Google {metric.capitalize()}",
                )
                # add line mean last 7 days
                mean_aa = f_df.Dataframe.Get.get_column_by_row_index(
                    df=df_agg_last_7_days, row_index=f"{col}_aa", col_name="mean"
                )
                mean_ga = f_df.Dataframe.Get.get_column_by_row_index(
                    df=df_agg_last_7_days, row_index=f"{col}_ga", col_name="mean"
                )
                ax.axhline(y=mean_aa, color="blue", linestyle="--")
                ax.axhline(y=mean_ga, color="red", linestyle="--")

                # add mean values as text on the plot
                label_aa = nf.NumberFormatter.format_number(value=mean_aa, large_number_formatter=True)
                label_ga = nf.NumberFormatter.format_number(value=mean_ga, large_number_formatter=True)
                ax.text(
                    x=df["date"].iloc[-1],
                    y=mean_aa,
                    s=f"Mean (last 7 days): {label_aa}",
                    color="white",
                    va="top",
                    bbox=dict(facecolor="blue", alpha=0.9),
                )
                ax.text(
                    x=df["date"].iloc[-1],
                    y=mean_ga,
                    s=f"Mean (last 7 days): {label_ga}",
                    color="white",
                    va="top",
                    bbox=dict(facecolor="red", alpha=0.9),
                )

                # Add lines for mean ± standard deviation
                # std = f_df.Dataframe.Get.get_column_by_row_index(df=df_agg, row_index=f"{col}_aa", col_name="std")
                # label = nf.NumberFormatter.format_number(mean + std)
                # axes[i].axhline(mean + std, color="green", linestyle="--", label=f"Mean + Std Dev: {label}")
                # label = nf.NumberFormatter.format_number(mean - std)
                # axes[i].axhline(mean - std, color="green", linestyle="--", label=f"Mean - Std Dev: {label}")

                ax.set_title(f"Comparison of Daily {metric.capitalize()}")
                ax.set_xlabel("Date")
                ax.set_ylabel(f"Number of {metric.capitalize()}")
                ax.legend()
                ax.grid(True)
                ax.tick_params(axis="x", rotation=45)

                # Format the y-axis with human-readable numbers
                ax.yaxis.set_major_formatter(
                    ticker.FuncFormatter(
                        lambda x, pos: nf.NumberFormatter.format_number(value=x, large_number_formatter=True)
                    )
                )

            plt.tight_layout()
            # plt.show()

            # Save the plot
            file_path = os.path.join(self.file_directory, f"img/benchmark_{platform}.png")
            f_plot.Plot.plot_to_file(file_path=file_path, plot=plt)
            self.log.print("App.save_plot", f"image saved to {file_path}")

    def send_email(self):
        email = gmail_smtplib_api.GmailSmtplibAPI(self.config)
        image_paths = [
            os.path.join(self.file_directory, "img", "benchmark_web.png"),
            os.path.join(self.file_directory, "img", "benchmark_and.png"),
            os.path.join(self.file_directory, "img", "benchmark_ios.png"),
        ]
        html_file_path = os.path.join(self.file_directory, "report/benchmark.html")
        html_body = f_files.File.read_file(html_file_path)
        html_body = html_body.replace("[[title]]", self.title)
        data_range = f"(últimos {self.date_diff_days} días, del {dtf.DatetimeFormatter.datetime_to_day_month_year(self.from_date, locale="es_ES.UTF-8")} al {dtf.DatetimeFormatter.datetime_to_day_month_year(self.to_date, locale="es_ES.UTF-8")})"
        html_body = html_body.replace("[[data_range]]", data_range)
        html_body = html_body.replace("[[spreadsheet_url]]", spreadsheet_url)
        for platform in self.PLATFORMS:
            if platform in ("and", "ios"):
                app_version = self.app_version.get(platform, None)
                if app_version:
                    html_body = html_body.replace(f"[[{platform}_app_version]]", f" ({app_version})")
                else:
                    html_body = html_body.replace(f"[[{platform}_app_version]]", "")
            for metric in self.COLS:
                col = f"{platform}_{metric}"
                # gap last day
                gap = f_df.Dataframe.Get.get_column_by_row_index(
                    df=df_agg_last_day, row_index=f"{col}_percentage_diff", col_name="sum"
                )
                gap_str = f"{abs(gap):.2f}% a favor de {'Adobe' if gap > 0 else 'Google'}"
                html_body = html_body.replace(f"[[{col}_percentage_sum_diff_last_day]]", gap_str)
                # gap last 7 days
                gap = f_df.Dataframe.Get.get_column_by_row_index(
                    df=df_agg_last_7_days, row_index=f"{col}_percentage_diff", col_name="sum"
                )
                gap_str = f"{abs(gap):.2f}% a favor de {'Adobe' if gap > 0 else 'Google'}"
                html_body = html_body.replace(f"[[{col}_percentage_sum_diff_last_7_days]]", gap_str)
                # gap last month
                gap = f_df.Dataframe.Get.get_column_by_row_index(
                    df=df_agg_last_month, row_index=f"{col}_percentage_diff", col_name="sum"
                )
                gap_str = f"{abs(gap):.2f}% a favor de {'Adobe' if gap > 0 else 'Google'}"
                html_body = html_body.replace(f"[[{col}_percentage_sum_diff_last_month]]", gap_str)
        email.send_email(email_to="lsalamo@gmail.com", subject=self.title, image_paths=image_paths, html_body=html_body)
        # Print success message
        self.log.print("App.send_email", "email sent successfully!")

    def print_end(self):
        self.log.print("App.main", "application finished!")
        self.log.print_line()


if __name__ == "__main__":
    try:
        app = App()
        df = app.merge_adobe_google()
        app.save_csv()
        spreadsheet_url = app.save_spreadsheet()
        df_agg_last_day, df_agg_last_7_days, df_agg_last_month = app.load_df_agg()
        if df_agg_last_day is not None and df_agg_last_day is not None and df_agg_last_month is not None:
            app.save_plot()
            app.send_email()
        app.print_end()
    except Exception as e:
        print(f"App.main: An error occurred: {str(e)}")
        sys.exit(1)
