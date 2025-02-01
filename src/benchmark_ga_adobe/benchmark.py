import sys
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.axes import Axes
import pandas as pd
from typing import Dict

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
from libs.google import gmail_smtplib_api, gsheet_api


class App(libs_base.LibsBase):
    PLATFORMS = ["web", "and", "ios"]
    COLS = ["visits", "visitors", "views"]

    def __init__(self):
        def get_site_name(site: str) -> str:
            match site:
                case "cnet":
                    return "Coches"
                case "mnet":
                    return "Motos"
                case "cnet_pro":
                    return "Coches.net PRO"
                case "ma":
                    return "Milanuncios"
                case "ijes":
                    return "Infojobs ES"
                case "ijit":
                    return "Infojobs IT"
                case "ij_epreselec":
                    return "Infojobs Epreselec"
                case "fc":
                    return "Fotocasa"
                case "hab":
                    return "Habitaclia"
                case "fc_pro":
                    return "Fotocasa PRO"
                case _:
                    return "Unknown site"

        super().__init__(__file__)

        # arguments
        self.args = self._parse_arguments()

        # configuration
        self.config = self.load_config()

        # properties
        self.site_title = get_site_name(self.args["site"]).upper()
        self.from_date = dtf.DatetimeFormatter.str_to_datetime(self.args["from_date"], "%Y-%m-%d")
        self.to_date = dtf.DatetimeFormatter.str_to_datetime(self.args["to_date"], "%Y-%m-%d")
        self.date_diff_days = dtf.DatetimeFormatter.diff_days(self.from_date, self.to_date)
        self.title = f"{self.site_title} Comparison of Adobe vs Google - {dtf.DatetimeFormatter.today_to_str()}"
        # self.title = f"{self.site_title} Comparison of Adobe vs Google - {dtf.DatetimeFormatter.get_current_datetime()}"

        # logging
        self._log_init_info()

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
        def cast_columns(df: pd.DataFrame) -> pd.DataFrame:
            # df.dtypes
            f_df.Dataframe.Cast.columns_regex_to_int64(df, "^(web_|and_|ios_)(?!.*_percentage_diff$)")
            f_df.Dataframe.Cast.columns_regex_to_float64(df, "^(web_|and_|ios_).*(?<=_percentage_diff)$")
            f_df.Dataframe.Cast.columns_to_datetime(df, "date", "%Y-%m-%d")
            return df

        # load csv files
        df_ga = self._load_csv("benchmark_ga.csv")
        df_aa = self._load_csv("benchmark_adobe.csv")

        # merge dataframes
        df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_aa, df_ga, "date", "outer", ("_aa", "_ga"))

        # fill missing values and cast
        df = df.pipe(f_df.Dataframe.Fill.nan, value=0)

        # add columns "{platform}_{metric}_diff" and "{platform}_{metric}_percentage_diff"
        for platform in self.PLATFORMS:
            for metric in self.COLS:
                col = f"{platform}_{metric}"
                aa_col, ga_col = f"{col}_aa", f"{col}_ga"
                df[f"{col}_diff"] = df[aa_col] - df[ga_col]
                df[f"{col}_percentage_diff"] = (df[aa_col] - df[ga_col]) / df[ga_col] * 100

        # fill infinity values
        df = df.pipe(f_df.Dataframe.Fill.infinity, value=100)

        # cast
        df = df.pipe(cast_columns)

        # sort by date
        df = f_df.Dataframe.Sort.sort_by_columns(df, columns="date", ascending=False)

        # log
        self.log.print("App.merge_adobe_google", "dataframes merged successfully!")

        return df[
            [
                "date",
                "web_visits_aa",
                "web_visits_ga",
                "web_visits_percentage_diff",
                "web_visitors_aa",
                "web_visitors_ga",
                "web_visitors_percentage_diff",
                "web_views_aa",
                "web_views_ga",
                "web_views_percentage_diff",
                "and_visits_aa",
                "and_visits_ga",
                "and_visits_percentage_diff",
                "and_visitors_aa",
                "and_visitors_ga",
                "and_visitors_percentage_diff",
                "and_views_aa",
                "and_views_ga",
                "and_views_percentage_diff",
                "ios_visits_aa",
                "ios_visits_ga",
                "ios_visits_percentage_diff",
                "ios_visitors_aa",
                "ios_visitors_ga",
                "ios_visitors_percentage_diff",
                "ios_views_aa",
                "ios_views_ga",
                "ios_views_percentage_diff",
            ]
        ]

    def _load_csv(self, filename: str) -> pd.DataFrame:
        file_path = os.path.join(self.file_directory, "csv", filename)
        df = f_csv.CSV.csv_to_dataframe(file_path)
        self.log.print("App.merge_adobe_google", f"dataframe loaded from file {file_path}")
        return df

    def save_csv(self):
        file_path = os.path.join(self.file_directory, "csv", "benchmark.csv")
        f_csv.CSV.dataframe_to_csv(file_path, df)
        self.log.print("App.save_csv", f"CSV file saved to {file_path}")

    def save_spreadsheet(self) -> str:
        def format_worksheet(gsheet: gsheet_api.GsheetAPI, rows: int, cols: int):
            requests = []

            # headers
            requests.append(
                gsheet.format_worksheet(
                    range="A1:J1",
                    bg_color={"red": 0.8784, "green": 0.8784, "blue": 0.8784},
                    fontsize=10,
                    bold=True,
                )
            )

            # format Numbers
            for range in [f"B2:C{rows}", f"E2:F{rows}", f"H2:I{rows}"]:
                requests.append(
                    gsheet.format_worksheet(
                        range=range,
                        number_pattern="#,##0",
                    )
                )

            # borders
            for range in [f"B1:D{rows}", f"E1:G{rows}", f"H1:J{rows}"]:
                requests.append(gsheet.format_worksheet_borders(range=range))

            # frozen
            requests.append(gsheet.format_worksheet_frozen(rows=1, cols=1))

            # Send the request to apply borders
            gsheet.spreadsheet.batch_update({"requests": requests})

        gsheet = gsheet_api.GsheetAPI(self.config)
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
        columns_min = "date,visits_adobe,visits_google,visits_gap,visitor_adobe,visitor_google,visitor_gap,views_adobe,views_google,views_gap"

        # Create worksheets for each platform
        for platform in ["web", "and", "ios"]:
            platform_columns = columns.replace("{{platform}}", platform).split(",")
            df_platform = df[platform_columns]
            df_platform.columns = columns_min.split(",")
            df_platform["date"] = dtf.DatetimeFormatter.datetime_to_str(df["date"].dt, "%Y-%m-%d")

            gsheet.add_worksheet(gsheet.spreadsheet, df=df_platform, title=platform.upper(), tab_color=color_code)
            format_worksheet(gsheet=gsheet, rows=df_platform.shape[0] + 1, cols=df_platform.shape[1])

        self.log.print("App.save_spreadsheet", f"url: {gsheet.spreadsheet.url}")
        self.log.print("App.save_spreadsheet", "spreadsheet saved succesfully!")

        return gsheet.spreadsheet.url

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

        # full data
        df_agg_full = aggregate_data(df)

        # Filter last day
        df_last_day = df[df["date"] == self.to_date]
        df_agg_last_day = aggregate_data(df_last_day)

        # filter last 7 days
        df_last_7_days = df[df["date"] >= dtf.DatetimeFormatter.datetime_add_days(value=self.to_date, days=-7)]
        df_agg_last_7_days = aggregate_data(df_last_7_days)

        # filter last month
        df_last_month = df[df["date"] >= dtf.DatetimeFormatter.datetime_add_months(value=self.to_date, months=-1)]
        df_agg_last_month = aggregate_data(df_last_month)

        return df_agg_full, df_agg_last_day, df_agg_last_7_days, df_agg_last_month

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
        title = f"{self.site_title} - GAP Adobe vs Google"
        html_body = html_body.replace("[[title]]", title)
        data_range = f"(últimos {self.date_diff_days} días, del {dtf.DatetimeFormatter.datetime_to_day_month_year(self.from_date, locale="es_ES.UTF-8")} al {dtf.DatetimeFormatter.datetime_to_day_month_year(self.to_date, locale="es_ES.UTF-8")})"
        html_body = html_body.replace("[[data_range]]", data_range)
        html_body = html_body.replace("[[spreadsheet_url]]", spreadsheet_url)
        for platform in self.PLATFORMS:
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
        df_agg_full, df_agg_last_day, df_agg_last_7_days, df_agg_last_month = app.load_df_agg()
        app.save_plot()
        app.send_email()
        app.print_end()
    except Exception as e:
        print(f"App.main: An error occurred: {str(e)}")
        sys.exit(1)
