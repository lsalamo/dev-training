import sys
import os
import matplotlib.pyplot as plt
import pandas as pd
from typing import Dict

# adding libraries folder to the system path
from libs import (
    dataframe as f_df,
    csv as f_csv,
    plot as f_plot,
    number_formatter as nf,
    datetime_formatter as f_datetime_formatter,
    files as f_files,
    libs_base,
)
from libs.google import gmail_smtplib_api, gsheet_api


class App(libs_base.LibsBase):
    PLATFORMS = ["web", "and", "ios"]
    COLS = ["visits", "visitors", "views"]

    def __init__(self):
        super().__init__(__file__)

        # arguments
        self.args = self._parse_arguments()

        # configuration
        self.config = self.load_config()

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
        # load csv files
        df_ga = self._load_csv("benchmark_ga.csv")
        df_aa = self._load_csv("benchmark_adobe.csv")

        # merge dataframes
        df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_aa, df_ga, "date", "outer", ("_aa", "_ga"))

        # process dataframe
        df = f_df.Dataframe.Sort.sort_by_columns(df, columns="date", ascending=False)
        df.pipe

        # cast
        df = df.sort_values("date", ascending=False).fillna(0).pipe(self._cast_columns)

        # log
        self.log.print("App.merge_adobe_google", "dataframes merged successfully!")
        return df

    def _load_csv(self, filename: str) -> pd.DataFrame:
        file_path = os.path.join(self.file_directory, "csv", filename)
        df = f_csv.CSV.csv_to_dataframe(file_path)
        self.log.print("App.merge_adobe_google", f"dataframe loaded from file {file_path}")
        return df

    def _cast_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        f_df.Dataframe.Cast.columns_regex_to_int64(df, "^(web_|and_|ios_)")
        f_df.Dataframe.Cast.columns_to_datetime(df, "date", "%Y-%m-%d")
        return df

    def save_csv(self, df: pd.DataFrame):
        file_path = os.path.join(self.file_directory, "csv", "benchmark.csv")
        f_csv.CSV.dataframe_to_csv(file_path, df)
        self.log.print("App.save_csv", f"CSV file saved to {file_path}")

    def save_spreadsheet(self, df):
        gsheet = gsheet_api.GsheetAPI(__file__)
        spreadsheet = gsheet.delete_and_create_spreadsheet(
            f"Benchmark AdobeGA - {f_datetime_formatter.DatetimeFormatter.today_to_str()}"
        )
        gsheet.share_spreadsheet(spreadsheet, "lsalamo@gmail.com")

        # update worksheet
        site = self.args["site"]
        if site == "cnet":
            color_code = "#ff0000"  # Red
        elif site == "ma":
            color_code = "#00ff00"  # Green
        elif site == "fc":
            color_code = "#0000ff"  # Blue
        elif site == "ij":
            color_code = "#3d85c6"  # Blue lighter
        else:
            color_code = "#ffffff"  # Default to white if site is not recognized
        gsheet.update_worksheet(spreadsheet, df, site, color_code)

    def get_kpis(self, df):
        kpis = {}
        for platform in self.PLATFORMS:
            for metric in self.COLS:
                col = f"{platform}_{metric}"
                aa_col, ga_col = f"{col}_aa", f"{col}_ga"

                # update kpis
                df[f"{col}_diff"] = df[aa_col] - df[ga_col]
                df[f"{col}_percentage_diff"] = (df[aa_col] - df[ga_col]) / df[ga_col] * 100
                stats_summary = df[[aa_col, ga_col, f"{col}_diff", f"{col}_percentage_diff"]].agg(
                    ["sum", "min", "max", "mean", "std"]
                )
                print(stats_summary)
                # stats_summary[f"{col}_diff"]["mean"]
                kpis.update(
                    {
                        f"{col}_aa_sum": df[aa_col].sum(),  # stats_summary[aa_col]["sum"]
                        f"{col}_ga_sum": df[ga_col].sum(),  # stats_summary[ga_col]["sum"]
                        f"{col}_percentage_sum_diff": (df[aa_col].sum() - df[ga_col].sum()) / df[ga_col].sum() * 100,
                        f"{col}_aa_mean": df[aa_col].mean(),
                        f"{col}_ga_mean": df[ga_col].mean(),
                        f"{col}_percentage_mean_diff": (df[aa_col].mean() - df[ga_col].mean())
                        / df[ga_col].mean()
                        * 100,
                        f"{col}_aa_std": df[aa_col].std(),
                        f"{col}_ga_std": df[ga_col].std(),
                        f"{col}_percentage_diff_min": df[f"{col}_percentage_diff"].min(),
                        f"{col}_percentage_diff_max": df[f"{col}_percentage_diff"].max(),
                        f"{col}_mean_diff": df[f"{col}_diff"].mean(),
                        f"{col}_median_diff": df[f"{col}_diff"].median(),
                        f"{col}_std_diff": df[f"{col}_diff"].std(),
                        f"{col}_threshold": df[f"{col}_diff"].mean() + 2 * df[f"{col}_diff"].std(),
                    }
                )

                # log.print("\nApp.set_kpis", f"Day-to_Day Comparison {col}:")
                # log.print(
                #     "App.set_kpis",
                #     f"The difference ranges from {formatter.format_percentage(kpis[f"{col}_percentage_diff_min"])} to {formatter.format_percentage(kpis[f"{col}_percentage_diff_max"])} higher for AA.",
                # )
                # self.log.print_line()
        return kpis

    def save_plot(self, df, kpis: Dict[str, str]):
        for platform in self.PLATFORMS:
            # fig, axes = plt.subplots(1, 3, figsize=(20, 15))
            fig, axes = plt.subplots(1, 3, figsize=(15, 5))
            for i, metric in enumerate(self.COLS):
                # Visualizations
                col = f"{platform}_{metric}"
                axes[i].scatter(df["date"], df[f"{col}_aa"], color="blue", label=f"Adobe {metric.capitalize()}")
                axes[i].plot(df["date"], df[f"{col}_aa"], color="blue")
                axes[i].scatter(df["date"], df[f"{col}_ga"], color="red", label=f"GA {metric.capitalize()}")
                axes[i].plot(df["date"], df[f"{col}_ga"], color="red")

                # Add lines for mean
                mean = kpis[f"{col}_aa_mean"]
                label = nf.NumberFormatter.format_number(mean)
                axes[i].axhline(y=mean, color="green", linestyle="--", label=f"Mean AA: {label})")

                # Add lines for mean ± standard deviation
                std = kpis[f"{col}_aa_std"]
                label = nf.NumberFormatter.format_number(mean + std)
                axes[i].axhline(mean + std, color="green", linestyle="--", label=f"Mean + Std Dev: {label}")
                label = nf.NumberFormatter.format_number(mean - std)
                axes[i].axhline(mean - std, color="green", linestyle="--", label=f"Mean - Std Dev: {label}")

                axes[i].set_title(f"Comparison of Daily {metric.capitalize()}")
                axes[i].set_xlabel("Date")
                axes[i].set_ylabel(f"Number of {metric.capitalize()}")
                axes[i].legend()
                axes[i].grid(True)
                axes[i].tick_params(axis="x", rotation=45)

            plt.tight_layout()
            # plt.show()

            # Save the plot
            file_path = os.path.join(self.file_directory, f"img/benchmark_{platform}.png")
            f_plot.Plot.plot_to_file(file_path=file_path, plot=plt)
            self.log.print("App.save_plot", f"image saved to {file_path}")

    def send_email(self, kpis: Dict[str, str]):
        email = gmail_smtplib_api.GmailSmtplibAPI(self.config)
        subject = title

        image_paths = [
            os.path.join(self.file_directory, "img", "benchmark_web.png"),
            os.path.join(self.file_directory, "img", "benchmark_and.png"),
            os.path.join(self.file_directory, "img", "benchmark_ios.png"),
        ]
        html_file_path = os.path.join(self.file_directory, "report/benchmark.html")
        html_body = f_files.File.read_file(html_file_path)

        for platform in self.PLATFORMS:
            for metric in self.COLS:
                col = f"{platform}_{metric}"
                html_body = html_body.replace(
                    f"[[{col}_aa_sum]]", nf.NumberFormatter.format_number(kpis[f"{col}_aa_sum"])
                )
                html_body = html_body.replace(
                    f"[[{col}_ga_sum]]", nf.NumberFormatter.format_number(kpis[f"{col}_ga_sum"])
                )
                html_body = html_body.replace(
                    f"[[{col}_percentage_sum_diff]]",
                    nf.NumberFormatter.format_percentage(kpis[f"{col}_percentage_sum_diff"]),
                )
                html_body = html_body.replace(
                    f"[[{col}_aa_mean]]", nf.NumberFormatter.format_number(kpis[f"{col}_aa_mean"])
                )
                html_body = html_body.replace(
                    f"[[{col}_ga_mean]]", nf.NumberFormatter.format_number(kpis[f"{col}_ga_mean"])
                )
                html_body = html_body.replace(
                    f"[[{col}_percentage_mean_diff]]",
                    nf.NumberFormatter.format_percentage(kpis[f"{col}_percentage_mean_diff"]),
                )
        email.send_email("lsalamo@gmail.com", subject, image_paths, html_body)
        # Print success message
        self.log.print("App.send_email", "email sent successfully!")

    def print_end(self):
        self.log.print("App.main", "application finished!")
        self.log.print_line()


if __name__ == "__main__":
    try:
        title = f"Benchmark AdobeGA - {f_datetime_formatter.DatetimeFormatter.today_to_str()}"
        app = App()
        df = app.merge_adobe_google()
        app.save_csv(df)
        # app.save_spreadsheet(df)
        kpis = app.get_kpis(df)
        app.save_plot(df, kpis)
        app.send_email(kpis)
        app.print_end()
    except Exception as e:
        print(f"App.main: An error occurred: {str(e)}")
        sys.exit(1)
