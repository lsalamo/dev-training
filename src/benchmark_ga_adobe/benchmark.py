import sys
import os
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from colorama import init, Fore, Style  # https://patorjk.com/software/taag/#p=display&f=Doom&t=Benchmark

# adding libraries folder to the system path
from libs import log as f_log, dataframe as f_df, csv as f_csv, number_formatter as f_number_formatter
from libs.google import gsheet_api


class App:
    COLS = ["visits", "visitors", "views"]

    def __init__(self, platform):
        self.platform = platform
        self.kpis = {}

        # logging
        self._log_init_info()

    def _log_init_info(self):
        # init(autoreset=True)

        # Header
        log.print_header(
            """
▗▄▄▖ ▗▄▄▄▖▗▖  ▗▖ ▗▄▄▖▗▖ ▗▖▗▖  ▗▖ ▗▄▖ ▗▄▄▖ ▗▖ ▗▖
▐▌ ▐▌▐▌   ▐▛▚▖▐▌▐▌   ▐▌ ▐▌▐▛▚▞▜▌▐▌ ▐▌▐▌ ▐▌▐▌▗▞▘
▐▛▀▚▖▐▛▀▀▘▐▌ ▝▜▌▐▌   ▐▛▀▜▌▐▌  ▐▌▐▛▀▜▌▐▛▀▚▖▐▛▚▖ 
▐▙▄▞▘▐▙▄▄▖▐▌  ▐▌▝▚▄▄▖▐▌ ▐▌▐▌  ▐▌▐▌ ▐▌▐▌ ▐▌▐▌ ▐▌
        """
        )

        # Archivo y parámetros
        script_name = sys.argv[0]
        arguments = sys.argv[1:]

        # Mostrar el encabezado
        log.print_params("Name of python script:", script_name)
        # print(f"{Fore.LIGHTBLUE_EX}{Style.BRIGHT}Name of Python script: {Style.NORMAL}{Fore.CYAN}{script_name}")
        # print(f"{Fore.LIGHTBLUE_EX}{Style.BRIGHT}Current working directory: {Fore.LIGHTGREEN_EX}{os.getcwd()}")
        # print(f"{Fore.LIGHTBLUE_EX}{Style.BRIGHT}Python search paths: {Fore.LIGHTGREEN_EX}{sys.path}")

        log.print_params("Arguments:", "")
        if arguments:
            for i, arg in enumerate(arguments, start=1):
                log.print_params(f"  {i}.", f"{arg}")
        else:
            log.print_error(f"No se proporcionaron parámetros.")

        # Línea final decorativa
        log.print_line()

    def load_csv(self, file):
        file_path = os.path.join(os.getcwd(), file)
        df = f_csv.CSV.csv_to_dataframe(file_path)
        log.print("App.load_csv", f"dataframe loaded from file {file_path}")
        return df

    def merge_adobe_google(self, df_aa, df_ga):
        # merge
        df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_aa, df_ga, "date", "outer", ("-aa", "-ga"))
        # cast
        df = df.fillna(0)
        f_df.Dataframe.Cast.columns_regex_to_int64(df, "^(web-|android-|ios-)")
        f_df.Dataframe.Cast.columns_to_datetime(df, "date", "%Y-%m-%d")

        # columns
        columns = f"date,{self.platform}_visits_aa,{self.platform}_visits_ga,{self.platform}_visitors_aa,{self.platform}_visitors_ga,{self.platform}_views_aa,{self.platform}_views_ga"
        df.columns = columns.split(",")

        # log
        log.print("App.merge_adobe_google", "completed")
        return df

    def set_kpis(self, df):
        formatter = f_number_formatter.NumberFormatter(decimal_places=2)
        for metric in self.COLS:
            col = f"{self.platform}_{metric}"
            df[f"{col}_diff"] = df[f"{col}_aa"] - df[f"{col}_ga"]
            df[f"{col}_percentage_diff"] = (df[f"{col}_aa"] - df[f"{col}_ga"]) / df[f"{col}_ga"] * 100
            f_df.Dataframe.Cast.columns_to_float64(df, f"{col}_percentage_diff", 2)

            stats_summary = df[[f"{col}_diff", f"{col}_percentage_diff"]].agg(["mean", "std", "min", "max"])
            print(stats_summary)
            # stats_summary[f"{col}_diff"]["mean"]

            self.kpis[f"{col}_aa_sum"] = df[f"{col}_aa"].sum()
            self.kpis[f"{col}_ga_sum"] = df[f"{col}_ga"].sum()
            self.kpis[f"{col}_aa_mean"] = df[f"{col}_aa"].mean()
            self.kpis[f"{col}_ga_mean"] = df[f"{col}_ga"].mean()
            self.kpis[f"{col}_aa_std"] = df[f"{col}_aa"].std()
            self.kpis[f"{col}_ga_std"] = df[f"{col}_ga"].std()
            self.kpis[f"{col}_percentage_diff_min"] = df[f"{col}_percentage_diff"].min()
            self.kpis[f"{col}_percentage_diff_max"] = df[f"{col}_percentage_diff"].max()
            self.kpis[f"{col}_mean_diff"] = (df[f"{col}_aa"] - df[f"{col}_ga"]).mean()
            self.kpis[f"{col}_median_diff"] = (df[f"{col}_aa"] - df[f"{col}_ga"]).median()
            self.kpis[f"{col}_std_diff"] = (df[f"{col}_aa"] - df[f"{col}_ga"]).std()
            self.kpis[f"{col}_percentage_sum_diff"] = (
                (df[f"{col}_aa"].sum() - df[f"{col}_ga"].sum()) / df[f"{col}_ga"].sum() * 100
            )
            self.kpis[f"{col}_percentage_mean_diff"] = (
                (df[f"{col}_aa"].mean() - df[f"{col}_ga"].mean()) / df[f"{col}_ga"].mean() * 100
            )
            self.kpis[f"{col}_threshold"] = df[f"{col}_diff"].mean() + 2 * df[f"{col}_diff"].std()

            # print results
            log.print("App.set_kpis", f"Total:")
            log.print("App.set_kpis", f"{col}_aa: {formatter.format_number(self.kpis[f"{col}_aa_sum"])}")
            log.print("App.set_kpis", f"{col}_ga: {formatter.format_number(self.kpis[f"{col}_ga_sum"])}")
            log.print(
                "App.set_kpis",
                f"AA are about {formatter.format_percentage(self.kpis[f"{col}_percentage_sum_diff"])} higher than GA",
            )

            log.print("\nApp.set_kpis", f"Average Daily:")
            log.print("App.set_kpis", f"{col}_aa: {formatter.format_number(self.kpis[f"{col}_aa_mean"])}")
            log.print("App.set_kpis", f"{col}_ga: {formatter.format_number(self.kpis[f"{col}_ga_mean"])}")
            log.print(
                "App.set_kpis",
                f"AA are about {formatter.format_percentage(self.kpis[f"{col}_percentage_mean_diff"])} higher than GA",
            )

            log.print("\nApp.set_kpis", f"Day-to_Day Comparison {col}:")
            log.print(
                "App.set_kpis",
                f"The difference ranges from {formatter.format_percentage(self.kpis[f"{col}_percentage_diff_min"])} to {formatter.format_percentage(self.kpis[f"{col}_percentage_diff_max"])} higher for AA.",
            )
            log.print_line()

    def show_plot(self, df):
        # fig, axes = plt.subplots(1, 3, figsize=(20, 15))
        fig, axes = plt.subplots(1, 3, figsize=(10, 5))
        for i, metric in enumerate(self.COLS):
            # Visualizations
            col = f"{self.platform}_{metric}"
            axes[i].scatter(df["date"], df[f"{col}_aa"], color="blue", label=f"Data points AA {metric.capitalize()}")
            axes[i].plot(df["date"], df[f"{col}_aa"], color="blue")
            axes[i].scatter(df["date"], df[f"{col}_ga"], color="red", label=f"Data points GA {metric.capitalize()}")
            axes[i].plot(df["date"], df[f"{col}_ga"], color="red")

            # Add lines for mean
            mean = self.kpis[f"{col}_aa_mean"]
            axes[i].axhline(y=mean, color="green", linestyle="--", label=f"Mean AA (μ = {mean:.2f})")

            # Add lines for mean ± standard deviation
            std = self.kpis[f"{col}_aa_std"]
            axes[i].axhline(
                mean + std, color="green", linestyle="--", label=f"Mean + Std Dev (μ + σ): {mean + std:.2f}"
            )
            axes[i].axhline(
                mean - std, color="green", linestyle="--", label=f"Mean - Std Dev (μ - σ): {mean - std:.2f}"
            )

            axes[i].set_title(f"Comparison of Daily {metric.capitalize()}")
            axes[i].set_xlabel("Date")
            axes[i].set_ylabel(f"Number of {metric.capitalize()}")
            axes[i].legend()
            axes[i].grid(True)
            axes[i].tick_params(axis="x", rotation=45)

        plt.tight_layout()
        plt.show()

    def analyze_data(self, df):
        # Detect anomalies
        for metric in ["visits", "visitors", "views"]:
            diff_col = f"{metric}_diff"
            threshold = df[diff_col].mean() + 2 * df[diff_col].std()
            anomalies = df[np.abs(df[diff_col]) > threshold]
            print(f"\nAnomalies in {metric}:")
            print(anomalies[["date", f"{platform}-{metric}-aa", f"{platform}-{metric}-ga", diff_col]])

        # Correlation heatmap
        correlation_matrix = df[
            [
                f"{platform}-visits-aa",
                f"{platform}-visits-ga",
                f"{platform}-visitors-aa",
                f"{platform}-visitors-ga",
                f"{platform}-views-aa",
                f"{platform}-views-ga",
            ]
        ].corr()

        plt.figure(figsize=(10, 8))
        sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", vmin=-1, vmax=1, center=0)
        plt.title("Correlation Heatmap")
        plt.show()


if __name__ == "__main__":
    # Logging
    log = f_log.Log()

    platform = sys.argv[2]

    app = App(platform)

    # merge
    df_ga = app.load_csv("src/benchmark_ga_adobe/csv/benchmark_ga.csv")
    df_aa = app.load_csv("src/benchmark_ga_adobe/csv/benchmark_adobe.csv")
    df = app.merge_adobe_google(df_ga, df_aa)
    df = f_df.Dataframe.Sort.sort_by_columns(df, columns="date", ascending=False)
    log.print_line()

    # analyze data
    app.set_kpis(df)
    app.show_plot(df)

    gsheet_api.GsheetAPI()

    # csv
    csv = f_csv.CSV(__file__)
    csv.dataframe_to_csv(df)

    log.print("App.main", "completed")
