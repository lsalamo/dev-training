import sys
import os
from colorama import init, Fore, Style  # https://patorjk.com/software/taag/#p=display&f=Doom&t=Benchmark

# adding libraries folder to the system path
from libs import (
    log as f_log,
    dataframe as f_df,
    csv as f_csv,
)


class App:
    def __init__(self, platform):
        self.platform = platform

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
