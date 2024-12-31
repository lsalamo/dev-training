import sys
import os
from typing import Dict

# adding libraries folder to the system path
from libs import (
    log as f_log,
    json as f_json,
    dataframe as f_df,
)
from libs.google import admin_api as api_google


class App:
    def __init__(self):
        # logging
        self._log_init_info()

        # configuration
        self.config = self._load_config()

        # client
        self.google = api_google.AdminAPI(self.config)

    def _log_init_info(self):
        log.print_header(
            """
 ▗▄▄▖ ▗▄▖  ▗▄▖  ▗▄▄▖▗▖   ▗▄▄▄▖     ▗▄▖ ▗▄▄▄ ▗▖  ▗▖▗▄▄▄▖▗▖  ▗▖     ▗▄▖ ▗▄▄▖▗▄▄▄▖
▐▌   ▐▌ ▐▌▐▌ ▐▌▐▌   ▐▌   ▐▌       ▐▌ ▐▌▐▌  █▐▛▚▞▜▌  █  ▐▛▚▖▐▌    ▐▌ ▐▌▐▌ ▐▌ █  
▐▌▝▜▌▐▌ ▐▌▐▌ ▐▌▐▌▝▜▌▐▌   ▐▛▀▀▘    ▐▛▀▜▌▐▌  █▐▌  ▐▌  █  ▐▌ ▝▜▌    ▐▛▀▜▌▐▛▀▘  █  
▝▚▄▞▘▝▚▄▞▘▝▚▄▞▘▝▚▄▞▘▐▙▄▄▖▐▙▄▄▖    ▐▌ ▐▌▐▙▄▄▀▐▌  ▐▌▗▄█▄▖▐▌  ▐▌    ▐▌ ▐▌▐▌  ▗▄█▄▖
        """
        )

        # log.print("init", f"Current working directory: {os.getcwd()}")
        # log.print("init", f"Python search paths: {sys.path}")
        log.print_params("Name of python script:", sys.argv[0])
        # log.print("init", f"Total arguments passed: {len(sys.argv)}")
        log.print_params("Arguments:", "")
        arguments = sys.argv[1:]
        if arguments:
            for i, arg in enumerate(arguments, start=1):
                log.print_params(f"  {i}.", f"{arg}")
        else:
            log.print_error(f"No se proporcionaron parámetros.")
        log.print_line()

    def _load_config(self) -> Dict[str, str]:
        file_config = os.path.join(os.getcwd(), "src/google/_credentials/config.json")
        config = f_json.JSON.load_json(file_config)
        config["google"].update({"__file__": __file__})
        return config

    def list_accounts(self):
        df = self.google.list_accounts()
        if not f_df.Dataframe.is_empty(df):
            self.google.save_csv(df)


if __name__ == "__main__":
    try:
        log = f_log.Log()
        app = App()
        df = app.list_accounts()
        log.print("App.main", "completed")
    except Exception as e:
        log.print("App.main", f"An error occurred: {str(e)}")
        sys.exit(1)
