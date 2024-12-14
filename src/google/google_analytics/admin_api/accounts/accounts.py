import os
from libs import (
    json as f_json,
    dataframe as f_df,
)
from libs.google import admin_api as api_google

if __name__ == "__main__":
    # configuration
    file_config = os.path.join(os.getcwd(), "src/google/config.json")
    config = f_json.JSON.load_json(file_config)
    config["google"]["__file__"] = __file__

    google = api_google.AdminAPI(config)
    df = google.list_accounts()
    if not f_df.Dataframe.is_empty(df):
        # csv
        google.save_csv(df)

    print(df)
