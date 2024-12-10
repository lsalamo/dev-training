import os

# adding libraries folder to the system path
from libs import (
    files as f_files,
    json as f_json,
)
from libs.google import api_google_analytics_rest as api_google

if __name__ == '__main__':
    file_config = os.path.join(os.getcwd(), "src/google/config.json")
    config = f_json.JSON.load_json(file_config) 
    config = config['google']
    config['property'] = 'mnet'
    config['platform'] = 'web'

    google = api_google.GoogleAnalyticsRest(config)
    file_payload = os.path.join(f_files.Directory.get_directory(__file__), 'payload.json')
    df = google.run_report(file_payload)
    print(df)