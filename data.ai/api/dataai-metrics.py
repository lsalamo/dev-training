"""
@author: lsalamo@gmail.com
USAGE:
    python3 api-main-etrics.py -i 2022-08-01 -e 2023-03-01
"""
import pandas as pd
import sys
import os
import argparse
import libraries.functions as f
import libraries.api_dataai_2_0 as api_dataai
import libraries.dataframe as f_df
import libraries.dt as f_dt
import libraries.logs as f_log
import yaml


class App:
    def __init__(self):
        def get_config():
            config_file = os.path.join(os.path.dirname(__file__), '../credentials/config.yaml')
            with open(config_file, 'r') as config_file:
                config = yaml.safe_load(config_file)
            return config

        # vars
        self.log = f_log.Logging()
        self.config = get_config()

        # directory
        self.directory = os.path.dirname(os.path.realpath(__file__))
        f.Directory.set_working_directory(self.directory)
        f.Directory.create_directory('csv')
        self.log.print('init', f'working_directory: {f.Directory.get_working_directory()}')

        # args
        self.log.print('init', 'Total arguments passed: ' + str(len(sys.argv)))
        self.log.print('init', 'Name of Python script: ' + sys.argv[0])
        for i in range(1, len(sys.argv)):
            self.log.print('init', 'Argument: ' + sys.argv[i])

    def request(self):
        try:
            # report_downloads
            api_report_downloads = api_dataai.report_downloads(self.config['token'])
            api_report_downloads.date_from = variables['from_date']
            api_report_downloads.to_date = variables['to_date']

            # ratings_history
            api_ratings_history = api_dataai.ratings_history(self.config['token'])
            api_ratings_history.date_from = variables['from_date']
            api_ratings_history.to_date = variables['to_date']

            # active_users
            api_active_users = api_dataai.report_active_users(self.config['token'])
            api_active_users.date_from = variables['from_date']
            api_active_users.to_date = variables['to_date']

            # dataframe
            df_report_downloads = pd.DataFrame()
            df_ratings_history = pd.DataFrame()
            df_active_users = pd.DataFrame()
            for vertical in self.config['verticals']:
                products_and = ",".join(self.config['verticals'][vertical]['android'].values())
                products_ios = ",".join(self.config['verticals'][vertical]['ios'].values())

                # report_downloads
                self.log.print('request', f'VERTICAL:{vertical}::API:downloads')
                df = api_report_downloads.request(vertical, products_and)
                df_report_downloads = f_df.Dataframe.Rows.concat_two_frames(df_report_downloads, df)
                df = api_report_downloads.request(vertical, products_ios)
                df_report_downloads = f_df.Dataframe.Rows.concat_two_frames(df_report_downloads, df)

                # ratings_history
                self.log.print('request', f'VERTICAL:{vertical}::API:ranting history')
                df = api_ratings_history.request_and(vertical, products_and)
                df_ratings_history = f_df.Dataframe.Rows.concat_two_frames(df_ratings_history, df)
                df = api_ratings_history.request_ios(vertical, products_ios)
                df_ratings_history = f_df.Dataframe.Rows.concat_two_frames(df_ratings_history, df)

                # active_users
                self.log.print('request', f'VERTICAL:{vertical}::API:active users')
                df = api_active_users.request(vertical, products_and)
                df_active_users = f_df.Dataframe.Rows.concat_two_frames(df_active_users, df)
                df = api_active_users.request(vertical, products_ios)
                df_active_users = f_df.Dataframe.Rows.concat_two_frames(df_active_users, df)

            self.log.print('request', 'JOINING DATAFRAMES...')
            df = f_df.Dataframe.Columns.join_by_columns([df_report_downloads, df_ratings_history, df_active_users], ['date', 'vertical', 'platform', 'product_id'], 'outer')
            df['product'] = df['product_x'].fillna(df['product_y'])
            df = df[['date', 'vertical', 'platform', 'product', 'downloads', 'average', 'total_rating', 'rating_five', 'rating_four', 'rating_three', 'rating_two', 'rating_one', 'active_users']]
            df = df[df['product'].notna()]
            df = df.fillna(0)
            self.log.print(f'request', 'DONE!!')
            return df
        except Exception as e:
            raise Exception(f'request()::something went wrong: {str(e)}')


# main function
if __name__ == '__main__':
    # args
    parser = argparse.ArgumentParser()
    dt_default = f_dt.Datetime.get_current_datetime()
    parser.add_argument("-i", "--initdate", help="Initial Date (YY-MM-DD)", default=f_dt.Datetime.datetime_to_str(dt_default, '%Y-%m-01'))
    parser.add_argument("-e", "--enddate", help="End Date (YY-MM-DD)", default=f_dt.Datetime.datetime_to_str(dt_default, '%Y-%m-31'))
    args = parser.parse_args()

    result = {}
    variables = {
        'from_date': args.initdate,
        'to_date': args.enddate
    }
    # app
    app = App()
    # csv
    df_result = app.request()
    f.CSV.dataframe_to_file(df_result, 'df.csv')


