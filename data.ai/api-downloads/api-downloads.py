# usage > ./api-downloads.py [-i INITDATE] [-e ENDDATE]
#   -i    Inital date (YYYY-MM-DD)
#   -e    End date (YYYY-MM-DD)
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
            config_file = os.path.join(os.path.dirname(__file__), '../credentials/config.ini')
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
            # api
            api = api_dataai.report_downloads(self.config['token'])
            api.date_from = variables['from_date']
            api.to_date = variables['to_date']

            # dataframe
            df = pd.DataFrame()
            for vertical in self.config['verticals']:
                products = ",".join(self.config['verticals'][vertical]['android'].values())
                df_and = api.request(vertical, products)
                df = f_df.Dataframe.Rows.concat_two_frames(df, df_and)
                products = ",".join(self.config['verticals'][vertical]['ios'].values())
                df_ios = api.request(vertical, products)
                df = f_df.Dataframe.Rows.concat_two_frames(df, df_ios)
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


