# AA > https://adobedocs.github.io/analytics-2.0-apis/
# parameters > -i 2023-01-01 -e 2023-02-01
# usage > ./api-license.sh [-h] [-i INITDATE] [-e ENDDATE]
#   -i    Inital date (YYYY-MM-DD)
#   -e    End date (YYYY-MM-DD)
#   -h    Help
import pandas as pd
import sys
import os
import numpy as np
import argparse
import libs.csv as f
import libs.adobe.api_adobe_analytics as api
import libs.dataframe as f_df
import libs.dt as f_dt
import libs.log as f_log


class App:
    def __init__(self):
        self.config = None
        self.log = f_log.Logging()

        # args
        self.log.print('init', 'Total arguments passed: ' + str(len(sys.argv)))
        self.log.print('init', 'Name of Python script: ' + sys.argv[0])
        for i in range(1, len(sys.argv)):
            self.log.print('init', 'Argument: ' + sys.argv[i])

        # directory
        self.directory = os.path.dirname(os.path.realpath(__file__))
        f.Directory.set_working_directory(self.directory)
        f.Directory.create_directory('csv')
        self.log.print('init', f'working_directory: {f.Directory.get_working_directory()}')

    def request(self, config):
        try:
            self.config = config
            df = self.get_usage_logs()
            # df.sort_values(by='dateCreated', ascending=True, inplace=True)
            df["month"] = pd.to_datetime(df['dateCreated']).dt.strftime('%Y-%m')
            df = df.groupby(['month', 'login'], as_index=False)['eventType'].count()
            df = pd.pivot_table(df, values=['eventType'], index=['login'], columns=['month'], aggfunc=np.sum)
            df.columns = df.columns.droplevel(level=0)
            df = df.reset_index()
            self.log.print(f'request', 'DONE!!')
            return df
        except Exception as e:
            raise Exception(str(e))

    def get_usage_logs(self):
        try:
            api_usage_logs = api.usage_logs(self.config)
            df = api_usage_logs.request()
            self.log.print('get_usage_logs', 'dataframe loaded')
            return df
        except Exception as e:
            raise Exception(f'get_usage_logs()::something went wrong: {str(e)}')


# main function
if __name__ == '__main__':
    # args
    parser = argparse.ArgumentParser()
    dt_default = f_dt.Datetime.get_current_datetime()
    parser.add_argument("-i", "--initdate", help="Initial Date (YY-MM-DD)", default=f_dt.Datetime.datetime_to_str(dt_default, '%Y-%m-01'))
    parser.add_argument("-e", "--enddate", help="End Date (YY-MM-DD)", default=f_dt.Datetime.datetime_to_str(dt_default, '%Y-%m-31'))
    args = parser.parse_args()

    # app
    app = App()
    params = {
        'from_date': '2022-09-01',
        'to_date': '2022-12-01'
    }
    df_usage_logs_1 = app.request(params)
    params = {
        'from_date': '2022-12-01',
        'to_date': '2023-03-01'
    }
    df_usage_logs_2 = app.request(params)
    # merge
    df_usage_logs = f_df.Dataframe.Columns.join_two_frames_by_columns(df_usage_logs_1, df_usage_logs_2, ['login'], 'outer', None)
    df_usage_logs = df_usage_logs.fillna(0)
    f_df.Dataframe.Cast.columns_regex_to_int64(df_usage_logs, '^[2022-2023].*$')
    df_usage_logs.sort_values(by='login', ascending=True, inplace=True)

    # csv
    f.CSV.dataframe_to_file(df_usage_logs, 'df.csv')


