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
import libs.adobe.api_adobe_analytics as api_adobe
import libs.dataframe as f_df
import libs.dt as f_dt
import libs.log as f_log


class App:
    def __init__(self):
        self.payload = variables['payload']
        self.from_date = variables['from_date']
        self.to_date = variables['to_date']
        self.columns = variables['columns']
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

    def request(self):
        try:
            rs = self.get_adobe_report_suite()
            df = self.get_adobe_api_calls(rs)
            df = self.get_adobe_api_calls_by_site(df)
            self.log.print(f'request', 'DONE!!')
            return df
        except Exception as e:
            self.log.print_error(str(e))

    def get_adobe_report_suite(self):
        try:
            api = api_adobe.Adobe_Report_Suite_API()
            df = api.request()
            df = df.loc[df['collectionItemType'] == 'reportsuite']
            self.log.print('get_adobe_report_suite', 'dataframe loaded')
            return df
        except Exception as e:
            raise Exception(f'get_adobe_report_suite()::something went wrong: {str(e)}')

    def get_adobe_api_calls(self, rs):
        df = pd.DataFrame()
        try:
            for index, row in rs.iterrows():
                self.log.print('get_adobe_api_calls', f'{str(index)}::rsid::{row["rsid"]}')
                # request
                api = api_adobe.Adobe_Report_API(row['rsid'], self.payload, self.from_date, self.to_date)
                df_request = api.request()
                if not f_df.Dataframe.is_empty(df_request):
                    df_request = df_request[['value', 0, 1]]
                    df_request.insert(loc=0, column='rs', value=row['rsid'])
                    df_request.columns = self.columns.split(',')
                    df_request = f_df.Dataframe.Cast.columns_to_datetime(df_request, ['month'], '%Y-%m')
                    df_request = f_df.Dataframe.Cast.columns_regex_to_int64(df_request, '^(page_).*$')
                    df_request.sort_values(by='month', ascending=True, inplace=True)
                    df = f_df.Dataframe.Rows.concat_two_frames(df, df_request)
            self.log.print('get_adobe_api_calls', 'dataframe loaded')
            return df
        except Exception as e:
            raise Exception(f'get_adobe_api_calls()::something went wrong: {str(e)}')

    def get_adobe_api_calls_by_site(self, df):
        df['site'] = 'others'
        df['total'] = df['page_views'] + df['page_events']
        df.loc[df['report_suite'].str.contains("fotocasa"), 'site'] = 'realestate'
        df.loc[df['report_suite'].str.contains("habitaclia"), 'site'] = 'realestate'
        df.loc[df['report_suite'].str.contains("inmofactory"), 'site'] = 'realestate'
        df.loc[df['report_suite'].str.contains("uniquetool"), 'site'] = 'realestate'
        df.loc[df['report_suite'].str.contains("jobs"), 'site'] = 'jobs'
        df.loc[df['report_suite'].str.contains("motor"), 'site'] = 'motor'
        df.loc[df['report_suite'].str.contains("milanuncios"), 'site'] = 'generalist'
        # transform
        df = df.groupby(['site', 'month'], as_index=False).sum()
        df = pd.pivot_table(df, values=['total'], index=['month'], columns=['site'], aggfunc=np.sum)
        df.columns = df.columns.droplevel(level=0)
        df = df.reset_index()
        df = df[['month', 'realestate', 'jobs', 'motor', 'generalist', 'others']]
        # log
        self.log.print('get_adobe_by_site', 'dataframe loaded')
        return df


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
        'to_date': args.enddate,
        'payload': 'aa/request.json',
        'columns': 'report_suite,month,page_views,page_events'
    }
    # app
    app = App()
    # csv
    df = app.request()
    f.CSV.dataframe_to_file(df, 'df.csv')


