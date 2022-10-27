# AA > https://adobedocs.github.io/analytics-2.0-apis/

import pandas as pd
import sys

# adding libraries folder to the system path
sys.path.insert(0, '/Users/luis.salamo/Documents/github enterprise/python-training/libraries')

import functions as f
import api_adobe_analytics2_0 as f_api_adobe
import dataframe as f_df


def init():
    f.Directory.change_working_directory(variables['directory'])
    f.Directory.create_directory('csv')
    f.Log.print('init', 'loaded')


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================
class Adobe:
    @staticmethod
    def get_adobe_report_suite():
        # request
        api = f_api_adobe.Adobe_Report_Suite_API(result['access_token'])
        df = api.request()
        df = df.loc[df['collectionItemType'] == 'reportsuite']
        f.Log.print('get_adobe_report_suite', 'dataframe loaded')
        return df

    @staticmethod
    def get_adobe_license(file_payload, from_date, to_date, ):
        df = pd.DataFrame()
        for index, row in result['df_aa_rs'].iterrows():
            f.Log.print('get_adobe_license', str(index) + ' - rsid:' + row['rsid'])

            # request
            api = f_api_adobe.Adobe_Report_API(row['rsid'], file_payload, from_date, to_date, result['access_token'])
            df_request = api.request()
            df_request = df_request.loc[:, ('value', 0, 1, 2)]
            df_request.columns = 'value,total-page-views,total-page-events,total'.split(',')
            df_values = f_df.Dataframe.Cast.columns_regex_to_int64(df_request, '^(total).*$')
            df_request[df_values.columns] = df_values
            df_request['rs'] = row['rsid']
            df_request['week'] = pd.to_datetime(df_request["value"]).dt.strftime('%W')
            df_request['year'] = pd.to_datetime(df_request["value"]).dt.year
            df = f_df.Dataframe.Rows.concat_two_frames(df, df_request)

        df = df.groupby(['year', 'week']).agg(
            sum_pageviews=pd.NamedAgg(column='total-page-views', aggfunc='sum'),
            sum_pageevents=pd.NamedAgg(column='total-page-events', aggfunc='sum'),
            sum_total=pd.NamedAgg(column='total', aggfunc='sum'))

        return df


# main function
if __name__ == '__main__':
    result = {}
    variables = {'rs': {},
                 'directory': '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/api-license',
                 'from_date': '2022-01-01',
                 'to_date': '2022-12-31',
                 'file_payload': 'aa/request-web.json'
                 }

    init()
    result['access_token'] = f_api_adobe.Adobe_JWT.get_access_token()
    result['df_aa_rs'] = Adobe.get_adobe_report_suite()
    result['df_aa_license'] = Adobe.get_adobe_license(variables['file_payload'], variables['from_date'], variables['to_date'])
    f.CSV.dataframe_to_file(result['df_aa_license'], 'df.csv')

print('> END EXECUTION')
