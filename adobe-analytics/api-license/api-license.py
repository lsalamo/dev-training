# AA > https://adobedocs.github.io/analytics-2.0-apis/

import pandas as pd
import sys

# adding libraries folder to the system path
sys.path.insert(0, '/Users/luis.salamo/Documents/github enterprise/python-training/libraries')

import functions as f
import api as f_api
import dataframe as f_df


def init():
    f.Directory.change_working_directory(variables['directory'])
    f.Directory.create_directory('csv')
    f.Log.print('init', 'loaded')


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================
class Adobe:
    def __init__(self, token):
        self.token = token

    def get_adobe_report_suite(self):
        # request
        api = f_api.Adobe_Report_Suite_API(self.token)
        df = api.request()
        df = df.loc[df['collectionItemType'] == 'reportsuite']
        f.Log.print('get_adobe_report_suite', 'dataframe loaded')
        return df

    def get_adobe_license(self, file_payload, from_date, to_date,):
        df = pd.DataFrame()
        for index, row in result['df_aa_rs'].iterrows():
            f.Log.print('get_adobe_license', str(index) + ' - rsid:' + row['rsid'])

            # request
            api = f_api.Adobe_Report_API(row['rsid'], self.token, file_payload, from_date, to_date)
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
                 'token_aa': 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NjExNTE4NjY2ODRfZGNiNTk1MTctZmIzOC00OTZmLWJhYzktYTQ4YzEzMmUyZmM3X2V3MSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1YThkY2MyY2ZhNzE0NzJjYmZhNGZiNTM2NzFjNDVlZCIsInVzZXJfaWQiOiI3NjREN0Y4RDVFQjJDRDUwMEE0OTVFMUJAMmRkMjM0Mzg1ZTYxMDdkNzBhNDk1Y2E0Iiwic3RhdGUiOiIiLCJhcyI6Imltcy1uYTEiLCJhYV9pZCI6Ijc2NEQ3RjhENUVCMkNENTAwQTQ5NUUxQkAyZGQyMzQzODVlNjEwN2Q3MGE0OTVjYTQiLCJjdHAiOjAsImZnIjoiV1daTVJNMkpGUEc1SUhVT0Y0WUZZSFFBWFU9PT09PT0iLCJzaWQiOiIxNjYxMTUxODY2NDA1XzQyZGMzODIxLWJiZTktNDI5Yi1iZjUyLTUxN2I1MzNjYTc3Ml91ZTEiLCJydGlkIjoiMTY2MTE1MTg2NjY4NV9jMmQwYzFlOC0zZDU1LTRjNDQtYWIxYS0zODZmNTdjZGQ3YjVfZXcxIiwibW9pIjoiMzJlZmM4MmYiLCJwYmEiOiIiLCJydGVhIjoiMTY2MjM2MTQ2NjY4NSIsImV4cGlyZXNfaW4iOiI4NjQwMDAwMCIsImNyZWF0ZWRfYXQiOiIxNjYxMTUxODY2Njg0Iiwic2NvcGUiOiJvcGVuaWQsQWRvYmVJRCxyZWFkX29yZ2FuaXphdGlvbnMsYWRkaXRpb25hbF9pbmZvLnByb2plY3RlZFByb2R1Y3RDb250ZXh0LGFkZGl0aW9uYWxfaW5mby5qb2JfZnVuY3Rpb24ifQ.a8ZNjAb84QYwOTZRppcBi2Bn2zQ97Jf4WAZxDeAoKXWZiSNxGJe6s6y0SQbosEwXVDNz2V_cXr-0wn2qejoIBM1G7h8QxsaJYxEoJnUrFbc6Fke5kpXcUHZPpsVcOkLGeWUlL4VhXQDB4I3gI2fq4pbJkvNo1V2FmwwxKw7WwMFRXaA-N4idgcI3kJCD-8DxeLWOw1PKuwYK8PM3gCCZULJW84L-TZWQwZjdPHvx4gu0ovJhbnWws7ycIT5eBvxKNhA2M2KUl808JzBoQKpNwiIjv_KHIGEg-xsuBEKGyN70NyYqBuZvOFI47yksEX4avKZSM7On5VkQZr9kcMF-mA',
                 'directory': '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/api-license',
                 'from_date': '2022-01-01',
                 'to_date': '2022-12-31',
                 'file_payload': 'aa/request.json'
                 }

    init()

    adobe = Adobe(variables['token_aa'])
    result['df_aa_rs'] = adobe.get_adobe_report_suite()
    result['df_aa_license'] = adobe.get_adobe_license(variables['file_payload'], variables['from_date'], variables['to_date'])
    f.CSV.dataframe_to_file(result['df_aa_license'], 'df.csv')

print('> END EXECUTION')
