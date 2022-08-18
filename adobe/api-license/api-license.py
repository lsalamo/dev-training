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
                 'token_aa': 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NjA4MTIyMjA1MjFfZTUwNWMwMzktNTRhMi00Y2JmLThiM2ItZGVkNDliMTgyZmZiX2V3MSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1YThkY2MyY2ZhNzE0NzJjYmZhNGZiNTM2NzFjNDVlZCIsInVzZXJfaWQiOiI3NjREN0Y4RDVFQjJDRDUwMEE0OTVFMUJAMmRkMjM0Mzg1ZTYxMDdkNzBhNDk1Y2E0Iiwic3RhdGUiOiIiLCJhcyI6Imltcy1uYTEiLCJhYV9pZCI6Ijc2NEQ3RjhENUVCMkNENTAwQTQ5NUUxQkAyZGQyMzQzODVlNjEwN2Q3MGE0OTVjYTQiLCJjdHAiOjAsImZnIjoiV1dPS1hNMkpGUEc1SUhVT0Y0WUZRSFFBQ1k9PT09PT0iLCJzaWQiOiIxNjYwODEyMjIwMTk1XzM0MjU5ODcxLTUxOTAtNGY5Zi04OTEyLTJiZTBkNjZkYjFhZl91ZTEiLCJydGlkIjoiMTY2MDgxMjIyMDUyMl8zOGI0M2RiZC1mNWQwLTQxNDQtYWNkYS1kZjVmN2Q5MjI3ODFfZXcxIiwibW9pIjoiZDZhMGMxZTQiLCJwYmEiOiIiLCJydGVhIjoiMTY2MjAyMTgyMDUyMiIsImV4cGlyZXNfaW4iOiI4NjQwMDAwMCIsImNyZWF0ZWRfYXQiOiIxNjYwODEyMjIwNTIxIiwic2NvcGUiOiJvcGVuaWQsQWRvYmVJRCxyZWFkX29yZ2FuaXphdGlvbnMsYWRkaXRpb25hbF9pbmZvLnByb2plY3RlZFByb2R1Y3RDb250ZXh0LGFkZGl0aW9uYWxfaW5mby5qb2JfZnVuY3Rpb24ifQ.NExWReRVfz3JfD29Wud58KDRkbUbf1HqwkW9IRPSoU0RBhqZAY77nu9ejkaOmiLZw37-fn6UDMg3vSlSru14BBFTPcwjS6iDMrKYu-se5fbiLsZ50OaJVLzPLWsfy7hLzB47VtUoe5ax6LjwRJR1VrX9ml517VhZ-a0Dk5KkQ0b_kQ5jBwBrIdEWI6KLtlxBK3qjkhnof5QUuml5e-YjBfZqx_LhT1Kbu90-sGxsnKFLMPNYcC6tB9_K3_oQXB7ElwX7nA57vix0CJWKcC0SxL408ZGOk83QMr_wa2j1tRWNCTzjtFZ5S9a8cdcdUI_CJEetvxzfOp43Ur1Kt-UOUQ',
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
