# AA > https://adobedocs.github.io/analytics-2.0-apis/
# GA4 > https://ga-dev-tools.web.app/ga4/query-explorer/

import pandas as pd
import sys
import os

# adding libraries folder to the system path
sys.path.insert(0, '/Users/luis.salamo/Documents/github enterprise/python-training/libraries')

import constants
import functions as f
import api as f_api
import api_ga4 as f_api_ga4
import dataframe as f_df


def init():
    # f.Directory.change_working_directory(variables['directory'])
    f.Directory.create_directory('csv')
    f.Log.print('init', 'loaded')


def clean_columns(df, platform, to_columns):
    to_columns = to_columns.replace('{{platform}}', platform).split(',')
    if not f_df.Dataframe.is_empty(df):
        df.columns = to_columns
        if f_df.Dataframe.Columns.exists(df, 'day'):
            df['day'] = pd.to_datetime(df['day']).dt.strftime('%Y%m%d').astype('str')
        # df_values = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(' + platform + '-).*$')
        # df[df_values.columns] = df_values
    else:
        for i in to_columns:
            df[i] = 0
    return df


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================
class Adobe:
    def __init__(self, columns, rs, token, from_date, to_date):
        self.columns = columns
        self.rs = rs
        self.token = token
        self.from_date = from_date
        self.to_date = to_date

    def get_request(self, file_payload, columns, columns_join):
        # request
        api = f_api.Adobe_Report_API(self.rs, self.token, file_payload, self.from_date, self.to_date)
        df = api.request()
        # web
        from_columns = ('value', 0, 1, 2)
        df_web = clean_columns(df.loc[:, from_columns], constants.PLATFORM_WEB, columns)
        # android
        from_columns = ['value', 3, 4, 5]
        df_and = clean_columns(df.loc[:, from_columns], constants.PLATFORM_ANDROID, columns)
        # ios
        from_columns = ['value', 6, 7, 8]
        df_ios = clean_columns(df.loc[:, from_columns], constants.PLATFORM_IOS, columns)
        # join dataframes
        frames = [df_web, df_and, df_ios]
        df = f_df.Dataframe.Columns.join_by_columns(frames, columns_join, 'outer')

        f.Log.print('get_adobe', 'dataframe loaded <' + file_payload + '>')
        return df

    def get_adobe(self):
        return self.get_request('aa/request.json', self.columns, ['day'])


# =============================================================================
#   REQUEST GOOGLE ANALYTICS
# =============================================================================
class Google:
    def __init__(self):
        self.property_id = variables['site_ga']
        self.from_date = variables['from_date']
        self.to_date = variables['to_date']
        self.columns = variables['columns']

    def get_google(self):
        def get_platform(platform):
            df_platform = df.loc[df['platform'] == platform]
            # change column names
            df_platform = f_df.Dataframe.Columns.drop(df_platform, ['platform'])
            df_platform.columns = self.columns.replace('{{platform}}', platform[0:3].lower()).split(',')
            # sort
            df_platform['day'] = pd.to_datetime(df_platform['day']).dt.strftime('%Y%m%d').astype('str')
            frame = f_df.Dataframe.Sort.sort_by_columns(df_platform, ['day'], True)
            return frame

        api = f_api_ga4.API_GA4()
        dimensions = 'date,platform'
        metrics = 'sessions,totalUsers,screenPageViews'
        date_ranges = {'start_date': self.from_date, 'end_date': self.to_date}

        # Request
        df = api.request(self.property_id, dimensions, metrics, date_ranges)

        # Get Platform
        df_web = get_platform(constants.GA4.platform.web)
        df_and = get_platform(constants.GA4.platform.android)
        df_ios = get_platform(constants.GA4.platform.ios)

        # join dataframes
        frames = [df_web, df_and, df_ios]
        df = f_df.Dataframe.Columns.join_by_columns(frames, 'day', 'outer')

        f.Log.print('get_google', 'dataframe loaded')
        return df


def merge_adobe_google(df_aa, df_ga):
    df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_aa, df_ga, ['day'], 'outer', ('-aa', '-ga'))
    df = df.fillna(0)
    df_values = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(web-|and-|ios-)')
    df[df_values.columns] = df_values
    f.Log.print('merge_adobe_google', 'data loaded')
    return df


def get_csv_by_platform(df):
    def get_platform(platform):
        columns = variables['columns_tools'].replace('{{platform}}', platform).split(',')
        df_platform = df[columns]
        f.CSV.dataframe_to_file(df_platform, 'df_' + platform + '.csv')
        return df_platform

    result['df_web'] = get_platform(constants.PLATFORM_WEB)
    f.Log.print('get_csv_by_platform', 'web - data loaded')
    result['df_and'] = get_platform(constants.PLATFORM_ANDROID)
    f.Log.print('get_csv_by_platform', 'and - data loaded')
    result['df_ios'] = get_platform(constants.PLATFORM_IOS)
    f.Log.print('get_csv_by_platform', 'ios - data loaded')


# =============================================================================
#   MAIN
# =============================================================================
print('\n=====================================  BEGIN EXECUTION =====================================\n')

# main function
if __name__ == '__main__':
    result = {}
    variables = {'rs': {},
                 'token_aa': 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NjI1NjY4NTgwOTNfN2JhYjk3MzctY2Q2MS00NDJmLTljMmUtOTc4YWIxNTE4N2Y4X2V3MSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1YThkY2MyY2ZhNzE0NzJjYmZhNGZiNTM2NzFjNDVlZCIsInVzZXJfaWQiOiI3NjREN0Y4RDVFQjJDRDUwMEE0OTVFMUJAMmRkMjM0Mzg1ZTYxMDdkNzBhNDk1Y2E0Iiwic3RhdGUiOiIiLCJhcyI6Imltcy1uYTEiLCJhYV9pZCI6Ijc2NEQ3RjhENUVCMkNENTAwQTQ5NUUxQkAyZGQyMzQzODVlNjEwN2Q3MGE0OTVjYTQiLCJjdHAiOjAsImZnIjoiV1lIT1BZVEZGUEU1SUhVS0VNUUZZSFFBQk09PT09PT0iLCJzaWQiOiIxNjYyNTY2ODU3NTkwXzllZjY1MmMzLWYzYzUtNDc0MS04MjkyLWQwMzkxMGI5YTcyNV91ZTEiLCJydGlkIjoiMTY2MjU2Njg1ODA5NF8wMjc4MGVkMC1mZWExLTRlMzctYTY1My04Mjk1ODQyMWMwYWVfZXcxIiwibW9pIjoiMzlkMzY5MzkiLCJwYmEiOiIiLCJydGVhIjoiMTY2Mzc3NjQ1ODA5NCIsImV4cGlyZXNfaW4iOiI4NjQwMDAwMCIsInNjb3BlIjoib3BlbmlkLEFkb2JlSUQscmVhZF9vcmdhbml6YXRpb25zLGFkZGl0aW9uYWxfaW5mby5wcm9qZWN0ZWRQcm9kdWN0Q29udGV4dCxhZGRpdGlvbmFsX2luZm8uam9iX2Z1bmN0aW9uIiwiY3JlYXRlZF9hdCI6IjE2NjI1NjY4NTgwOTMifQ.EEBaBlkXqOe9RESiae9JJEITrj8BE3XRjZ1RWmJcAigpmGswyAhVDCQEafliMsda40RzBw9dUlpFqWVkm-_shY3Ym-XHrwKczDqlPK9IwDtVFFB7IzhK4s9eo3BI9fj6794RqSZySdLhVqwpQp99YsfPH8UhpnwbZ3QnCCob4mdKebkWm3oheHoQZVKxrYxUSErksY-Heh_ugjWVV9dSes0AXcEi_DyWlVFFF2WRFrIaqfLJPcqIJmpGyTGbKEMIgZbYR_NZAanLIHvvrf8IV6jScLUVChK3bSp39dA9pRI7B7tMNMk5mcG6Bg6dVKBYQG9RM7qD3yF57D1xltWLJQ',
                 'from_date': '2022-08-01',
                 'to_date': '2022-09-07',
                 'site_aa': f_api.Adobe_Report_API.rs_motosnet,
                 'site_ga': f_api.Google_Report_API.property_motosnet,
                 'columns': 'day,{{platform}}-visits,{{platform}}-visitors,{{platform}}-views',
                 'columns_tools': 'day,{{platform}}-visits-aa,{{platform}}-visits-ga,{{platform}}-visitors-aa,{{platform}}-visitors-ga,{{platform}}-views-aa,{{platform}}-views-ga'
                 }

    init()

    adobe = Adobe(variables['columns'], variables['site_aa'], variables['token_aa'], variables['from_date'], variables['to_date'])
    result['df_aa'] = adobe.get_adobe()
    google = Google()
    result['df_ga'] = google.get_google()

    result['df'] = merge_adobe_google(result['df_aa'], result['df_ga'])
    get_csv_by_platform(result['df'])

print('\n=====================================  END EXECUTION =====================================\n')


