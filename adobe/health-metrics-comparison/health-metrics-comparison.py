# https://github.com/AdobeDocs/analytics-2.0-apis
# https://github.com/pitchmuc/adobe-analytics-api-2.0
# AA > https://adobedocs.github.io/analytics-2.0-apis/
# GA4 > https://ga-dev-tools.web.app/ga4/query-explorer/

import requests
import pandas as pd
import numpy as np
import os
import sys
import shutil
import sys
import re

# adding libraries folder to the system path
sys.path.insert(0, '/Users/luis.salamo/Documents/github enterprise/python-training/libraries')

import constants
import functions as f
import api as f_api
import dataframe as f_df


def init():
    f.Directory.change_working_directory(variables['directory'])
    f.Directory.create_directory('csv')
    f.Log.print('init', 'loaded')


def clean_columns(df, platform, to_columns):
    to_columns = to_columns.replace('{{platform}}', platform).split(',')
    if not f_df.Dataframe.is_empty(df):
        df.columns = to_columns
        if f_df.Dataframe.Columns.exists(df, 'day'):
            df['day'] = pd.to_datetime(df['day']).dt.strftime('%Y%m%d').astype('str')
        if f_df.Dataframe.Columns.exists(df, 'event'):
            df['event'] = df['event'].str.lower().replace(' ', '_', regex=True)
        df_values = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(' + platform + '-).*$')
        df[df_values.columns] = df_values
    else:
        for i in to_columns:
            df[i] = 0
    return df


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================
class Adobe:
    def __init__(self,request_columns, request_events_columns, rs, token, from_date, to_date):
        self.request_columns = request_columns
        self.request_events_columns = request_events_columns
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
        return self.get_request('aa/request.json', self.request_columns, ['day'])

    def get_adobe_events(self):
        return self.get_request('aa/request-events.json', self.request_events_columns, ['event'])


# =============================================================================
#   REQUEST GOOGLE ANALYTICS
# =============================================================================
class GoogleAPI:
    def __init__(self):
        self.request_columns = variables['request_columns']
        self.request_events_columns = variables['request_events_columns']
        self.property_id = variables['site_ga']
        self.token = variables['token_ga']
        self.from_date = variables['from_date']
        self.to_date = variables['to_date']

    def get_request(self, file_payload, to_columns, type_request):
        def run_request(platform):
            api = f_api.Google_Report_API(self.property_id, self.token, file_payload, self.from_date, self.to_date, platform)
            df_request = api.request()
            if not df_request.empty:
                if type_request == "event":
                    from_columns = ['0_x', '0_y', '1_y', 2]
                    df_request.loc[df_request['1_x'] != '(not set)', '0_x'] = df_request.loc[df_request['1_x'] != '(not set)', '1_x']
                else:
                    from_columns = ['0_x', '0_y', 1, 2]
                df_request = df_request.loc[:, from_columns]
            df_request = clean_columns(df_request, platform, to_columns)
            return df_request

        # web
        df_web = run_request(constants.PLATFORM_WEB)
        # android
        df_and = run_request(constants.PLATFORM_ANDROID)
        # # ios
        df_ios = run_request(constants.PLATFORM_IOS)
        # join dataframes
        frames = [df_web, df_and, df_ios]
        columns_join = ['event'] if type_request == 'event' else ['day']
        df = f_df.Dataframe.Columns.join_by_columns(frames, columns_join, 'outer')

        f.Log.print('get_google', 'dataframe loaded <' + file_payload + '>')
        return df

    def get_google(self):
        return self.get_request('ga/request.json', self.request_columns, 'day')

    def get_google_events(self):
        return self.get_request('ga/request-events.json', self.request_events_columns, 'event')


class GoogleCSV:
    def __init__(self):
        self.request_columns = variables['request_columns']
        self.request_events_columns = variables['request_events_columns']
        self.pattern = '{{request}}'
        self.file_web = 'ga/data-' + self.pattern + constants.PLATFORM_WEB + '.csv'
        self.file_and = 'ga/data-' + self.pattern + constants.PLATFORM_ANDROID + '.csv'
        self.file_ios = 'ga/data-' + self.pattern + constants.PLATFORM_IOS + '.csv'

    def get_google(self):
        # web
        file = self.file_web.replace(self.pattern, '')
        df_web = Google.csv_to_dataframe(file)
        df_web = Google.clean_columns(df_web, constants.PLATFORM_WEB, self.request_columns)
        # android
        file = self.file_and.replace(self.pattern, '')
        df_and = Google.csv_to_dataframe(file)
        df_and = Google.clean_columns(df_and, constants.PLATFORM_ANDROID, self.request_columns)
        # ios
        file = self.file_ios.replace(self.pattern, '')
        df_ios = Google.csv_to_dataframe(file)
        df_ios = Google.clean_columns(df_ios, constants.PLATFORM_IOS, self.request_columns)
        # join dataframes
        frames = [df_web, df_and, df_ios]
        df = f_df.Dataframe.Columns.join_by_columns(frames, ['day'], 'outer')
        return df

    def get_google_events(self):
        # web
        file = self.file_web.replace(self.pattern, 'events-')
        df_web = Google.csv_to_dataframe(file)
        df_web = Google.clean_columns(df_web, constants.PLATFORM_WEB, self.request_events_columns)
        # android
        file = self.file_and.replace(self.pattern, 'events-')
        df_and = Google.csv_to_dataframe(file)
        df_and = Google.clean_columns(df_and, constants.PLATFORM_ANDROID, self.request_events_columns)
        # ios
        file = self.file_ios.replace(self.pattern, 'events-')
        df_ios = Google.csv_to_dataframe(file)
        df_ios = Google.clean_columns(df_ios, constants.PLATFORM_IOS, self.request_events_columns)
        # join dataframes
        frames = [df_web, df_and, df_ios]
        df = f_df.Dataframe.Columns.join_by_columns(frames, ['event'], 'outer')
        return df


# =============================================================================
#   JOIN
# =============================================================================

def merge_adobe_google(frames, columns):
    df = f_df.Dataframe.Columns.join_two_frames_by_columns(frames['df_aa'], frames['df_ga'], columns, 'outer', ('-aa', '-ga'))
    df = df.fillna(0)
    df_values = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(web-|and-|ios-)')
    df[df_values.columns] = df_values
    f.Log.print('merge_adobe_google', 'data loaded')
    return df


def merge_adobe_google_platform(df, platform, columns):
    columns = columns.replace('{{platform}}', platform).split(',')
    df = df[columns]
    if f_df.Dataframe.Columns.exists(df, 'event'):
        df = df.loc[~((df[platform + '-count-aa'] == 0) & (df[platform + '-count-ga'] == 0))]
        df.sort_values(by=platform + '-count-aa', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    f.Log.print('merge_adobe_google_platform', platform + ' - data loaded')
    return df


# =============================================================================
#   MAIN
# =============================================================================

# main function
if __name__ == '__main__':
    result = {}
    result_events = {}
    variables = {'rs': {},
                 'token_aa': 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NTY1MzM2OTU1MjBfZjFlMTk2NmMtMWUzYS00ODkwLWE4YmYtODM1MzBlYmJlZDRhX2V3MSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1YThkY2MyY2ZhNzE0NzJjYmZhNGZiNTM2NzFjNDVlZCIsInVzZXJfaWQiOiI3NjREN0Y4RDVFQjJDRDUwMEE0OTVFMUJAMmRkMjM0Mzg1ZTYxMDdkNzBhNDk1Y2E0Iiwic3RhdGUiOiIiLCJhcyI6Imltcy1uYTEiLCJhYV9pZCI6Ijc2NEQ3RjhENUVCMkNENTAwQTQ5NUUxQkAyZGQyMzQzODVlNjEwN2Q3MGE0OTVjYTQiLCJjdHAiOjAsImZnIjoiV1NEQjVTQ1BGUE41SUhVT0VNUUZRSFFBT009PT09PT0iLCJzaWQiOiIxNjU2NTMzNjk1MTk5XzJkZTliMjRmLWEzYjctNGFjNi05MzgzLWQ5OGU5ZmUxNTZiZl91ZTEiLCJydGlkIjoiMTY1NjUzMzY5NTUyMl9kZjZjMDE0Yy04ZDRiLTQ1NGItOWMyOS1lYjU0MzczZDEyNjRfZXcxIiwibW9pIjoiNDY1MzI0ZSIsInBiYSI6IiIsInJ0ZWEiOiIxNjU3NzQzMjk1NTIyIiwiZXhwaXJlc19pbiI6Ijg2NDAwMDAwIiwic2NvcGUiOiJvcGVuaWQsQWRvYmVJRCxyZWFkX29yZ2FuaXphdGlvbnMsYWRkaXRpb25hbF9pbmZvLnByb2plY3RlZFByb2R1Y3RDb250ZXh0LGFkZGl0aW9uYWxfaW5mby5qb2JfZnVuY3Rpb24iLCJjcmVhdGVkX2F0IjoiMTY1NjUzMzY5NTUyMCJ9.FTahCRCkUG0MpyVnB0jrdEXhgiZPZOR0h45ZquQ1rHGwgMIDCayLK5Ls5ZleFZ_CMhaVU2EdpaLT6dXX57RCTCyl5TWd5hgDBPOfrCbgiQVrhKmIX0KLAfLYKZXGTTVwo4CuU0X_Aj23gXRXr91Gm_OqRizzIHY3jEO9mdL58roOVGzvazJyDDx5mkmWjqoSUo2QuBZ6m1yzhBmZELVOc0malQ0F80xHMAKoo-FwS1OfVUBJ_jSNVtHMPA58YBFPT3WayduBmhHNbacg_nCFV59rgjOZeCHN9bGwAEYCC4azJYTswBmyZ_rfQpTrSvLdnPpkYHEx9giHIyFX9nK_Rg',
                 'token_ga': 'ya29.A0ARrdaM9F8JT2MwRpkVUwlgRBeLjE7nAHTZv3Kg5bfXHX43ZJrA-0QXmk81WMf7u80IRxJZKu1M1C4vPGpn9KbNMVNN0iYXwkiZrkOr1zXWd7lHOBPP6maRbybjXJRtuPz4cexYjLFV84FnjRyhgHKDSW4DaP8N4YUNnWUtBVEFTQVRBU0ZRRl91NjFWQy1lZmJBMGNqOG1acVB4NWNCN3dkQQ0166',
                 'directory': '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/health-metrics-comparison',
                 'from_date': '2022-06-28',
                 'to_date': '2022-06-28',
                 'site_aa': f_api.Adobe_Report_API.rs_motosnet,
                 'site_ga': f_api.Google_Report_API.property_motosnet,
                 'request_columns': 'day,{{platform}}-visits,{{platform}}-visitors,{{platform}}-views',
                 'request_events_columns': 'event,{{platform}}-count,{{platform}}-visits,{{platform}}-visitors',
                 'merge_columns': 'day,{{platform}}-visits-aa,{{platform}}-visits-ga,{{platform}}-visitors-aa,{{platform}}-visitors-ga,{{platform}}-views-aa,{{platform}}-views-ga',
                 'merge_events_columns': 'event,{{platform}}-count-aa,{{platform}}-count-ga,{{platform}}-visits-aa,{{platform}}-visits-ga,{{platform}}-visitors-aa,{{platform}}-visitors-ga'
                 }

    init()

    # Adobe Analytics > user and visits
    adobe = Adobe(variables['request_columns'], variables['request_events_columns'], variables['site_aa'], variables['token_aa'], variables['from_date'], variables['to_date'])
    result['df_aa'] = adobe.get_adobe()
    result_events['df_aa'] = adobe.get_adobe_events()

    # Google Analytics > user and visits
    google = GoogleAPI()
    # google = GoogleCSV()
    result['df_ga'] = google.get_google()
    result_events['df_ga'] = google.get_google_events()

    # df > merge data
    result['df'] = merge_adobe_google(result, ['day'])
    result_events['df'] = merge_adobe_google(result_events, ['event'])

    # df > merge data by platform and create csv
    result['df_web'] = merge_adobe_google_platform(result['df'], constants.PLATFORM_WEB, variables['merge_columns'])
    f.CSV.dataframe_to_file(result['df_web'], 'df_web.csv')
    result['df_and'] = merge_adobe_google_platform(result['df'], constants.PLATFORM_ANDROID, variables['merge_columns'])
    f.CSV.dataframe_to_file(result['df_and'], 'df_and.csv')
    result['df_ios'] = merge_adobe_google_platform(result['df'], constants.PLATFORM_IOS, variables['merge_columns'])
    f.CSV.dataframe_to_file(result['df_ios'], 'df_ios.csv')

    # df > merge events data by platform and create csv
    result_events['df_web'] = merge_adobe_google_platform(result_events['df'], constants.PLATFORM_WEB, variables['merge_events_columns'])
    f.CSV.dataframe_to_file(result_events['df_web'], 'df_events_web.csv')
    result_events['df_and'] = merge_adobe_google_platform(result_events['df'], constants.PLATFORM_ANDROID, variables['merge_events_columns'])
    f.CSV.dataframe_to_file(result_events['df_and'], 'df_events_and.csv')
    result_events['df_ios'] = merge_adobe_google_platform(result_events['df'], constants.PLATFORM_IOS, variables['merge_events_columns'])
    f.CSV.dataframe_to_file(result_events['df_ios'], 'df_events_ios.csv')

print('> END EXECUTION')


