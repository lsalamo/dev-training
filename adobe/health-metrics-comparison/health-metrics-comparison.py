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

    @staticmethod
    def clean_columns(df, platform, columns):
        columns = columns.replace('{{platform}}', platform).split(',')
        df.columns = columns
        if f_df.Dataframe.Columns.exists(df, 'day'):
            df['day'] = pd.to_datetime(df['day']).dt.strftime('%Y%m%d').astype('str')
        if f_df.Dataframe.Columns.exists(df, 'event'):
            df['event'] = df['event'].str.lower().replace(' ', '_', regex=True)
        df_values = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(' + platform + '-).*$')
        df[df_values.columns] = df_values
        return df

    def get_adobe(self):
        # request
        api = f_api.Adobe_Report_API(self.rs, self.token, 'aa/request.json', self.from_date, self.to_date)
        df = api.request()

        # web
        df_web = df[['value', 0, 1, 2]]
        df_web = Adobe.clean_columns(df_web, constants.PLATFORM_WEB, self.request_columns)
        # android
        df_and = df[['value', 3, 4, 5]]
        df_and = Adobe.clean_columns(df_and, constants.PLATFORM_ANDROID, self.request_columns)
        # ios
        df_ios = df[['value', 6, 7, 8]]
        df_ios = Adobe.clean_columns(df_ios, constants.PLATFORM_IOS, self.request_columns)
        # join dataframes
        frames = [df_web, df_and, df_ios]
        df = f_df.Dataframe.Columns.join_by_columns(frames, ['day'], 'outer')

        f.Log.print('get_adobe', 'dataframe loaded')
        return df

    def get_adobe_events(self):
        # request
        api = f_api.Adobe_Report_API(self.rs, self.token, 'aa/request-events.json', self.from_date, self.to_date)
        df = api.request()

        # web
        df_web = df[['value', 0, 1, 2]]
        df_web = Adobe.clean_columns(df_web, constants.PLATFORM_WEB, self.request_events_columns)
        # android
        df_and = df[['value', 3, 4, 5]]
        df_and = Adobe.clean_columns(df_and, constants.PLATFORM_ANDROID, self.request_events_columns)
        # ios
        df_ios = df[['value', 6, 7, 8]]
        df_ios = Adobe.clean_columns(df_ios, constants.PLATFORM_IOS, self.request_events_columns)
        # join dataframes
        frames = [df_web, df_and, df_ios]
        df = f_df.Dataframe.Columns.join_by_columns(frames, ['event'], 'outer')

        f.Log.print('get_adobe_events', 'dataframe loaded')
        return df


# =============================================================================
#   REQUEST GOOGLE ANALYTICS
# =============================================================================

class Google:
    def __init__(self, request_columns, request_events_columns):
        self.request_columns = request_columns
        self.request_events_columns = request_events_columns

    @staticmethod
    def csv_to_dataframe(file):
        df = f.CSV.csv_to_dataframe(file)
        if df.empty:
            f.Log.print_error('google.csv_to_dataframe', 'file csv not loaded - <' + file + '>')
        else:
            f.Log.print('google.csv_to_dataframe', 'file csv loaded - <' + file + '>')
        return df

    @staticmethod
    def clean_columns(df, platform, columns):
        columns = columns.replace('{{platform}}', platform).split(',')
        f_df.Dataframe.Columns.drop_from_index(df, len(columns), True)
        df.columns = columns
        if f_df.Dataframe.Columns.exists(df, 'day'):
            df['day'] = df['day'].astype('str')
        return df


class GoogleAPI(Google):
    def __init__(self, request_columns, request_events_columns, property_id, token, from_date, to_date):
        super().__init__(request_columns, request_events_columns)
        self.request_columns = request_columns
        self.property_id = property_id
        self.token = token
        self.from_date = from_date
        self.to_date = to_date

    def get_google(self):
        # web
        api = f_api.Google_Report_API(self.property_id, self.token, 'ga/request.json', self.from_date, self.to_date, constants.PLATFORM_WEB)
        df = api.request()
        df[['day', 'platform']] = f_df.Dataframe.Columns.split_into_columns(df, 'dimensionValues', ',')
        df['day'] = pd.to_datetime(df['day']).dt.strftime('%Y%m%d')
        df = f_df.Dataframe.Columns.join_two_frames_by_index(df, f_df.Dataframe.Columns.split_into_columns(df, 'metricValues', ','), 'inner')
        return df


class GoogleCSV(Google):
    def __init__(self, request_columns, request_events_columns):
        super().__init__(request_columns, request_events_columns)
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
                 'token_aa': 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NTUxMzM4OTIyNjBfZTMzY2Q1MTYtZTk2MC00ZWNkLWI4ZDMtYWQxNjI5YWQ4ODM1X2V3MSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1YThkY2MyY2ZhNzE0NzJjYmZhNGZiNTM2NzFjNDVlZCIsInVzZXJfaWQiOiI3NjREN0Y4RDVFQjJDRDUwMEE0OTVFMUJAMmRkMjM0Mzg1ZTYxMDdkNzBhNDk1Y2E0Iiwic3RhdGUiOiIiLCJhcyI6Imltcy1uYTEiLCJhYV9pZCI6Ijc2NEQ3RjhENUVCMkNENTAwQTQ5NUUxQkAyZGQyMzQzODVlNjEwN2Q3MGE0OTVjYTQiLCJjdHAiOjAsImZnIjoiV1FWUFlSWEpGUE41SUhVS0VNUUZRSFFBMjQ9PT09PT0iLCJzaWQiOiIxNjU1MTMzODkxOTI0X2U0N2ZmMzlhLTRiNWYtNDM5Yy1hNGU5LTQwYzZlMzdmZWFiNF91ZTEiLCJydGlkIjoiMTY1NTEzMzg5MjI2MV9hYTE1N2Y2Yy0wNjU0LTRlMjUtYTU5Yy0xMmU0OWNmZGJmZjRfZXcxIiwibW9pIjoiZjFjZDc5ZmYiLCJwYmEiOiIiLCJydGVhIjoiMTY1NjM0MzQ5MjI2MSIsImV4cGlyZXNfaW4iOiI4NjQwMDAwMCIsImNyZWF0ZWRfYXQiOiIxNjU1MTMzODkyMjYwIiwic2NvcGUiOiJvcGVuaWQsQWRvYmVJRCxyZWFkX29yZ2FuaXphdGlvbnMsYWRkaXRpb25hbF9pbmZvLnByb2plY3RlZFByb2R1Y3RDb250ZXh0LGFkZGl0aW9uYWxfaW5mby5qb2JfZnVuY3Rpb24ifQ.IbKHPcWh_V3VYm34V35vZYr72BXt-1RVJu4oT49rr02D7kdGBoZdQGVLS6IUTiC03jiE4SrCFxJTIDfcSD7IpgJEBBabOCXbpAKKfhjes8qUXHy_fJjVG2N4dBA8u2wFGWvbxOa04JrgoyADNR0Eh-OMe1gRvNQ3G9t1-l-YHLGzos9CUZ5QU_HJd_DO_2_Hl90CmzRLyW5csJ8m6p6T1mhXwPYVuhZOHBR6AzKMtc3kvxe9Xt_tn_0qJ2dPoyl6BIjRDf6m4juGkSBPKa93YcluTLuCeqHWk0wWscaVe-rb8YTtwhUiDKco8jwlKumYdiinCK_aIcFizfN8wYa7Dw',
                 'token_ga': 'ya29.a0ARrdaM-p_58RV4q9wyw8RiB6ZT4Pg-631tlaLc6_mv5grU5A8aBNAGhMytzwXfgvs5B8YjPnBcZGK3sFtemJY_9zouxrQKs9QWuO9PZ0tyFc_ID61O8yNmkAV-5TaYG0PRSC-bSF2X1Te4GAOdbFwI-S3nPdDEE',
                 'directory': '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/health-metrics-comparison',
                 'from_date': '2022-05-28',
                 'to_date': '2022-06-06',
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
    # google = GoogleAPI(variables['request_columns'], variables['request_events_columns'], variables['site_ga'], variables['token_ga'], variables['from_date'], variables['to_date'])
    google = GoogleCSV(variables['request_columns'], variables['request_events_columns'])
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


