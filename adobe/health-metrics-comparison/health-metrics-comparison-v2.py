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
import dataframe as f_dataframe


def init():
    f.Directory.change_working_directory(variables['directory'])
    f.Directory.create_directory('csv')
    f.Log.print('init', 'loaded')


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================
class Adobe:
    def __init__(self, rs, token, from_date, to_date):
        self.rs = rs
        self.token = token
        self.from_date = from_date
        self.to_date = to_date

    def get_adobe(self):
        # request api
        api = f_api.Adobe_Report_API(self.rs, self.token, 'aa/request.json', self.from_date, self.to_date)
        df = api.request()
        df['day'] = pd.to_datetime(df['value']).dt.strftime('%Y%m%d')
        df[['web-visits', 'web-visitors', 'web-pageviews', 'and-visits', 'and-visitors', 'and-pageviews', 'ios-visits', 'ios-visitors', 'ios-pageviews']] = \
            pd.DataFrame(df['data'].tolist(), index=df.index).astype('int64')
        f.Log.print('get_adobe', 'request api loaded')

        # Clean dataframe
        df.drop(['itemId', 'data', 'value'], axis=1, inplace=True)
        df['day'] = df['day'].astype('str')
        f.Log.print('get_adobe', 'dataframe cleaned')

        return df

    def get_adobe_events(self):
        # request api
        api = f_api.Adobe_Report_API(self.rs, self.token, 'aa/request-events.json', self.from_date, self.to_date)
        df = api.request()
        df['event'] = df['value']
        df[['web-count', 'web-visits', 'web-visitors', 'and-count', 'and-visits', 'and-visitors',
                    'ios-count', 'ios-visits', 'ios-visitors']] = pd.DataFrame(df['data'].tolist(),
                                                                               index=df.index).astype('int64')
        f.Log.print('get_adobe_events', 'request api loaded')

        # Clean dataframe
        df.drop(['itemId', 'data', 'value'], axis=1, inplace=True)
        df['event2'] = df['event'].str.lower().replace(' ', '_', regex=True)
        f.Log.print('get_adobe_events', 'dataframe cleaned')

        return df


# =============================================================================
#   REQUEST GOOGLE ANALYTICS
# =============================================================================

class Google2:
    def __init__(self, property, token, from_date, to_date):
        self.property = property
        self.token = token
        self.from_date = from_date
        self.to_date = to_date

    def get_google(self):
        # request api
        api = f_api.Google_Report_API(self.property, self.token, 'ga/request.json', self.from_date, self.to_date)
        df = api.request()
        df['day'] = pd.to_datetime(df['dimensionValues']).dt.strftime('%Y%m%d')
        df['metricValues'] = df['metricValues'].str.split(',')
        df[['web-pageviews', 'web-visits', 'web-visitors', 'and-visits', 'and-visitors', 'and-pageviews', 'ios-visits', 'ios-visitors', 'ios-pageviews']] = pd.DataFrame(df['metricValues'].tolist(), index=df.index).astype('int64')
        return df
        # df['day'] = pd.to_datetime(df['value']).dt.strftime('%Y%m%d')
        # df[['web-visits', 'web-visitors', 'web-pageviews', 'and-visits', 'and-visitors', 'and-pageviews', 'ios-visits', 'ios-visitors', 'ios-pageviews']] = \
        #     pd.DataFrame(df['data'].tolist(), index=df.index).astype('int64')
        # f.Log.print('get_adobe', 'request api loaded')
        # 
        # # Clean dataframe
        # df.drop(['itemId', 'data', 'value'], axis=1, inplace=True)
        # df['day'] = df['day'].astype('str')
        # f.Log.print('get_adobe', 'dataframe cleaned')        


class Google:
    def __init__(self):
        pass

    @staticmethod
    def csv_to_dataframe(file):
        df = f.CSV.csv_to_dataframe(file)
        if df.empty:
            f.Log.print_error('google.csv_to_dataframe', 'file csv not loaded - <' + file + '>')
        else:
            f.Log.print('google.csv_to_dataframe', 'file csv loaded - <' + file + '>')
        return df

    @staticmethod
    def clean_dataframe(df, platform):
        df.drop(columns=[3, 4], axis=1, inplace=True)
        columns = ['day', platform + '-visits', platform + '-visitors']
        df.columns = columns
        df['day'] = df['day'].astype('str')
        return df

    @staticmethod
    def clean_dataframe_events(df, platform):
        df.drop(columns=[4, 5, 6], axis=1, inplace=True)
        columns = ['event2', platform + '-count', platform + '-visits', platform + '-visitors']
        df.columns = columns
        return df

    @staticmethod
    def get_google():
        file = 'ga/data-' + constants.PLATFORM_WEB + '.csv'
        df_web = Google.csv_to_dataframe(file)
        df_web = Google.clean_dataframe(df_web, constants.PLATFORM_WEB)
        file = 'ga/data-' + constants.PLATFORM_ANDROID + '.csv'
        df_and = Google.csv_to_dataframe(file)
        df_and = Google.clean_dataframe(df_and, constants.PLATFORM_ANDROID)
        file = 'ga/data-' + constants.PLATFORM_IOS + '.csv'
        df_ios = Google.csv_to_dataframe(file)
        df_ios = Google.clean_dataframe(df_ios, constants.PLATFORM_IOS)

        # join dataframes
        frames = [df_web, df_and, df_ios]
        df = f_dataframe.Dataframe.Columns.join_by_columns(frames, ['day'], 'outer')
        return df

    @staticmethod
    def get_google_events():
        file = 'ga/data-events-' + constants.PLATFORM_WEB + '.csv'
        df_web = Google.csv_to_dataframe(file)
        df_web = Google.clean_dataframe_events(df_web, constants.PLATFORM_WEB)
        file = 'ga/data-events-' + constants.PLATFORM_ANDROID + '.csv'
        df_and = Google.csv_to_dataframe(file)
        df_and = Google.clean_dataframe_events(df_and, constants.PLATFORM_ANDROID)
        file = 'ga/data-events-' + constants.PLATFORM_IOS + '.csv'
        df_ios = Google.csv_to_dataframe(file)
        df_ios = Google.clean_dataframe_events(df_ios, constants.PLATFORM_IOS)

        # join dataframes
        frames = [df_web, df_and, df_ios]
        df = f_dataframe.Dataframe.Columns.join_by_columns(frames, ['event2'], 'outer')
        return df


# =============================================================================
#   JOIN
# =============================================================================

def merge_adobe_google(frames, columns):
    df = f_dataframe.Dataframe.Columns.join_two_frames_by_columns(frames['df_aa'], frames['df_ga'], columns, 'outer', ('-aa', '-ga'))
    df = df.fillna(0)

    # cast values to int64
    columns = f_dataframe.Dataframe.Columns.columns_names(df)
    r = re.compile('(-aa|-ga)$')
    columns_values = list(filter(r.search, columns))
    df[columns_values] = df[columns_values].astype(np.int64)
    f.Log.print('merge_adobe_google', 'data loaded')
    return df


def merge_adobe_google_platform(platform):
    df = result['df'][['day', platform + '-visits-aa', platform + '-visits-ga', platform + '-visitors-aa', platform + '-visitors-ga']]
    f.Log.print('merge_adobe_google_platform', platform + ' - data loaded')
    return df


def merge_events_adobe_google_platform(platform):
    df = result_events['df'][
        ['event2', platform + '-count-aa', platform + '-count-ga', platform + '-visits-aa',
         platform + '-visits-ga', platform + '-visitors-aa',
         platform + '-visitors-ga']]
    df = df.loc[~((df[platform + '-count-aa'] == 0) & (df[platform + '-count-ga'] == 0))]
    df.sort_values(by=platform + '-count-aa', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    f.Log.print('merge_events_adobe_google_platform', platform + ' - data loaded')
    return df


def get_data_events_count_ga_0(df_source, field):
    print('')
    df = df_source.loc[df_source[field] == 0]
    df.reset_index(drop=True, inplace=True)
    print('> get_data_events_count_ga_0() -', 'data loaded')
    return df


# =============================================================================
#   MAIN
# =============================================================================

# main function
if __name__ == '__main__': 
    
    result = {}
    result_events = {}
    variables = {}
    variables['rs'] = {}
    variables['token_aa'] = 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NTM3Nzc0MzY2NTNfNDNkNGZmMGUtZTkzYS00YzUwLTk0YzctYTMwOGUzZGY2ZTBkX2V3MSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1YThkY2MyY2ZhNzE0NzJjYmZhNGZiNTM2NzFjNDVlZCIsInVzZXJfaWQiOiI3NjREN0Y4RDVFQjJDRDUwMEE0OTVFMUJAMmRkMjM0Mzg1ZTYxMDdkNzBhNDk1Y2E0Iiwic3RhdGUiOiIiLCJhcyI6Imltcy1uYTEiLCJhYV9pZCI6Ijc2NEQ3RjhENUVCMkNENTAwQTQ5NUUxQkAyZGQyMzQzODVlNjEwN2Q3MGE0OTVjYTQiLCJjdHAiOjAsImZnIjoiV1BKSzNSMkNGUE41SUhVS0VNUUZRSFFBMjQ9PT09PT0iLCJzaWQiOiIxNjUzNzc3NDM2MjY1XzNkY2RkMjYzLWMwOTYtNDcyYS1hYTQ1LWFiNWJkOWEwOWU0MF91ZTEiLCJydGlkIjoiMTY1Mzc3NzQzNjY1NV9hOTE3NjI2Yy02MjQxLTRkNzctYTA3My00NTg5ODk3YWU2N2VfZXcxIiwibW9pIjoiOTc1NDM5ZmEiLCJwYmEiOiIiLCJydGVhIjoiMTY1NDk4NzAzNjY1NSIsImV4cGlyZXNfaW4iOiI4NjQwMDAwMCIsInNjb3BlIjoib3BlbmlkLEFkb2JlSUQscmVhZF9vcmdhbml6YXRpb25zLGFkZGl0aW9uYWxfaW5mby5wcm9qZWN0ZWRQcm9kdWN0Q29udGV4dCxhZGRpdGlvbmFsX2luZm8uam9iX2Z1bmN0aW9uIiwiY3JlYXRlZF9hdCI6IjE2NTM3Nzc0MzY2NTMifQ.MZUi-DJ449P20wwGZXWUM9lMnLZXIk4eS3TLX0XBGCmYLcEJdB8CwsCzDMcRsONaeQ8NzvREJOmzkqyu6v9glYWsCXBXx3fdGTwyifDqi2qcAJggomD2jxtKLzyoCLkQcO5JjHN0Yj382zmzRUpDA5SKle--CIvar5y5YjK6IqJtNxgkg-Np2OM58XZof7uLpnaC39UwTUK5Vyt4_9Susk1wSpMZNKbHS7Iiu5fsoYKVztB122CK5loKfSApd7FjJHSrhjK-d-yDIZQH2lL4Ubqnd7N5rv4_AjnmlmuEiUYYBGQi6yPpoEVgvnVgBtVy1aJM7ThAxA7x2Lr7AJx0RQ'
    variables['token_ga'] = 'ya29.a0ARrdaM_OQWY2dU36gaIMYxFvRQ1139ER13Lp0RwE9rm-kbSN2XlbQS8qingZ7D5fiWJKzOJvvr8LG0eHpFxHGBFQ2j7DRpFLPa3tfwKZxhyeKC8gBGmiQwe5qnejUL9YcDHmPxiCxom3WRsMIEfyFhJEDazyqgU'
    variables['directory'] = '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/health-metrics-comparison'
    variables['from_date'] = '2022-05-25'
    variables['to_date'] = '2022-05-26'

# https://github.com/AdobeDocs/analytics-2.0-apis
# https://github.com/pitchmuc/adobe-analytics-api-2.0

    init()

    site = f_api.Adobe_Report_API.rs_cochesnet
    property_id = f_api.Google_Report_API.property_cochesnet

    # Adobe Analytics > user and visits
    adobe = Adobe(site, variables['token_aa'], variables['from_date'], variables['to_date'])
    result['df_aa'] = adobe.get_adobe()
    result_events['df_aa'] = adobe.get_adobe_events()

    # Google Analytics > user and visits
    google = Google2(property_id, variables['token_ga'], variables['from_date'], variables['to_date'])
    result['df_ga'] = google.get_google()
    result_events['df_ga'] = Google.get_google_events()

    # df > merge data
    result['df'] = merge_adobe_google(result, ['day'])
    result_events['df'] = merge_adobe_google(result_events, ['event2'])

    # df > merge data by platform and create csv
    result['df_web'] = merge_adobe_google_platform(constants.PLATFORM_WEB)
    f.CSV.dataframe_to_file(result['df_web'], 'df_web.csv')
    result['df_and'] = merge_adobe_google_platform(constants.PLATFORM_ANDROID)
    f.CSV.dataframe_to_file(result['df_and'], 'df_and.csv')
    result['df_ios'] = merge_adobe_google_platform(constants.PLATFORM_IOS)
    f.CSV.dataframe_to_file(result['df_ios'], 'df_ios.csv')

    # df > merge events data by platform and create csv
    result_events['df_web'] = merge_events_adobe_google_platform(constants.PLATFORM_WEB)
    f.CSV.dataframe_to_file(result_events['df_web'], 'df_events_web.csv')
    result_events['df_and'] = merge_events_adobe_google_platform(constants.PLATFORM_ANDROID)
    f.CSV.dataframe_to_file(result_events['df_and'], 'df_events_and.csv')
    result_events['df_ios'] = merge_events_adobe_google_platform(constants.PLATFORM_IOS)
    f.CSV.dataframe_to_file(result_events['df_ios'], 'df_events_ios.csv')

    print('> END EXECUTION')


