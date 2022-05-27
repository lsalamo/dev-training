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

class Google:
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
    variables['token'] = 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NTM2NjE2NzU5OTNfZjc4NTNjN2YtZmM4OC00MzhjLThjNDctNDAyNjg1ZDEzMzdmX2V3MSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1YThkY2MyY2ZhNzE0NzJjYmZhNGZiNTM2NzFjNDVlZCIsInVzZXJfaWQiOiI3NjREN0Y4RDVFQjJDRDUwMEE0OTVFMUJAMmRkMjM0Mzg1ZTYxMDdkNzBhNDk1Y2E0Iiwic3RhdGUiOiIiLCJhcyI6Imltcy1uYTEiLCJhYV9pZCI6Ijc2NEQ3RjhENUVCMkNENTAwQTQ5NUUxQkAyZGQyMzQzODVlNjEwN2Q3MGE0OTVjYTQiLCJjdHAiOjAsImZnIjoiV1BGU0hSMkNGUE41SUhVS0VNUUZRSFFBMjQ9PT09PT0iLCJzaWQiOiIxNjUzNjYxNjc1NjUyX2ZiNTI2NDkzLWIwYWItNGZjMS1iYmI1LWUwYTFlOTQ4OGY4Zl91ZTEiLCJydGlkIjoiMTY1MzY2MTY3NTk5NF9iMjdmYjcxMC0yNGZkLTQzMjktYmRhNy1jNWI2YmI3ZTcwNGZfZXcxIiwibW9pIjoiNTUyNDBhMjUiLCJwYmEiOiIiLCJydGVhIjoiMTY1NDg3MTI3NTk5NCIsImV4cGlyZXNfaW4iOiI4NjQwMDAwMCIsInNjb3BlIjoib3BlbmlkLEFkb2JlSUQscmVhZF9vcmdhbml6YXRpb25zLGFkZGl0aW9uYWxfaW5mby5wcm9qZWN0ZWRQcm9kdWN0Q29udGV4dCxhZGRpdGlvbmFsX2luZm8uam9iX2Z1bmN0aW9uIiwiY3JlYXRlZF9hdCI6IjE2NTM2NjE2NzU5OTMifQ.S43FuSIWOBYqY3bzacVJCulwRDmEAsDbiggFqtUpXr7cA9B7f8vX56CeJRM7RNjZauJdeJqGi8lSOSgicq3vH6W4ameTKt-pokunLutyMqBZ5dZlAmUc1JJZWNvEYnRqw71Enn-_DR0p19yu8Z-M3BH4Lu_EipkwUAaJ3C9baGGs_NwUXThlBabyayQn-OMe9WzM3pjpXNf4iY0EcbHSLiGfuNi0Vj7ewVLIon0mej7BTHwOEglX-SROItq5QaQCsMIkMHUs2rPHTeNAhB0ooHUuc_n3o2f4OwYHMOwLdI84Y7AXfV4wzKJQr7Yi9yuYvWlUOsLClR8zDVP9UkPQFw'
    variables['directory'] = '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/health-metrics-comparison'
    variables['from_date'] = '2022-05-01'
    variables['to_date'] = '2022-05-02'

# https://github.com/AdobeDocs/analytics-2.0-apis
# https://github.com/pitchmuc/adobe-analytics-api-2.0

    init()

    site = f_api.Adobe_Report_API.rs_cochesnet

    # Adobe Analytics > user and visits
    adobe = Adobe(site, variables['token'], variables['from_date'], variables['to_date'])
    result['df_aa'] = adobe.get_adobe()
    result_events['df_aa'] = adobe.get_adobe_events()

    # Google Analytics > user and visits
    result['df_ga'] = Google.get_google()
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


# result_events['df_web_count_ga_0'] = get_data_events_count_ga_0(result_events['df_web'], 'web-count-ga')
# result_events['df_web_count_ga_0_str'] = ','.join(result_events['df_web_count_ga_0']['event'])

print('> END EXECUTION')

# df_platform = df[['event2', 'and-aa', 'and-ga']]
# result_events['df_and_events'] = df_platform.loc[
#     ~((df_platform['and-count-aa'] == 0) & (df_platform['and-count-ga'] == 0))]
# df_platform = df[['event2', 'ios-aa', 'ios-ga']]
# result_events['df_ios_events'] = df_platform.loc[
#     ~((df_platform['ios-count-aa'] == 0) & (df_platform['ios-count-ga'] == 0))]

# result_events['df_aa_events_fc'] = result_events['df_aa_events'].loc[result_events['df_aa_events'].rs == variables['rs']['rs_fotocasaes']]

# summary = {}
# summary['count_aa_events_web'] = len(result_events['df_web_events'].loc[result_events['df_web_events']['web-aa'] > 0])
# summary['count_aa_events_and'] = len(result_events['df_and_events'].loc[result_events['df_and_events']['and-aa'] > 0])
# summary['count_aa_events_ios'] = len(result_events['df_ios_events'].loc[result_events['df_ios_events']['ios-aa'] > 0])

# summary['count_ga_events_web'] = len(result_events['df_web_events'].loc[result_events['df_web_events']['web-ga'] > 0])
# summary['count_ga_events_and'] = len(result_events['df_and_events'].loc[result_events['df_and_events']['and-ga'] > 0])
# summary['count_ga_events_ios'] = len(result_events['df_ios_events'].loc[result_events['df_ios_events']['ios-ga'] > 0])

# Events blocked by platform > pending to fix and split by plantform (not only web devices)
# summary['count_aa_events_web'] = len(result_events['df_web_events'].loc[result_events['df_web_events']['web-aa'] > 0])
# summary['count_aa_events_and'] = len(result_events['df_and_events'].loc[result_events['df_and_events']['and-aa'] > 0])
# summary['count_aa_events_ios'] = len(result_events['df_ios_events'].loc[result_events['df_ios_events']['ios-aa'] > 0])

# df = result_events['df_web_events']
# summary['df_new_events_web'] = df.loc[(df['web-aa'] == 0) & (df['web-ga'] > 0)]
# summary['count_new_events_web'] = len(summary['df_new_events_web'])
# df = result_events['df_and_events']
# summary['df_new_events_and'] = df.loc[(df['and-aa'] == 0) & (df['and-ga'] > 0)]
# summary['count_new_events_and'] = len(summary['df_new_events_and'])
# df = result_events['df_ios_events']
# summary['df_new_events_ios'] = df.loc[(df['ios-aa'] == 0) & (df['ios-ga'] > 0)]
# summary['count_new_events_ios'] = len(summary['df_new_events_ios'])

# df_sumary = pd.DataFrame(summary.items())

