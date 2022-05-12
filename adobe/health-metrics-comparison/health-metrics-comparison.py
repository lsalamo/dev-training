import requests
import pandas as pd
import numpy as np
import os
import sys
import shutil
import sys

# adding libraries folder to the system path
sys.path.insert(0, '/Users/luis.salamo/Documents/github enterprise/python-training/libraries')

import constants
import functions as lse_functions

# from functions import API,decir_hola

# =============================================================================
# VARIABLES
# =============================================================================


def init():
    lse_functions.Directory.change_working_directory(variables['directory'])
    lse_functions.Directory.create_directory('csv')
    lse_functions.Log.print('init', 'loaded')


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================

def get_adobe(rs):
    # file payload
    file = lse_functions.File('aa/request.json')
    payload = file.read_file()
    payload = payload.replace('{{rs}}', rs)
    lse_functions.Log.print('get_adobe', 'file payload loaded')

    # request api
    api = lse_functions.Adobe_Report_API(variables['token'], payload)
    df = api.request()
    df['day'] = pd.to_datetime(df['value']).dt.strftime('%Y%m%d')
    df[['web-visits', 'web-visitors', 'and-visits', 'and-visitors', 'ios-visits','ios-visitors']] = \
        pd.DataFrame(df['data'].tolist(), index=df.index).astype('int64')
    lse_functions.Log.print('get_adobe', 'request api loaded')

    # Clean dataframe
    df.drop(['itemId', 'data', 'value'], axis=1, inplace=True)
    df['day'] = df['day'].astype('str')
    lse_functions.Log.print('get_adobe', 'dataframe cleaned')

    return df


def get_adobe_events(rs):
    # file payload
    file = lse_functions.File('aa/request-events.json')
    payload = file.read_file()
    payload = payload.replace('{{rs}}', rs)
    lse_functions.Log.print('get_adobe_events', 'file payload loaded')

    # request api
    api = lse_functions.Adobe_Report_API(variables['token'], payload)
    df = api.request()
    df['event'] = df['value']
    df[['web-count', 'web-visits', 'web-visitors', 'and-count', 'and-visits', 'and-visitors',
                'ios-count', 'ios-visits', 'ios-visitors']] = pd.DataFrame(df['data'].tolist(),
                                                                           index=df.index).astype('int64')
    lse_functions.Log.print('get_adobe_events', 'request api loaded')

    # Clean dataframe
    df.drop(['itemId', 'data', 'value'], axis=1, inplace=True)
    df['event2'] = df['event'].str.lower().replace(' ', '_', regex=True)
    lse_functions.Log.print('get_adobe_events', 'dataframe cleaned')

    return df


# =============================================================================
#   REQUEST GOOGLE ANALYTICS
# =============================================================================

class Google:
    def __init__(self, platform, file):
        self.platform = platform
        self.file = file

    def csv_to_dataframe(self):
        df = lse_functions.CSV.csv_to_dataframe(self.file)
        if df.empty:
            lse_functions.Log.print_and_exit('google.csv_to_dataframe', 'file csv not loaded - <' + file + '>')
        else:
            lse_functions.Log.print('google.csv_to_dataframe', 'file csv loaded - <' + file + '>')
        return df

    def get_google(self):
        # csv_to_dataframe
        df = self.csv_to_dataframe()

        # clean dataframe
        df.drop(columns=[3, 4], axis=1, inplace=True)
        values = [self.platform + '-visits', self.platform + '-visitors']
        columns = values.copy()
        columns.insert(0, 'day')
        df.columns = columns
        df[values] = df[values].astype(np.int64)
        df['day'] = df['day'].astype('str')
        return df

    def get_google_events(self):
        # csv_to_dataframe
        df = self.csv_to_dataframe()

        # clean dataframe
        df.drop(columns=[4, 5, 6], axis=1, inplace=True)
        values = [self.platform + '-count', self.platform + '-visits', self.platform + '-visitors']
        columns = values.copy()
        columns.insert(0, 'event2')
        df.columns = columns
        df[values] = df[values].astype(np.int64)
        return df


# =============================================================================
#   JOIN
# =============================================================================

def merge_adobe_google():
    df = pd.merge(result['df_aa'], result['df_ga'], on='day', suffixes=('-aa', '-ga'), how='outer')
    lse_functions.Log.print('merge_adobe_google', 'data loaded')
    return df


def merge_adobe_google_platform(platform):
    df = result['df'][
        ['day', platform + '-visits-aa', platform + '-visits-ga', platform + '-visitors-aa', platform + '-visitors-ga']]
    lse_functions.Log.print('merge_adobe_google_platform', platform + ' - data loaded')
    return df


def merge_events_adobe_google():
    df = pd.merge(result_events['df_aa'], result_events['df_ga'], on='event2', suffixes=('-aa', '-ga'), how='outer')

    # Clean dataframe
    df = df.fillna(0)
    values_aa = ['web-count-aa', 'web-visits-aa', 'web-visitors-aa', 'and-count-aa', 'and-visits-aa', 'and-visitors-aa',
                 'ios-count-aa', 'ios-visits-aa', 'ios-visitors-aa']
    df[values_aa] = df[values_aa].astype(np.int64)
    values_ga = ['web-count-ga', 'web-visits-ga', 'web-visitors-ga', 'and-count-ga', 'and-visits-ga', 'and-visitors-ga',
                 'ios-count-ga', 'ios-visits-ga', 'ios-visitors-ga']
    df[values_ga] = df[values_ga].astype(np.int64)
    lse_functions.Log.print('merge_events_adobe_google', 'data loaded')
    return df


def merge_events_adobe_google_platform(platform):
    df = result_events['df'][
        ['event', 'event2', platform + '-count-aa', platform + '-count-ga', platform + '-visits-aa',
         platform + '-visits-ga', platform + '-visitors-aa',
         platform + '-visitors-ga']]
    df = df.loc[~((df['web-count-aa'] == 0) & (df['web-count-ga'] == 0))]
    df.sort_values(by=platform + '-count-aa', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    lse_functions.Log.print('merge_events_adobe_google_platform', platform + ' - data loaded')
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


result = {}
result_events = {}
variables = {}
variables['rs'] = {}
variables[
    'token'] = 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NTIzNjg5OTI3NDBfY2UyYjI1ZTAtZmRmNC00ZDRhLWI1M2EtZGIyYzJiZGViNmIxX3VlMSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1YThkY2MyY2ZhNzE0NzJjYmZhNGZiNTM2NzFjNDVlZCIsInVzZXJfaWQiOiI3NjREN0Y4RDVFQjJDRDUwMEE0OTVFMUJAMmRkMjM0Mzg1ZTYxMDdkNzBhNDk1Y2E0Iiwic3RhdGUiOiIiLCJhcyI6Imltcy1uYTEiLCJhYV9pZCI6Ijc2NEQ3RjhENUVCMkNENTAwQTQ5NUUxQkAyZGQyMzQzODVlNjEwN2Q3MGE0OTVjYTQiLCJjdHAiOjAsImZnIjoiV04zUFhaVTVGUE41SVBVS0VNUUZSSFFBWUE9PT09PT0iLCJzaWQiOiIxNjUyMzY4OTkyMzM0XzJkZGYwNmExLTMwMzQtNGI2Zi1iZTJlLWJjMjNiMmVlYTBhM191ZTEiLCJydGlkIjoiMTY1MjM2ODk5Mjc0MV9iYjZhMjVjNi05NmM5LTQzYzYtODMxOC0wNTdhMzdiOWI4NjFfdWUxIiwibW9pIjoiZmVkMDJlMjciLCJwYmEiOiIiLCJydGVhIjoiMTY1MzU3ODU5Mjc0MSIsIm9jIjoicmVuZ2EqbmExcioxODBiOGRkZTZlZio2Nlk5R1dCOUhONUdIQ0pGR0IyTjNCR0daUiIsImV4cGlyZXNfaW4iOiI4NjQwMDAwMCIsImNyZWF0ZWRfYXQiOiIxNjUyMzY4OTkyNzQwIiwic2NvcGUiOiJvcGVuaWQsQWRvYmVJRCxyZWFkX29yZ2FuaXphdGlvbnMsYWRkaXRpb25hbF9pbmZvLnByb2plY3RlZFByb2R1Y3RDb250ZXh0LGFkZGl0aW9uYWxfaW5mby5qb2JfZnVuY3Rpb24ifQ.hA0N6mPKFSe46KLJemTQx7E1yvQuqYRXczstrIg_TdDFtnLv9mt_58C3unl3xcW54bpZgzvk9kEpsSkkg2RWzhHrmRadWmW0NTYn-qZBfAcLM0hKbjDbQnGBbkAoG1BMqBSUxviCaAsxO9-6DY6l3PL3XuXvvjK3A8wkQCg76giYJRqiohcqd2a_AtbuBVit7P8wOOBSMIVkSlZm2Cp7mRIEFq8fADYAEA2xqv7s5KL4UoimWo9RqrCDS5OeVCI6qAfdGcWFIcfcByZQXe41-PR8OxwUPUCm6lluVBOQsjdszmwKCSV9jfeEfhvDXgrTJ9p9gDBYrUKDRDprtX6TWw'
variables['directory'] = '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/health-metrics-comparison'
variables['rs']['rs_fotocasaes'] = 'vrs_schibs1_fcall'
variables['rs']['rs_motosnet'] = 'vrs_schibs1_motorcochesnet'
variables['from_date'] = '2021-02-01'
variables['to_date'] = '2021-02-01'

init()

site = variables['rs']['rs_fotocasaes']
# Adobe Analytics > user and visits
result['df_aa'] = get_adobe(site)
# GA4 WEB > user and visits
file = 'ga/data-' + constants.PLATFORM_WEB + '.csv'
google = Google(constants.PLATFORM_WEB, file)
result['df_ga'] = google.get_google()
# GA4 AND > user and visits
file = 'ga/data-' + constants.PLATFORM_ANDROID + '.csv'
google = Google(constants.PLATFORM_ANDROID, file)
df = google.get_google()
result['df_ga'] = pd.merge(result['df_ga'], df, on='day', how='outer')
# GA4 IOS > user and visits
file = 'ga/data-' + constants.PLATFORM_IOS + '.csv'
google = Google(constants.PLATFORM_IOS, file)
df = google.get_google()
result['df_ga'] = pd.merge(result['df_ga'], df, on='day', how='outer')
# df > merge data
result['df'] = merge_adobe_google()
# df web, and, ios > user and visits
result['df_web'] = merge_adobe_google_platform(constants.PLATFORM_WEB)
lse_functions.CSV.dataframe_to_file(result['df_web'], 'df_web.csv')
result['df_and'] = merge_adobe_google_platform(constants.PLATFORM_ANDROID)
lse_functions.CSV.dataframe_to_file(result['df_and'], 'df_and.csv')
result['df_ios'] = merge_adobe_google_platform(constants.PLATFORM_IOS)
lse_functions.CSV.dataframe_to_file(result['df_ios'], 'df_ios.csv')

# Adobe Analytics > events
result_events['df_aa'] = get_adobe_events(site)
# GA4 > events
file = 'ga/data-events-' + constants.PLATFORM_WEB + '.csv'
google = Google(constants.PLATFORM_WEB, file)
result_events['df_ga'] = google.get_google_events()
result_events['df_ga']['and-count'] = 0
result_events['df_ga']['and-visits'] = 0
result_events['df_ga']['and-visitors'] = 0
result_events['df_ga']['ios-count'] = 0
result_events['df_ga']['ios-visits'] = 0
result_events['df_ga']['ios-visitors'] = 0
# df > merge data
result_events['df'] = merge_events_adobe_google()
# df web, and, ios > user and visits
result_events['df_web'] = merge_events_adobe_google_platform('web')
lse_functions.CSV.dataframe_to_file(result_events['df_web'], 'df_events_web.csv')


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

