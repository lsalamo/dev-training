import requests
import pandas as pd
import numpy as np
import os
import sys
import shutil


# =============================================================================
# VARIABLES
# =============================================================================

def init():
    # Change working directory
    os.chdir('/Users/luis.salamo/Documents/github enterprise/python-training/adobe/health-metrics-comparison')

    # Set list report suites
    data_values = variables['rs'].values()
    variables['list_rs'] = list(data_values)
    print('> init() -', 'loaded')


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================

def get_adobe_analytics_data():
    print('')
    file = open('aa/request.json')
    payload = file.read()
    file.close()
    print('> get_adobe_analytics_data() -', 'file payload loaded')

    url = 'https://analytics.adobe.io/api/schibs1/reports'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + variables['token'],
        'x-api-key': '5e9fd55fa92c4a0a82b3f2a74c088e60',
        'x-proxy-global-company-id': 'schibs1'
    }
    df = pd.DataFrame();
    for row in variables['rs'].values():
        print('> get_adobe_analytics_data() - rs:', row)
        response = requests.request('POST', url, headers=headers, data=payload.replace('{{rs}}', row))
        if response.status_code != 200:
            sys.exit('ERROR ' + str(response.status_code) + ' > ' + response.text)
        else:
            response = response.json()
            total_records = response['numberOfElements']
            if total_records > 0:
                df_request = pd.DataFrame.from_dict(response['rows'])
                df_request['rs'] = row
                df_request['day'] = pd.to_datetime(df_request['value']).dt.strftime('%Y%m%d')
                df_request[['web-visits', 'web-visitors', 'and-visits', 'and-visitors', 'ios-visits',
                            'ios-visitors']] = pd.DataFrame(df_request['data'].tolist(), index=df_request.index).astype(
                    'int64')
                df = pd.concat([df, df_request])
        print('> get_adobe_analytics_data() -', 'data loaded')

    # Clean dataframe
    df.drop(['itemId', 'data', 'value'], axis=1, inplace=True)
    df['day'] = df['day'].astype('str')
    print('> get_adobe_analytics_data() -', 'clean dataframe loaded')

    return df


def get_adobe_analytics_data_events(site):
    print('')
    file = open('aa/request-events.json')
    payload = file.read()
    file.close()
    print('> get_adobe_analytics_data_events() -', 'file payload loaded')

    url = 'https://analytics.adobe.io/api/schibs1/reports'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + variables['token'],
        'x-api-key': '5e9fd55fa92c4a0a82b3f2a74c088e60',
        'x-proxy-global-company-id': 'schibs1'
    }
    df = pd.DataFrame();
    for row in variables['rs'].values():
        if row == site:
            print('> get_adobe_analytics_data_events() - rs:', row)
            response = requests.request('POST', url, headers=headers, data=payload.replace('{{rs}}', row))
            if response.status_code != 200:
                sys.exit('ERROR ' + str(response.status_code) + ' > ' + response.text)
            else:
                response = response.json()
                total_records = response['numberOfElements']
                if total_records > 0:
                    df_request = pd.DataFrame.from_dict(response['rows'])
                    df_request['rs'] = row
                    df_request['event'] = df_request['value']
                    df_request[['web-count', 'web-visits', 'web-visitors', 'and-count', 'and-visits', 'and-visitors',
                                'ios-count', 'ios-visits', 'ios-visitors']] = pd.DataFrame(df_request['data'].tolist(),
                                                                                           index=df_request.index).astype(
                        'int64')
                    df = pd.concat([df, df_request])
        print('> get_adobe_analytics_data_events() -', 'data loaded')

    # Clean dataframe
    df.drop(['itemId', 'data', 'value'], axis=1, inplace=True)
    df['event2'] = df['event'].str.lower().replace(' ', '_', regex=True)
    print('> get_adobe_analytics_data_events() -', 'clean dataframe loaded')

    return df


# =============================================================================
#   REQUEST GOOGLE ANALYTICS
# =============================================================================

def get_google_analytics_data():
    print('')
    df = pd.DataFrame();
    dir = os.path.join(variables['directory'], 'ga')
    if os.path.isdir(dir):
        df = pd.read_csv(dir + '/data_health_metrics_comparison.csv', index_col=False)
    print('> get_google_analytics_data() -', 'data loaded')

    # Clean dataframe
    df.drop(df.index[:1], inplace=True)
    df.drop(['Totals', 'Totals.1'], axis=1, inplace=True)
    df.columns = ['day', 'web-visits', 'web-visitors', 'and-visits', 'and-visitors', 'ios-visits', 'ios-visitors']
    # df.columns = ['day', 'and-visits', 'and-visitors', 'web-visits', 'web-visitors', 'ios-visits', 'ios-visitors']
    df['day'] = df['day'].astype('int64').astype('str')

    print('> get_google_analytics_data() -', 'clean dataframe loaded')
    return df


def get_google_analytics_data_events():
    print('')
    df = pd.DataFrame();
    dir = os.path.join(variables['directory'], 'ga')
    if os.path.isdir(dir):
        df = pd.read_csv(dir + '/data-events.csv', header=None)
    print('> get_google_analytics_data_events() -', 'data loaded')

    # Clean dataframe
    df.drop(columns=[10, 11, 12], axis=1, inplace=True)
    values = ['web-count', 'web-visits', 'web-visitors', 'and-count', 'and-visits', 'and-visitors', 'ios-count',
              'ios-visits', 'ios-visitors']
    columns = values.copy()
    columns.insert(0, 'event2')
    df.columns = columns
    df[values] = df[values].astype(np.int64)
    print('> get_google_analytics_data_events() -', 'clean dataframe loaded')
    return df


# =============================================================================
#   JOIN
# =============================================================================

def get_data(rs):
    print('')
    df_aa = result['df_aa'].loc[result['df_aa'].rs == rs]
    df = df_aa.join(result['df_ga'].set_index('day'), on='day', lsuffix='-aa', rsuffix='-ga')

    result['df_web'] = df[['day', 'web-visits-aa', 'web-visitors-aa', 'web-visits-ga', 'web-visitors-ga']]
    result['df_android'] = df[['day', 'and-visits-aa', 'and-visitors-aa', 'and-visits-ga', 'and-visitors-ga']]
    result['df_ios'] = df[['day', 'ios-visits-aa', 'ios-visitors-aa', 'ios-visits-ga', 'ios-visitors-ga']]
    print('> get_data() -', 'data loaded')
    return df


def get_data_events():
    print('')
    df = pd.merge(result_events['df_aa'], result_events['df_ga'], on='event2', suffixes=('-aa', '-ga'), how='outer')

    # Clean dataframe
    df = df.fillna(0)
    values_aa = ['web-count-aa', 'web-visits-aa', 'web-visitors-aa', 'and-count-aa', 'and-visits-aa', 'and-visitors-aa',
                 'ios-count-aa', 'ios-visits-aa', 'ios-visitors-aa']
    df[values_aa] = df[values_aa].astype(np.int64)
    values_ga = ['web-count-ga', 'web-visits-ga', 'web-visitors-ga', 'and-count-ga', 'and-visits-ga', 'and-visitors-ga',
                 'ios-count-ga', 'ios-visits-ga', 'ios-visitors-ga']
    df[values_ga] = df[values_ga].astype(np.int64)
    df.drop(['rs', 'event'], axis=1, inplace=True)

    print('> get_data_events() -', 'data loaded')
    return df


def get_data_events_web():
    print('')
    df = result_events['df'][
        ['event2', 'web-count-aa', 'web-count-ga', 'web-visits-aa', 'web-visits-ga', 'web-visitors-aa',
         'web-visitors-ga']]
    df = df.loc[~((df['web-count-aa'] == 0) & (df['web-count-ga'] == 0))]
    df.sort_values(by='web-count-aa', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    print('> get_data_events_web() -', 'data loaded')
    return df


# =============================================================================
#   REQUEST GOOGLE ANALYTICS
# =============================================================================

def export_csv(df, file):
    dir = os.path.join(variables['directory'], 'csv')
    if os.path.isdir(dir):
        shutil.rmtree(dir)
    os.makedirs(dir)
    df.to_csv(dir + '/' + file)
    print('> ' + file + ' -', 'export loaded')


# =============================================================================
#   MAIN
# =============================================================================

result = {}
result_events = {}
variables = {}
variables['rs'] = {}
variables['token'] = 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NDkxNjQ3NjYxNzdfY2Y4MDllZGQtODM3ZS00NTIyLTg4MGItOGNkNDA2NTg1NGIwX2V3MSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1ZTlmZDU1ZmE5MmM0YTBhODJiM2YyYTc0YzA4OGU2MCIsInVzZXJfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiYXMiOiJpbXMtbmExIiwiYWFfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiY3RwIjowLCJmZyI6IldLVEY2Q0o2RkxFNUlQVUNFTVFGUkhRQVNBPT09PT09IiwibW9pIjoiZTY4MDFmYjUiLCJleHBpcmVzX2luIjoiODY0MDAwMDAiLCJzY29wZSI6Im9wZW5pZCxBZG9iZUlELHJlYWRfb3JnYW5pemF0aW9ucyxhZGRpdGlvbmFsX2luZm8ucHJvamVjdGVkUHJvZHVjdENvbnRleHQiLCJjcmVhdGVkX2F0IjoiMTY0OTE2NDc2NjE3NyJ9.ZQ3bBU2e9nNpH1MxdnMPFqjCbqQ0UECHySxnAn-f_ZZmWAsAiXWaS4hHq-uXlzS5khTrGUcMG4WJJGgpUJAs5zHyBCPiu6tft9oSVU-lnDe0DnWtQVNaUuabLc2OC4tuH93UOjTobfaFBPXw_ucLmLQI1u-bg3o_tViSmVKPijKkuA-i6El4PxAX_jrRtuEpFsxogbxvEpzqwuTQWST_SHcDjlDSLYW8m_qtr9W6Z4jr7L1q8OLfeQTtnIwLg2Mc8AiYmahfBOMxvSFTjsl-2Mh7UT6HY9hehcxL6OvQCBZMg1PrhIV_UYaswxURbnTr0hCVnyMmGFvncKkX4dKmgw'
variables['directory'] = '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/health-metrics-comparison'
variables['rs']['rs_fotocasaes'] = 'vrs_schibs1_fcall'
variables['rs']['rs_motosnet'] = 'vrs_schibs1_motorcochesnet'
variables['from_date'] = '2021-02-01'
variables['to_date'] = '2021-02-01'

init()

# result['df_aa'] = get_adobe_analytics_data()
# result['df_ga'] = get_google_analytics_data()
# result['df'] = get_data(variables['rs']['rs_fotocasaes'])

site = variables['rs']['rs_fotocasaes']
result_events['df_aa'] = get_adobe_analytics_data_events(site)
result_events['df_ga'] = get_google_analytics_data_events()
result_events['df'] = get_data_events()
result_events['df_web'] = get_data_events_web()
export_csv(result_events['df_web'], 'df_web.csv')

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
