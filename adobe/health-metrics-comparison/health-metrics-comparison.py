import requests
import pandas as pd
import os
import sys

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
                df_request[['web-visits', 'web-visitors', 'and-visits', 'and-visitors', 'ios-visits', 'ios-visitors']] = pd.DataFrame(df_request['data'].tolist(), index= df_request.index).astype('int64')
                df = pd.concat([df, df_request])
        print('> get_adobe_analytics_data() -', 'data loaded')
        
    
    # Clean dataframe
    df.drop(['itemId', 'data', 'value'], axis=1, inplace=True)
    df['day'] = df['day'].astype('str')
    print('> get_adobe_analytics_data() -', 'clean dataframe loaded')
    
    return df

def get_adobe_analytics_data_events():
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
                df_request[['web', 'and', 'ios']] = pd.DataFrame(df_request['data'].tolist(), index= df_request.index).astype('int64')
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
        df = pd.read_csv(dir + '/data_health_metrics_comparison_events.csv', index_col=False)
    print('> get_google_analytics_data_events() -', 'data loaded')
    
    # Clean dataframe
    df.drop(df.index[:1], inplace=True)
    df.drop(['Totals'], axis=1, inplace=True)
    df.columns = ['event2', 'web', 'and', 'ios']
    df['web'] = df['web'].astype('int64')
    df['and'] = df['and'].astype('int64')
    df['ios'] = df['ios'].astype('int64')

    print('> get_google_analytics_data_events() -', 'clean dataframe loaded')
    return df

# =============================================================================
#   JOIN
# =============================================================================

def get_data(rs):
    print('')
    df_aa = result['df_aa'].loc[result['df_aa'].rs == rs]
    df = df_aa.join(result['df_ga'].set_index('day'), on='day',lsuffix='-aa', rsuffix='-ga')
    
    result['df_web'] = df[['day', 'web-visits-aa', 'web-visitors-aa', 'web-visits-ga', 'web-visitors-ga']]
    result['df_android'] = df[['day', 'and-visits-aa', 'and-visitors-aa', 'and-visits-ga', 'and-visitors-ga']]
    result['df_ios'] = df[['day', 'ios-visits-aa', 'ios-visitors-aa', 'ios-visits-ga', 'ios-visitors-ga']]    
    print('> get_data() -', 'data loaded')
    return df

def get_data_events(df_aa):
    print('')    
    df = pd.merge(df_aa, result_events['df_ga_events'], on='event2',suffixes=('-aa', '-ga'), how='outer')
      
    # Clean dataframe
    df = df.fillna(0)
    df = df.astype({'web-aa':'int64', 'web-ga':'int64', 'and-aa':'int64', 'and-ga':'int64', 'ios-aa':'int64', 'ios-ga':'int64'})   
    
    df_platform = df[['event2', 'web-aa', 'web-ga']]
    result_events['df_web_events'] = df_platform.loc[~((df_platform['web-aa']==0) & (df_platform['web-ga']==0))]
    df_platform = df[['event2', 'and-aa', 'and-ga']]
    result_events['df_and_events'] = df_platform.loc[~((df_platform['and-aa']==0) & (df_platform['and-ga']==0))]
    df_platform = df[['event2', 'ios-aa', 'ios-ga']]
    result_events['df_ios_events'] = df_platform.loc[~((df_platform['ios-aa']==0) & (df_platform['ios-ga']==0))]
    print('> get_data_events() -', 'data loaded')
    return df
 
# =============================================================================
#   MAIN
# =============================================================================

result = {}
result_events = {}
variables = {}
variables['rs'] = {}
variables['token'] = 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NDY2NTUzMzY0NDhfODExNDBlZGItMWU3Yy00NTkyLThlOTctNmViOWE2M2M5YWFlX3VlMSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1ZTlmZDU1ZmE5MmM0YTBhODJiM2YyYTc0YzA4OGU2MCIsInVzZXJfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiYXMiOiJpbXMtbmExIiwiYWFfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiY3RwIjowLCJmZyI6IldJQlA2Q0o2RkxFNUlQVUNFTVFGUkhRQVNBPT09PT09IiwibW9pIjoiMWE3NzUzNTkiLCJleHBpcmVzX2luIjoiODY0MDAwMDAiLCJjcmVhdGVkX2F0IjoiMTY0NjY1NTMzNjQ0OCIsInNjb3BlIjoib3BlbmlkLEFkb2JlSUQscmVhZF9vcmdhbml6YXRpb25zLGFkZGl0aW9uYWxfaW5mby5wcm9qZWN0ZWRQcm9kdWN0Q29udGV4dCJ9.VrTF-vRZ0bdoVjYf1iS__G6d0FzIMqujEMAmWGOIAck0pKfHQSpfLGzWftN0Cvp8ucAdKVcWV10fDFF6mTeAidLLerw01Zf6_LZ9P3uAZ1BJGB31A9a_t7gOALmKt_wOWnat7TWKfJikxQRqWZotWWZdBjx1AICLnKVt0m04nhBlhIgP2XcrXCPeEe9ipYjBq1UkSjJxdFksHkCOgZhu3zdLU0zRNfveiDIYccqERRPnwRDavHE46lKyCkgpyPPK31hA93-prGerWi7hYdafM450ezfsZG4TEnc1PJAcqhRX6M90LRV7eVuiQM_Ox2Lsz5vIZC6les9ALnkXyPaBzA'
variables['directory'] = '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/health-metrics-comparison'
variables['rs']['rs_fotocasaes'] = 'vrs_schibs1_fcall'
variables['rs']['rs_motosnet'] = 'vrs_schibs1_motorcochesnet'
variables['from_date'] = '2021-02-01'
variables['to_date'] = '2021-02-01'

init()

result['df_aa'] = get_adobe_analytics_data()
result['df_ga'] = get_google_analytics_data()
result['df'] = get_data(variables['rs']['rs_fotocasaes'])

result_events['df_aa_events'] = get_adobe_analytics_data_events()
df_aa_events_fc = result_events['df_aa_events'].loc[result_events['df_aa_events'].rs == variables['rs']['rs_fotocasaes']]
result_events['df_ga_events'] = get_google_analytics_data_events()
result_events['df'] = get_data_events(df_aa_events_fc)
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

# # =============================================================================
# #   REQUEST GOOGLE ANALYTICS
# # =============================================================================

# def set_adobe_analytics_export_csv():
#     dir = os.path.join(variables['directory'], 'aa')
#     if os.path.isdir(dir):
#         shutil.rmtree(dir)
#     os.makedirs(dir)
#     result['df_aa'].to_csv(dir + '/data_health_metrics_comparison.csv')
#     print('> set_adobe_analytics_export_csv() -', 'export loaded')
    



