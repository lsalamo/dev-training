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
    data_values = result['variables']['rs'].values()
    result['variables']['list_rs'] = list(data_values)
    print('> init() -', 'loaded')


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================

def get_adobe_analytics_data():
    print('')
    file = open('request.json')
    payload = file.read()
    file.close()
    print('> get_adobe_analytics_data() -', 'file payload loaded')

    url = 'https://analytics.adobe.io/api/schibs1/reports'
    headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + result['variables']['token'],
      'x-api-key': '5e9fd55fa92c4a0a82b3f2a74c088e60',
      'x-proxy-global-company-id': 'schibs1'
    } 
    df = pd.DataFrame();
    for row in result['variables']['list_rs']:
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
                df = df.append(df_request)
        print('> get_adobe_analytics_data() -', 'data loaded')
        
    
    # Clean dataframe
    df.drop(['itemId', 'data', 'value'], axis=1, inplace=True)
    df['day'] = df['day'].astype('str')
    print('> get_adobe_analytics_data() -', 'clean dataframe loaded')
    
    return df

def get_adobe_analytics_data_events():
    file = open('request-events.json')
    payload = file.read()
    file.close()
    print('> get_adobe_analytics_data() -', 'file payload loaded')

    url = 'https://analytics.adobe.io/api/schibs1/reports'
    headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + result['variables']['token'],
      'x-api-key': '5e9fd55fa92c4a0a82b3f2a74c088e60',
      'x-proxy-global-company-id': 'schibs1'
    } 
    df = pd.DataFrame();
    for row in result['variables']['list_rs']:
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
                df_request['day'] = pd.to_datetime(df_request['value']).dt.strftime('%Y%m%d')
                df_request[['web', 'and', 'ios']] = pd.DataFrame(df_request['data'].tolist(), index= df_request.index)
                df = df.append(df_request)
        print('> get_adobe_analytics_data_events() -', 'data loaded')
        
    
    # Clean dataframe
    # df.drop(['itemId', 'data', 'value'], axis=1, inplace=True)
    print('> get_adobe_analytics_data_events() -', 'clean dataframe loaded')
    
    return df

# =============================================================================
#   REQUEST GOOGLE ANALYTICS
# =============================================================================

def get_google_analytics_data():
    print('')
    df = pd.DataFrame();
    dir = os.path.join(result['variables']['directory'], 'ga')
    if os.path.isdir(dir):
        df = pd.read_csv(dir + '/data_health_metrics_comparison.csv', index_col=False)
    print('> get_google_analytics_data() -', 'data loaded')
    
    # Clean dataframe
    df.drop(df.index[:1], inplace=True)
    df.drop(['Totals', 'Totals.1'], axis=1, inplace=True)
    df.columns = ['day', 'and-visits', 'and-visitors', 'ios-visits', 'ios-visitors', 'web-visits', 'web-visitors']
    df['day'] = df['day'].astype('int64').astype('str')

    print('> get_google_analytics_data() -', 'clean dataframe loaded')
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
    print(result['df_web'].head())
    return df
 
# =============================================================================
#   MAIN
# =============================================================================

result = {}
result['variables'] = {}
result['variables']['rs'] = {}
result['variables']['token'] = 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NDU0MjQ3OTMwMDdfNTlhMTM0ZTMtY2M3My00MmE0LWFhNWQtZjU5NTRlOWVhZjJmX3VlMSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1ZTlmZDU1ZmE5MmM0YTBhODJiM2YyYTc0YzA4OGU2MCIsInVzZXJfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiYXMiOiJpbXMtbmExIiwiYWFfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiY3RwIjowLCJmZyI6IldHWk9FQ0o2RkxFNUlQVUNFTVFGUkhRQVNBPT09PT09IiwibW9pIjoiMzQyY2MwIiwiZXhwaXJlc19pbiI6Ijg2NDAwMDAwIiwic2NvcGUiOiJvcGVuaWQsQWRvYmVJRCxyZWFkX29yZ2FuaXphdGlvbnMsYWRkaXRpb25hbF9pbmZvLnByb2plY3RlZFByb2R1Y3RDb250ZXh0IiwiY3JlYXRlZF9hdCI6IjE2NDU0MjQ3OTMwMDcifQ.jOnpQGqhFEMVaX1rsPOzfMHXopECDVG9WiAUCkD9ub7fuckV-NYXSg7SQQDDMpxyscV5XMZUQTyW63PQ9CQZc6R1N7SUPP40mz270BVvoWTE-dH4VSuxyayN6BM1DGwxV8TdCZXAyVxpY5j_dyYCdC4lL-QA5caShVVDNTB3nKyuCPI3NpdSL65dcnrY1y1xdiPteYTha2lqT-zouwDljWlC2qM7cmKCUv2yFhTba92_mgpFoZCDgUt51obiI5vaRaduwPb9X_9hkRuN9XllCN8VPPGkDcsCjP_VcoyPfrsbTjUqALSZLU9NLc0AKyGAZQ18CecBtuwO1iGEJIEeGg'
result['variables']['directory'] = '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/health-metrics-comparison'
result['variables']['rs']['rs_fotocasaes'] = 'vrs_schibs1_fcall'
result['variables']['rs']['rs_motosnet'] = 'vrs_schibs1_motorcochesnet'
result['variables']['from_date'] = '2021-02-01'
result['variables']['to_date'] = '2021-02-01'

init()
result['df_aa'] = get_adobe_analytics_data()
result['df_ga'] = get_google_analytics_data()
result['df'] = get_data(result['variables']['rs']['rs_fotocasaes'])




# result['df_aa_events'] = get_adobe_analytics_data_events()



# # =============================================================================
# #   REQUEST GOOGLE ANALYTICS
# # =============================================================================

# def set_adobe_analytics_export_csv():
#     dir = os.path.join(result['variables']['directory'], 'aa')
#     if os.path.isdir(dir):
#         shutil.rmtree(dir)
#     os.makedirs(dir)
#     result['df_aa'].to_csv(dir + '/data_health_metrics_comparison.csv')
#     print('> set_adobe_analytics_export_csv() -', 'export loaded')
    



