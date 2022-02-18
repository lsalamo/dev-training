import requests
import pandas as pd
import os
import shutil
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
    print("> init() -", 'loaded')


# =============================================================================
# REQUEST RS
# =============================================================================

def get_request():
    file = open('request.json')
    payload = file.read()
    file.close()
    print("> get_request() -", 'file payload loaded')

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
        print("> get_request() - rs:", row)
        response = requests.request("POST", url, headers=headers, data=payload.replace('{{rs}}', row))
        if response.status_code != 200:
            sys.exit("ERROR " + str(response.status_code) + " > " + response.text)
        else:
            response = response.json()
            total_records = response['numberOfElements']
            if total_records > 0:
                df_request = pd.DataFrame.from_dict(response['rows'])
                df_request['rs'] = row
                df_request[['web-visits','web-visitors','web-visits/visitors','and-visits','and-visitors','and-visits/visitors','ios-visits','ios-visitors','ios-visits/visitors']] = pd.DataFrame(df_request['data'].tolist(), index= df_request.index)
                df = df.append(df_request)
        print('> get_request() -', 'request loaded')
        
    
    # Clean dataframe
    df.drop(['itemId', 'data'], axis=1, inplace=True)
    df.rename(columns={'value': 'day'}, inplace=True)
    print("> get_request() -", 'clean dataframe loaded')
    
    return df

# =============================================================================
#   EXPORT CSV
# =============================================================================

def set_export_csv():
    dir = os.path.join(result['variables']['directory'], 'export')
    if os.path.isdir(dir):
        shutil.rmtree(dir)
    os.makedirs(dir)
    result['df'].to_csv(dir + '/data_health_metrics_comparison.csv')


# =============================================================================
#   MAIN
# =============================================================================

result = {}
result['variables'] = {}
result['variables']['rs'] = {}
result['variables']['token'] = 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2NDUxODA2NjYzNzhfZDVmODM2MmQtYTc3MS00NTY5LWEyM2QtMWU4YWMyZmY0NjZiX3VlMSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1ZTlmZDU1ZmE5MmM0YTBhODJiM2YyYTc0YzA4OGU2MCIsInVzZXJfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiYXMiOiJpbXMtbmExIiwiYWFfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiY3RwIjowLCJmZyI6IldHUlAyQ0o2RkxFNUlQVUNFTVFGUkhRQVNBPT09PT09IiwibW9pIjoiMWUxNDJjZWIiLCJleHBpcmVzX2luIjoiODY0MDAwMDAiLCJzY29wZSI6Im9wZW5pZCxBZG9iZUlELHJlYWRfb3JnYW5pemF0aW9ucyxhZGRpdGlvbmFsX2luZm8ucHJvamVjdGVkUHJvZHVjdENvbnRleHQiLCJjcmVhdGVkX2F0IjoiMTY0NTE4MDY2NjM3OCJ9.MARYwQhizZ0gcRL1Fsorrhwp3MRbS9tWLC8eQ78YMx6C4N__y31mSt9jmasUKoHoIPPO0ulyNSaPHI4l914b12sw0Z2HoeWOi_yvC1F4KzbpKe0YwzHJoU14nCBvIoYURgc4yu0Me8KYO_i3657jsLq2ypJ4_321-yDslxIGjxmDcTdpz2GXJwv8CeujFSjK2oYYL7u87SgDWoJQh7guDcdkLJ2hWicTl5TXF-afvJBeDCsStSWlCd16ie7mslFzZ_GSmJXgHPhAynjBGSjMv0wobFPXmc0z82gJjQtgmHSDlr5RqWRVOlHsTTvsZ1jKD2a_V0LvPl4SWygcPYBfrw'
result['variables']['directory'] = '/Users/luis.salamo/Documents/github enterprise/python-training/adobe/health-metrics-comparison'
result['variables']['rs']['rs_fotocasaes'] = 'vrs_schibs1_fcall'
result['variables']['rs']['rs_motosnet'] = 'vrs_schibs1_motorcochesnet'
result['variables']['from_date'] = '2021-02-01'
result['variables']['to_date'] = '2021-02-01'

init()
result['df'] = get_request()
set_export_csv()


