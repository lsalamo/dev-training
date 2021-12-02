import requests
import pandas as pd
import os
import shutil
import sys
import datetime

# =============================================================================
# VARIABLES
# =============================================================================

TOKEN = "eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LTEuY2VyIn0.eyJpZCI6IjE2Mjk3MDU4ODQyMzNfYjcyYTY1YTQtYWQwMS00NGQ1LWE1OTUtNTU4MjFiOTM2NWNlX3VlMSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1ZTlmZDU1ZmE5MmM0YTBhODJiM2YyYTc0YzA4OGU2MCIsInVzZXJfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiYXMiOiJpbXMtbmExIiwiYWFfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiZmciOiJWV1pZSVlGS0ZMRzVQSDRHR01aUkJQUUFBQT09PT09PSIsIm1vaSI6ImJhMmIxOGQzIiwiZXhwaXJlc19pbiI6Ijg2NDAwMDAwIiwic2NvcGUiOiJvcGVuaWQsQWRvYmVJRCxyZWFkX29yZ2FuaXphdGlvbnMsYWRkaXRpb25hbF9pbmZvLnByb2plY3RlZFByb2R1Y3RDb250ZXh0IiwiY3JlYXRlZF9hdCI6IjE2Mjk3MDU4ODQyMzMifQ.oUtsWaYnM_Bg__hca2rxWJ47e2JfNxBxnVAv7I4VLSVwWtjHkaD0pW2nDhXio9it5xOcbl4VUgmwzcTj8nHPNClqojqTTShWet31d6TjUz4Udgl6GA3T60OCgmZr95UOHMmac0YOYbX3oD3Jpo2Qew20BBoSY53DyNmCSng9D3xM6N7jTavjUS1jHFB4oX0W4yQQAcek4QilDZ4JXyL1xqMddfGSYAC_5cjJKOHldp7f4COXy7QHM6K3r-kTMCdBiHPQrgCw_C7J1jCyZaCN-RlqYHHuGDUo2YThW7zkzP57SMKUcUTWeoWSxg-EcuFetgun5Nsw1JrpO8y5x2PlBA"
FROM_DATE = "2021-02-01"
TO_DATE = "2022-01-31"

# Directory
os.chdir('/Users/luis.salamo/Documents/github enterprise/python-training/adobe/api-analytics2.0')
DIR_PARENT = os.getcwd()
DIR_EXPORT = 'export' 

result = {}

# =============================================================================
# REQUEST RS
# =============================================================================

payload = {}
url = 'https://analytics.adobe.io/api/schibs1/collections/suites?limit=100&page=0'
headers = {
  'Accept': 'application/json',
  'Authorization': 'Bearer ' + TOKEN,
  'x-api-key': '5e9fd55fa92c4a0a82b3f2a74c088e60',
  'x-proxy-global-company-id': 'schibs1'
}
response = requests.request("GET", url, headers=headers, data=payload)
df = pd.DataFrame();
if response.status_code != 200:
    sys.exit("ERROR " + str(response.status_code) + " > " + response.text) 
else:
    response = response.json()
    total_records = response['numberOfElements']
    if total_records > 0:
        df = df.append(pd.DataFrame.from_dict(response['content']))
        df = df.loc[df['collectionItemType'] == 'reportsuite']
result["df_rs"] = df          


# =============================================================================
# REQUEST LICENSE
# =============================================================================

def get_payload():
    file = open('payload-license-by-week.json')
    line = file.read()
    file.close()
    return line    
payload = get_payload()
url = 'https://analytics.adobe.io/api/schibs1/reports'
headers = {
  'Accept': 'application/json',
  'Content-Type': 'application/json',
  'Authorization': 'Bearer ' + TOKEN,
  'x-api-key': '5e9fd55fa92c4a0a82b3f2a74c088e60',
  'x-proxy-global-company-id': 'schibs1'
}

df = pd.DataFrame();
for index, row in result['df_rs'].iterrows():
    print(index, row['rsid'])
    response = requests.request("POST", url, headers=headers, data=payload.replace('{{rs}}', row['rsid']))
    if response.status_code != 200:
        sys.exit("ERROR " + str(response.status_code) + " > " + response.text) 
    else:
        response = response.json()
        total_records = response['numberOfElements']
        if total_records > 0:
            df_request = pd.DataFrame.from_dict(response['rows'])
            df_request['rs'] = row['rsid']
            df_request[['pageviews','pageevents','total']] = pd.DataFrame(df_request['data'].tolist(), index= df_request.index)
            df_request['week'] = pd.to_datetime(df_request["value"]).dt.strftime('%W')
            df_request['year'] = pd.to_datetime(df_request["value"]).dt.year
            df = df.append(df_request)
result["df_report_license_week"] = df   
        

df = result['df_report_license_week'].groupby(['year','week']).agg(
    sum_pageviews = pd.NamedAgg(column='pageviews', aggfunc='sum'),
    sum_pageevents = pd.NamedAgg(column='pageevents', aggfunc='sum'),
    sum_total = pd.NamedAgg(column='total', aggfunc='sum')
)
result["df_report_license_week_grouping"] = df   




# # =============================================================================
# #   IMPORT CSV
# # =============================================================================

# df = pd.read_csv(DIR_PATH + "/data_license.csv")  

# # =============================================================================
# #   RESULT
# # =============================================================================

# df_1feb2020_to_31jan2021 = df[
#     ((df["year"].astype("int64") == 2020) & (df["month"].astype("str") != "Jan")) |
#     ((df["year"].astype("int64") == 2021) & (df["month"].astype("str") == "Jan"))
# ]
# df_last_year = pd.DataFrame(df_1feb2020_to_31jan2021.groupby(["report_suite", "rsid"])['total'].agg('sum'))
# df_last_year.rename(columns=({'total': '1feb2020_to_31jan2021'}),inplace=True)
# df_last_year["1feb2020_to_31jan2021"].sum()

# df_1feb2021_to_31jan2022 = df[
#     ((df["year"].astype("int64") == 2021) & (df["month"].astype("str") != "Jan")) |
#     ((df["year"].astype("int64") == 2022) & (df["month"].astype("str") == "Jan"))
# ]
# df_current_year = pd.DataFrame(df_1feb2021_to_31jan2022.groupby(["report_suite", "rsid"])['total'].agg('sum'))
# df_current_year.rename(columns=({'total': '1feb2021_to_31jan2022'}),inplace=True)
# df_current_year["1feb2021_to_31jan2022"].sum()

# result = pd.merge(df_last_year,df_current_year,on=['report_suite', 'rsid'])
# result = result.sort_values(by='1feb2020_to_31jan2021', ascending=False)
# result.info(verbose=True)
# result.dtypes
# result.index()

# =============================================================================
#   EXPORT CSV
# =============================================================================

dir = os.path.join(DIR_PARENT, DIR_EXPORT)
if os.path.isdir(dir):
    shutil.rmtree(dir)
os.makedirs(dir)
result['df_report_license_week_grouping'].to_csv(dir + "/data_license.csv")
  