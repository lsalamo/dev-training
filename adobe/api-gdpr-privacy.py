import requests
import pandas as pd
import os
import shutil

# =============================================================================
# VARIABLES
# =============================================================================
TOKEN = "eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LTEuY2VyIn0.eyJpZCI6IjE2MjE5Mjk5ODg0NTBfZTUwNTdmYTctZGVjYy00MjNjLThmODktYWVhMGYzMzkwNGZhX3VlMSIsImNsaWVudF9pZCI6IjVlOWZkNTVmYTkyYzRhMGE4MmIzZjJhNzRjMDg4ZTYwIiwidXNlcl9pZCI6IkFENEE3QTFENjA4OEY5RjQwQTQ5NUM2OEB0ZWNoYWNjdC5hZG9iZS5jb20iLCJ0eXBlIjoiYWNjZXNzX3Rva2VuIiwiYXMiOiJpbXMtbmExIiwiYWFfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiZmciOiJWTzRVTjZUTkZMUDU1SFVDQ01aTFJIUUFJST09PT09PSIsIm1vaSI6ImJiZDcyOGJkIiwiZXhwaXJlc19pbiI6Ijg2NDAwMDAwIiwic2NvcGUiOiJvcGVuaWQsQWRvYmVJRCxyZWFkX29yZ2FuaXphdGlvbnMsYWRkaXRpb25hbF9pbmZvLnByb2plY3RlZFByb2R1Y3RDb250ZXh0IiwiY3JlYXRlZF9hdCI6IjE2MjE5Mjk5ODg0NTAifQ.DkI7byQjdeiUdil_0hcIdaqQBSmI2KNTmYBSPsveV1We0OHlhUEOG2bNJai4ezVscke2fG8NlqI9YjVwjwTPlmwzywBhn7AYm4k9zhSpOipKbER7RE4VX6cJSw4vLzr6hANm5CUaU_wz_WH98_rSc5_Zjynd-sPok4LlT2mI4Vgji7RdAyTluZBzRuxN3sgWrPTzTV8h2ls8mgVpjQMJrhT2ClXNbgUFEzchEIXwi7IZQzIjOG6qpt0Q1JU02RIojE2ZQmWvSzVZ9jTBSDKzrDWxL5-m5VBCREW4W8uPjS44Y39u2Duz7b9CHOTJkK4al95YyAQZI2WtEgPKTK6vfw"
FROM_DATE = "2021-05-22"
TO_DATE = "2021-05-23"
DIR_PARENT = "/Users/luis.salamo/Documents/github enterprise/python-training/adobe"
DIR_EXPORT = 'export' 
DIR_PATH = os.path.join(DIR_PARENT, DIR_EXPORT)

# =============================================================================
# REQUEST
# =============================================================================
url = "https://platform.adobe.io/data/core/privacy/jobs?regulation=gdpr&size=100&page={{page}}&status=&fromDate={{from_date}}&toDate={{to_date}}&filterDate="
url = url.replace("{{from_date}}", FROM_DATE).replace("{{to_date}}", TO_DATE)
payload = {}
headers = {
  'Accept': 'application/json',
  'Authorization': 'Bearer ' + TOKEN,
  'x-api-key': '5e9fd55fa92c4a0a82b3f2a74c088e60',
  'x-gw-ims-org-id': '05FF6243578784B37F000101@AdobeOrg',
  'x-sandbox-name': 'prod'
}
response = requests.request("GET", url.replace("{{page}}", "1"), headers=headers, data=payload)

# =============================================================================
#   REQUEST
# =============================================================================

df = pd.DataFrame();
if response.status_code != 200:
    print("ERROR " + str(response.status_code) + " > " + response.text)
else:
    response = response.json()
    total_records = response['totalRecords']
    if total_records > 0:
        df = df.append(pd.DataFrame.from_dict(response['jobDetails']))
        iterator = (total_records // 100) + 2
        for i in range(2, iterator):
            response = requests.request("GET", url.replace("{{page}}", str(i)), headers=headers, data=payload)
            response = response.json()
            df = df.append(pd.DataFrame.from_dict(response['jobDetails']))

# =============================================================================
#   CAST
# =============================================================================

df['createdDate_datetime'] = pd.to_datetime(pd.to_datetime(df['createdDate']).dt.strftime('%Y-%m-%d %H:%M:%S'))
df['lastModifiedDate_datetime'] = pd.to_datetime(pd.to_datetime(df['lastModifiedDate']).dt.strftime('%Y-%m-%d %H:%M:%S'))
df["createdDate_datetime_ymd"] = df["createdDate_datetime"].dt.strftime('%Y-%m-%d')
df["diffDate"] = df["lastModifiedDate_datetime"] - df["createdDate_datetime"]

# =============================================================================
#   CLEAR
# =============================================================================

df_clean = df[
    df["userKey"].str.startswith("Analytics-") & df["userKey"].str.contains("sdrn:infojobs")
]

# =============================================================================
#   RESULT
# =============================================================================

df_summary = df_clean.groupby(["action", "status"]).agg(
    count = pd.NamedAgg(column="jobId", aggfunc="count"),
    min = pd.NamedAgg(column="diffDate", aggfunc="min"),
    max = pd.NamedAgg(column="diffDate", aggfunc="max"),
    mean = pd.NamedAgg(column="diffDate", aggfunc=lambda x: x.mean())
)

df_summary_by_day = df_clean.groupby(["createdDate_datetime_ymd", "action", "status"]).agg(
    count = pd.NamedAgg(column="jobId", aggfunc="count"),
    min = pd.NamedAgg(column="diffDate", aggfunc="min"),
    max = pd.NamedAgg(column="diffDate", aggfunc="max"),
    mean = pd.NamedAgg(column="diffDate", aggfunc=lambda x: x.mean())
)

result = { 
    'total_records': df.shape[0], 
    'df': df,
    'df_clean': df_clean,
    'df_summary': df_summary,
    'df_summary_by_day': df_summary_by_day
}

# =============================================================================
#   EXPORT CSV
# =============================================================================

if os.path.isdir(DIR_PATH):
    shutil.rmtree(DIR_PATH)
os.makedirs(DIR_PATH)
result['df_summary_by_day'].to_csv(DIR_PATH + "/data.csv")


