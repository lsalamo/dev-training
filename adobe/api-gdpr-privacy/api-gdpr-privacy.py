import requests
import pandas as pd
import os
import shutil
import sys
from datetime import datetime

# =============================================================================
# VARIABLES
# =============================================================================
TOKEN = "eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LTEuY2VyIn0.eyJpZCI6IjE2MjI1NDM0NjMwMDRfNWQ4Y2UzOTgtNWZiYS00ZWNmLWIzZGMtZjlkMjZlZDgxNzQ3X3VlMSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1ZTlmZDU1ZmE5MmM0YTBhODJiM2YyYTc0YzA4OGU2MCIsInVzZXJfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiYXMiOiJpbXMtbmExIiwiYWFfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiZmciOiJWUFFUT1ZVTUZMUDU1SFVDQ01aTFJIUUFJST09PT09PSIsIm1vaSI6ImJkOTM4NjYyIiwiZXhwaXJlc19pbiI6Ijg2NDAwMDAwIiwic2NvcGUiOiJvcGVuaWQsQWRvYmVJRCxyZWFkX29yZ2FuaXphdGlvbnMsYWRkaXRpb25hbF9pbmZvLnByb2plY3RlZFByb2R1Y3RDb250ZXh0IiwiY3JlYXRlZF9hdCI6IjE2MjI1NDM0NjMwMDQifQ.UvvWMUtYA9DTZlF6KJHRpH2QkGExSLndVSwCszEKoyz_I9J38wgcrwv5QjDZgtod_z26rh8GgeilIG41JenQLcoYFzN9abTgHoYPZWl18GL8z-oQrl5PXxO87sQWer55aEXtJS0Bs9KSAAwhN303QEeYxxKz2BmTr2xiIRSGSu_iGL1aQOnkvGVWunh0ZA0Sj27kmYS5YsGcSBOuq-qNJp3GLzY-ShvM8w-rgtPWYO88wZ2OV4bd4g29Fr5yL53Yx9IkcLfM-rA3dW6uMqE6LC_POBMv0-twySeONdq471_KLEB8zylPQf60L0Ox89JP0u-CD0XboAr8N5Ah_8b9Kg"
FROM_DATE = "2021-05-24"
TO_DATE = "2021-05-30"
DIR_PARENT = "/Users/luis.salamo/Documents/github enterprise/python-training/adobe/api-gdpr-privacy"
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
    sys.exit("ERROR " + str(response.status_code) + " > " + response.text) 
else:
    print("page 1 loaded")
    response = response.json()
    total_records = response['totalRecords']
    if total_records > 0:
        df = df.append(pd.DataFrame.from_dict(response['jobDetails']))
        iterator = (total_records // 100) + 2
        for i in range(2, iterator):
            print("page " + str(i) + " loaded")
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

df_clean = df[df["userKey"].str.startswith("Analytics-") & df["userKey"].str.contains("sdrn:infojobs")]

# =============================================================================
#   RESULT
# =============================================================================

df_summary_by_day = df_clean.groupby(["createdDate_datetime_ymd", "action", "status"]).agg(
    count = pd.NamedAgg(column="jobId", aggfunc="count"),
    min = pd.NamedAgg(column="diffDate", aggfunc="min"),
    max = pd.NamedAgg(column="diffDate", aggfunc="max"),
    mean = pd.NamedAgg(column="diffDate", aggfunc=lambda x: x.mean())
)

data = {
    'count': df_clean.shape[0],
    'count_delete': df_clean[df_clean["action"] == "delete"].shape[0],
    'count_access': df_clean[df_clean["action"] == "access"].shape[0],
    '%_count_delete': round(data["count_delete"] / data["count"] * 100, 2),
    '%_count_access': round(data["count_access"] / data["count"] * 100, 2),
    'date_from': datetime.strptime(FROM_DATE, '%Y-%m-%d').date(),
    'date_to': datetime.strptime(TO_DATE, '%Y-%m-%d').date(),
    'date_diff': (data["date_to"] - data["date_from"]).days + 1,
    'count / day': round(df_clean.shape[0] / date_diff),
    'time_min_delete': df_clean[(df_clean["action"] == "delete") & (df_clean["status"] == "complete")]["diffDate"].min(),
    'time_min_access': df_clean[(df_clean["action"] == "access") & (df_clean["status"] == "complete")]["diffDate"].min(),
    'time_max_delete': df_clean[(df_clean["action"] == "delete") & (df_clean["status"] == "complete")]["diffDate"].max(),
    'time_max_access': df_clean[(df_clean["action"] == "access") & (df_clean["status"] == "complete")]["diffDate"].max(),
    'time_mean_delete': df_clean[(df_clean["action"] == "delete") & (df_clean["status"] == "complete")]["diffDate"].mean(),
    'time_mean_access': df_clean[(df_clean["action"] == "access") & (df_clean["status"] == "complete")]["diffDate"].mean()      
}
df_summary = pd.DataFrame.from_dict(list(data.items()))
df_summary.rename(columns={0:'Key', 1:'Value'}, inplace = True)
df_summary.set_index('Key', inplace = True)

result = { 
    'total_records': df_clean.shape[0], 
    'df': df,
    'df_clean': df_clean,
    'df_clean_complete': df_clean_complete,
    'df_summary': df_summary,
    'df_summary_by_day': df_summary_by_day
}

# =============================================================================
#   EXPORT CSV
# =============================================================================

if os.path.isdir(DIR_PATH):
    shutil.rmtree(DIR_PATH)
os.makedirs(DIR_PATH)
result['df_summary_by_day'].to_csv(DIR_PATH + "/data_summary_by_day.csv")
result['df_summary'].to_csv(DIR_PATH + "/data_summary.csv")

a = result['df_clean'][result['df_clean']['userKey'].str.contains("40558615524")]


