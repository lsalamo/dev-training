import requests
import pandas as pd
import os
import shutil

# =============================================================================
# VARIABLES
# =============================================================================
TOKEN = "eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LTEuY2VyIn0.eyJpZCI6IjE2MjA4MTYyNDE1MjBfMmE0MGFhYjUtYzJmYy00ZjZmLWFkZDQtZmRiOGY5ZjdlYTk0X3VlMSIsImNsaWVudF9pZCI6IjVlOWZkNTVmYTkyYzRhMGE4MmIzZjJhNzRjMDg4ZTYwIiwidXNlcl9pZCI6IkFENEE3QTFENjA4OEY5RjQwQTQ5NUM2OEB0ZWNoYWNjdC5hZG9iZS5jb20iLCJ0eXBlIjoiYWNjZXNzX3Rva2VuIiwiYXMiOiJpbXMtbmExIiwiYWFfaWQiOiJBRDRBN0ExRDYwODhGOUY0MEE0OTVDNjhAdGVjaGFjY3QuYWRvYmUuY29tIiwiZmciOiJWTllNSTdWWEZMUDU1SFVDQ01aTFJIUUFJST09PT09PSIsIm1vaSI6ImUxNDNkZmM2IiwiZXhwaXJlc19pbiI6Ijg2NDAwMDAwIiwiY3JlYXRlZF9hdCI6IjE2MjA4MTYyNDE1MjAiLCJzY29wZSI6Im9wZW5pZCxBZG9iZUlELHJlYWRfb3JnYW5pemF0aW9ucyxhZGRpdGlvbmFsX2luZm8ucHJvamVjdGVkUHJvZHVjdENvbnRleHQifQ.GiTPTA2tAeqlyF2EfNzUpZqh8uPaBImzElGNTcwW6ZFLHVs4i9NVXZIt6cEPEit84z3l1UqALsA-dsMJo-mh85kRNZfMdDFrvajoxLfjeuHKfX5KmtsHAEltsttDz-gcdA7n5nka7m3Kd1_NuE7MGF_xlMgZ5ZC338vPbgQ3uJ98Lqn5QBuuvV90CAUE4WwvHSuE8jZZDcK5k96dzBfuXu4sSLevGABjCJEMAFU0PrtodtCxSHu0HA9f7sVHC-hNtssIH3JDt48JYiwqhsoott2wqmg4A-ehD8SfT7A4ZlGpTbB-xr_W_SmAU_AYTjRrj3RxIcVx1yrcQCsDfu394A"
FROM_DATE = "2021-05-03"
TO_DATE = "2021-05-09"
DIR_PARENT = "/Users/luis.salamo/Documents/github enterprise/python-training/adobe"
DIR_EXPORT = 'export' 
DIR_PATH = os.path.join(DIR_PARENT, DIR_EXPORT)

# =============================================================================
#   IMPORT CSV
# =============================================================================

df = pd.read_csv(DIR_PATH + "/data_license.csv")  

# =============================================================================
#   RESULT
# =============================================================================

df_1feb2020_to_31jan2021 = df[
    ((df["year"].astype("int64") == 2020) & (df["month"].astype("str") != "Jan")) |
    ((df["year"].astype("int64") == 2021) & (df["month"].astype("str") == "Jan"))
]
df_last_year = pd.DataFrame(df_1feb2020_to_31jan2021.groupby(["report_suite", "rsid"])['total'].agg('sum'))
df_last_year.rename(columns=({'total': '1feb2020_to_31jan2021'}),inplace=True)
df_last_year["1feb2020_to_31jan2021"].sum()

df_1feb2021_to_31jan2022 = df[
    ((df["year"].astype("int64") == 2021) & (df["month"].astype("str") != "Jan")) |
    ((df["year"].astype("int64") == 2022) & (df["month"].astype("str") == "Jan"))
]
df_current_year = pd.DataFrame(df_1feb2021_to_31jan2022.groupby(["report_suite", "rsid"])['total'].agg('sum'))
df_current_year.rename(columns=({'total': '1feb2021_to_31jan2022'}),inplace=True)
df_current_year["1feb2021_to_31jan2022"].sum()

result = pd.merge(df_last_year,df_current_year,on=['report_suite', 'rsid'])
result = result.sort_values(by='1feb2020_to_31jan2021', ascending=False)
result.info(verbose=True)
result.dtypes
result.index()
n s
# =============================================================================
#   EXPORT CSV
# =============================================================================

if os.path.isdir(DIR_PATH):
    shutil.rmtree(DIR_PATH)
os.makedirs(DIR_PATH)
result.to_csv(DIR_PATH + "/data_license.csv")
  