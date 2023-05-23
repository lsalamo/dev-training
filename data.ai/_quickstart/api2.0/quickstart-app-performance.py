"""
API:
    https://helpcenter.data.ai/community/s/article/API-2-0-User-Guide
    https://helpcenter.data.ai/community/s/article/API-Definitions-App-Performance-API-2-0
EXAMPLE:
    https://helpcenter.data.ai/community/s/article/API-Request-and-Response-Programming-Python-Script-API-2-0
USAGE:
    python3 quickstart-app-performance.py -i 2022-08-01 -e 2023-03-01
"""

import time
import urllib
import requests
import os
import yaml

config_file = os.path.join(os.path.dirname(__file__), '../../credentials/config.yaml')
with open(config_file, 'r') as config_file:
    config = yaml.safe_load(config_file)
API_KEY = config['token']

HEADERS = {
    'Content-Type': 'application/json',
    'Accept-Encoding': 'gzip',
    'Authorization': 'Bearer {}'.format(API_KEY),
}

PARAMETERS = {
    'product_id': '20600000604591,382581206',
    # 'company_id': '1000200000061645',
    'granularity': 'monthly',  # daily, monthly, yearly
    'start_date': '2023-01-01',
    'end_date': '2023-03-01',
    'countries': 'ES',  # US,JP,CN'
    'devices': 'android-all,ios-all',
    'bundles': 'all_supported'  # all_supported,download_revenue,active_users,engagement,install_metrics,demographics,retention,cross_app_usage
}

url = 'https://api.data.ai/v2.0/portfolio/app-performance?{}'.format(urllib.parse.urlencode(PARAMETERS))
print('API:{}'.format(url))
output_file_path = 'report-app-performance.json'
MAX_RETRY = 60

# If the fetch report ID times out after 10 minutes, please contact support with the Report ID from the call.

# STEP-1: SCHEDULE A REPORT
print('STEP-1: REQUEST APP PERFORMANCE REPORT...\n')
response = requests.request('GET', url, headers=HEADERS)
if response.status_code != 200:
    print(response.content)
    raise Exception('FAILED TO REQUEST REPORT')

report_id = response.json()['report_id']
print('REQUEST SUBMITTED, REPORT_ID: <{}> \n\n'.format(report_id))

# STEP-2: DOWNLOAD REPORT
url = 'https://api.data.ai/v2.0/portfolio/fetch-data?report_id={}'.format(report_id)
print('API: {}\n'.format(url))
data = ''

for i in range(MAX_RETRY):
    if bool(data):
        break

    response = requests.request('GET', url, headers=HEADERS)
    if response.status_code != 200:
        print(response.content)
        raise Exception('FAILED TO REQUEST REPORT DATA')

    report_status = response.headers.get('report_status')

    if report_status == 'progressing':
        print('NOT YET READY, RETRYING... [{}]'.format(i + 1))
        time.sleep(10)

    elif report_status == 'done':
        print('REPORT IS READY, DOWNLOADING...\n')
        data = response.content
    else:
        print(response.content)
        raise RuntimeError('FAILED TO REQUEST REPORT DATA... [{}]'.format(i + 1))

# SAVE RESULT
with open(output_file_path, 'wb') as f:
    f.write(data)
    print('DOWNLOADED FILE TO: {}'.format(output_file_path))

print('> END EXECUTION')


