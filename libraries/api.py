import requests
import functions as f
import pandas as pd

class API:
    def __init__(self, method, url, headers, payload):
        self.method = method
        self.url = url
        self.headers = headers
        self.payload = payload

    def request(self):
        df = pd.DataFrame()
        response = requests.request(self.method, self.url, headers=self.headers, data=self.payload)
        if response.status_code != 200:
            f.Log.print_and_exit('API.request', str(response.status_code) + ' > ' + response.text)
        else:
            response = response.json()
            total_records = response['numberOfElements']
            if total_records > 0:
                df = pd.DataFrame.from_dict(response['rows'])
        return df


class Adobe_API(API):
    rs_fotocasaes = 'vrs_schibs1_fcall'
    rs_cochesnet = 'vrs_schibs1_motorcochesnet'
    rs_motosnet = 'vrs_schibs1_motormotosnet'

    def __init__(self, method, url, token, payload):
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token,
            'x-api-key': '5e9fd55fa92c4a0a82b3f2a74c088e60',
            'x-proxy-global-company-id': 'schibs1'
        }
        super().__init__(method, url, headers, payload)


class Adobe_Report_API(Adobe_API):
    def __init__(self, token, payload):
        url = 'https://analytics.adobe.io/api/schibs1/reports'
        super().__init__('POST', url, token, payload)
