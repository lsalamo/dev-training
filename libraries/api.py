import requests
import pandas as pd
import functions as f
import dt as dt
import dataframe as f_df


class API:
    def __init__(self, method, url, headers, payload):
        self.method = method
        self.url = url
        self.headers = headers
        self.payload = payload

    def request(self):
        response = requests.request(self.method, self.url, headers=self.headers, data=self.payload)
        if response.status_code != 200:
            f.Log.print_and_exit('API.request', str(response.status_code) + ' > ' + response.text)
        else:
            return response.json()


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

    def __init__(self, rs, token, url_request, date_from, to_date):
        # endpoint
        url = 'https://analytics.adobe.io/api/schibs1/reports'

        # date
        date_from = dt.Datetime.str_to_datetime(date_from, '%Y-%m-%d')
        date_from = dt.Datetime.datetime_to_str(date_from, '%Y-%m-%dT00:00:00.000')
        to_date = dt.Datetime.str_to_datetime(to_date, '%Y-%m-%d')
        to_date = dt.Datetime.datetime_add_days(to_date, 1)
        to_date = dt.Datetime.datetime_to_str(to_date, '%Y-%m-%dT00:00:00.000')
        date = date_from + '/' + to_date

        # payload
        file = f.File(url_request)
        payload = file.read_file()
        payload = payload.replace('{{rs}}', rs)
        payload = payload.replace('{{dt}}', date)

        super().__init__('POST', url, token, payload)

    def request(self):
        df = pd.DataFrame()
        response = super().request()
        if 'rows' in response:
            rows = response['rows']
            if len(rows) > 0:
                df = pd.DataFrame.from_dict(rows)
                df = f_df.Dataframe.Columns.split_column_array_into_columns(df, 'data')
        return df


class Google_API(API):
    property_fotocasaes = '296810976'
    property_motosnet = '273930537'
    property_cochesnet = '313836548'
    platform_web = 'web'
    platform_android = 'android'
    platform_ios = 'ios'

    def __init__(self, method, url, token, payload):
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token
        }
        super().__init__(method, url, headers, payload)


class Google_Report_API(Google_API):

    def __init__(self, property, token, url_request, date_from, to_date, platform):
        # endpoint
        url = 'https://analyticsdata.googleapis.com/v1beta/properties/' + property + ':runReport'

        # payload
        file = f.File(url_request)
        payload = file.read_file()
        payload = payload.replace('{{dt_from}}', date_from)
        payload = payload.replace('{{dt_to}}', to_date)
        payload = payload.replace('{{platform}}', platform)

        super().__init__('POST', url, token, payload)

    def request(self):
        df = pd.DataFrame()
        response = super().request()
        if 'rows' in response:
            rows = response['rows']
            if len(rows) > 0:
                for row in rows:
                    # dimension
                    dimension_values = ''
                    for dimension in row['dimensionValues']:
                        dimension_values += dimension.get('value') + ','
                    row['dimensionValues'] = dimension_values.rstrip(',')

                    # metrics
                    metrics_values = ''
                    for metric in row['metricValues']:
                        metrics_values += metric.get('value') + ','
                    metrics_values
                    row['metricValues'] = metrics_values.rstrip(',')
                df = pd.DataFrame.from_dict(rows)
        return df
