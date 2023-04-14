import requests
import pandas as pd
import configparser
import datetime
import sys
import os
import jwt


class API:
    def __init__(self, method, url, headers, payload):
        self.method = method
        self.url = url
        self.headers = headers
        self.payload = payload

    def request(self):
        response = requests.request(self.method, self.url, headers=self.headers, data=self.payload)
        if response.status_code != 200:
            sys.exit('> ERROR - API.request() - ' + str(response.status_code) + ' > ' + response.text)
        else:
            return response.json()


class Adobe_API(API):
    rs_fotocasaes = 'vrs_schibs1_fcall'
    rs_cochesnet = 'vrs_schibs1_motorcochesnet'
    rs_motosnet = 'vrs_schibs1_motormotosnet'
    rs_milanuncioscom = 'vrs_schibs1_generalistmilanuncio'
    rs_infojobsnet = 'vrs_schibs1_jobsinfojobs'
    rs_infojobsit = 'vrs_schibs1_jobsitalyall'

    def __init__(self, method, url, payload, access_token):
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + access_token,
            'x-api-key': '5e9fd55fa92c4a0a82b3f2a74c088e60',
        }
        super().__init__(method, url, headers, payload)


class Adobe_Report_API(Adobe_API):
    def __init__(self, rs, url_request, date_from, to_date, access_token):
        # endpoint
        url = 'https://analytics.adobe.io/api/schibs1/reports'

        # date
        date_from = datetime.datetime.strptime(date_from, '%Y-%m-%d')
        date_from = date_from.strftime('%Y-%m-%dT00:00:00.000')
        to_date = datetime.datetime.strptime(to_date, '%Y-%m-%d')
        to_date = to_date + datetime.timedelta(days=1)
        to_date = to_date.strftime('%Y-%m-%dT00:00:00.000')
        date = date_from + '/' + to_date

        # payload
        with open(os.path.join(os.getcwd(), url_request), 'r') as file:
            payload = file.read()
            payload = payload.replace('{{rs}}', rs)
            payload = payload.replace('{{dt}}', date)

        super().__init__('POST', url, payload, access_token)

    def request(self):
        df = pd.DataFrame()
        response = super().request()
        if 'rows' in response:
            rows = response['rows']
            if len(rows) > 0:
                df = pd.DataFrame.from_dict(rows)
                df_values = pd.DataFrame(df['data'].tolist(), index=df.index)
                df = pd.merge(left=df, right=df_values, left_index=True, right_index=True, how='inner')
        return df


class Adobe_Report_Suite_API(Adobe_API):
    def __init__(self, access_token):
        # endpoint
        url = 'https://analytics.adobe.io/api/schibs1/collections/suites?limit=100&page=0'
        payload = {}
        super().__init__('GET', url, payload, access_token)

    def request(self):
        df = pd.DataFrame()
        response = super().request()
        if 'content' in response:
            rows = response['content']
            if len(rows) > 0:
                df = pd.DataFrame.from_dict(rows)
        return df


class Adobe_JWT:
    @staticmethod
    def get_access_token():
        def get_jwt_token():
            # with open(os.path.join(os.path.expanduser('~'), '.ssh/', config["private_key"]), 'r') as file:
            with open(os.path.join('/Users/luis.salamo/Documents/github/python-training/adobe/', config["private_key"]), 'r') as file:
                private_key = file.read()

            payload = {
                "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=30),
                "iss": config["orgid"],
                "sub": config["technicalaccountid"],
                "aud": "https://{}/c/{}".format(config["imshost"], config["apikey"])
            }
            for x in config["metascopes"].split(","):
                payload["https://{}/s/{}".format(config["imshost"], x)] = True

            return jwt.encode(payload, private_key, algorithm='RS256')

        def get_access_token():
            post_body = {
                "client_id": config["apikey"],
                "client_secret": config["secret"],
                "jwt_token": jwt_token
            }
            request = API('POST', config["imsexchange"], headers={}, payload=post_body)
            response = request.request()
            return response["access_token"]

        # JWT Token
        config_parser = configparser.ConfigParser()
        config_parser.read('/Users/luis.salamo/Documents/github/python-training/adobe/credentials/credentials.ini')
        config = dict(config_parser["default"])
        jwt_token = get_jwt_token()

        # Access Token
        access_token = get_access_token()
        return access_token
