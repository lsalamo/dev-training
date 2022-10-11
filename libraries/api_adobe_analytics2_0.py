import pandas as pd
import configparser
import datetime
import os
import jwt
import functions as f
import dt as dt
import api
import dataframe as f_df
import logs as f_log
import api as f_api


class Adobe_API(api.API):
    rs_fotocasaes = 'vrs_schibs1_fcall'
    rs_cochesnet = 'vrs_schibs1_motorcochesnet'
    rs_motosnet = 'vrs_schibs1_motormotosnet'
    rs_milanuncioscom = 'vrs_schibs1_generalistmilanuncio'
    rs_infojobsnet = 'vrs_schibs1_jobsinfojobs'
    rs_infojobsit = 'vrs_schibs1_jobsitalyall'

    def __init__(self, method, url, payload, access_token):
        # Logging
        self.log = f_log.Logging()

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

        super().__init__('POST', url, payload, access_token)

    def request(self):
        df = pd.DataFrame()
        response = super().request()
        if 'rows' in response:
            rows = response['rows']
            if len(rows) > 0:
                df = pd.DataFrame.from_dict(rows)
                df = f_df.Dataframe.Columns.split_column_array_into_columns(df, 'data')
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
            with open(os.path.join(os.path.expanduser('~'), '.ssh/', config["private_key"]), 'r') as file:
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

            # log.print('Adobe_API.init', 'Sending "POST" request to {}'.format(config['imsexchange']))
            # log.print('Adobe_API.init', 'Post body: {}'.format(post_body))
            request = f_api.API('POST', config["imsexchange"], headers={}, payload=post_body)
            response = request.request()
            return response["access_token"]

        # Logging
        log = f_log.Logging()

        # JWT Token
        config_parser = configparser.ConfigParser()
        config_parser.read('/Users/luis.salamo/Documents/github enterprise/python-training/adobe/adobe-credentials.ini')
        config = dict(config_parser["default"])
        jwt_token = get_jwt_token()
        # log.print('Adobe_JWT.get_access_token', 'JWT Token: {}'.format(jwt_token))

        # Access Token
        access_token = get_access_token()
        # log.print('Adobe_JWT.get_access_token', 'Access Token: {}'.format(access_token))
        return access_token
