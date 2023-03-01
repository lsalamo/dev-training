import pandas as pd
import configparser
import datetime
import os
import jwt
import libraries.api as f_api
import libraries.dataframe as f_df


class Adobe_API(f_api.API):
    rs_fotocasaes = 'vrs_schibs1_fcall'
    rs_habitaclia = 'schibstedspainrehabitacliaprod'
    rs_uniquetool = 'schibstedspainreuniquetoolprod'
    rs_cochesnet = 'vrs_schibs1_motorcochesnet'
    rs_motosnet = 'vrs_schibs1_motormotosnet'
    rs_carfactory = 'schibstedspainmotorcarfactorywebprod'
    rs_milanuncioscom = 'vrs_schibs1_generalistmilanuncio'
    rs_infojobsnet = 'vrs_schibs1_jobsinfojobs'
    rs_infojobsit = 'vrs_schibs1_jobsitalyall'
    rs_infojobs_epreselec = 'schibstedspainjobsepreselecprod'

    def __init__(self, method, url, payload):
        # adobe configuration
        config_parser = configparser.ConfigParser()
        config_parser.read(os.path.join(os.path.dirname(__file__), '../adobe/credentials/credentials.ini'))
        self.config = dict(config_parser["default"])

        # access token
        self.access_token = self.get_access_token()

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.access_token,
            'x-api-key': self.config["apikey"],
        }
        super().__init__(method, url, headers, payload)

    def get_access_token(self):
        def get_jwt_token():
            # with open(os.path.join(os.path.expanduser('~'), '.ssh/', config["private_key"]), 'r') as file:
            with open(os.path.join(os.path.dirname(__file__), '../adobe/credentials/' + self.config["private_key"]),
                      'r') as file:
                private_key = file.read()

            payload = {
                "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=30),
                "iss": self.config["orgid"],
                "sub": self.config["technicalaccountid"],
                "aud": "https://{}/c/{}".format(self.config["imshost"], self.config["apikey"])
            }
            for x in self.config["metascopes"].split(","):
                payload["https://{}/s/{}".format(self.config["imshost"], x)] = True

            return jwt.encode(payload, private_key, algorithm='RS256')

        def get_access_token():
            post_body = {
                "client_id": self.config["apikey"],
                "client_secret": self.config["secret"],
                "jwt_token": jwt_token
            }
            request = f_api.API('POST', self.config["imsexchange"], headers={}, payload=post_body)
            response = request.request()
            return response["access_token"]

        # JWT Tokens
        jwt_token = get_jwt_token()

        # Access Token
        return get_access_token()


class Adobe_Report_API(Adobe_API):
    def __init__(self, rs, url_request, date_from, to_date):
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

        super().__init__('POST', url, payload)

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
    def __init__(self):
        url = 'https://analytics.adobe.io/api/schibs1/collections/suites?limit=100&page=0'
        payload = {}
        super().__init__('GET', url, payload)

    def request(self):
        df = pd.DataFrame()
        response = super().request()
        if 'content' in response:
            rows = response['content']
            if len(rows) > 0:
                df = pd.DataFrame.from_dict(rows)
        return df


class Adobe_Dimensions_API(Adobe_API):
    def __init__(self, rs):
        self.rs = rs
        url = f'https://analytics.adobe.io/api/schibs1/dimensions?rsid={rs}&locale=en_US&classifiable=false'
        payload = {}
        super().__init__('GET', url, payload)

    def request(self):
        # df = pd.DataFrame()
        response = super().request()
        df = pd.DataFrame.from_dict(response)
        if not f_df.Dataframe.is_empty(df):
            df = df.loc[df['extraTitleInfo'].notnull()]
            df = df.loc[df['extraTitleInfo'].str.contains("evar")]
            df['evar'] = df['extraTitleInfo'].str.replace('evar', '')
            df['evar'] = df['evar'].astype('int')
            df = df[['evar', 'name']]
            df_evars = pd.DataFrame(pd.Series(range(1, 201)), columns=['evar'])
            df_evars['rsid'] = self.rs
            # merge
            df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_evars, df, 'evar', 'left', ('', ''))
        return df

