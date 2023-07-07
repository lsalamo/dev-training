import pandas as pd
import configparser
import datetime
import os
import jwt
import libraries.api as f_api
import libraries.dataframe as f_df


class Adobe_API(f_api.API):
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
            'x-gw-ims-org-id': self.config["orgid"],
            'x-sandbox-name': 'prod'
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


class Jobs(Adobe_API):
    def __init__(self, date_from, to_date):
        url = 'https://platform.adobe.io/data/core/privacy/jobs?regulation=gdpr&size=100&page={{page}}&status=&fromDate={{from_date}}&toDate={{to_date}}&filterDate='
        url = url.replace("{{from_date}}", date_from).replace("{{to_date}}", to_date)
        payload = {}
        super().__init__('GET', url, payload)

    def request(self):
        url = self.url
        self.url = url.replace("{{page}}", "1")
        response = super().request()
        if 'jobDetails' in response:
            rows = response['jobDetails']
            if len(rows) > 0:
                df_total = pd.DataFrame(rows)
                pagination = int(response['totalRecords'])
                pagination = (pagination // 100) + 1
                for i in range(2, pagination):
                    self.url = url.replace("{{page}}", str(i))
                    response = super().request()
                    rows = response['jobDetails']
                    df = pd.DataFrame(rows)
                    df_total = pd.concat([df, df_total])
        return df_total


