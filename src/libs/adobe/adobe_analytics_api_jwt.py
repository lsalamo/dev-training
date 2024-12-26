import pandas as pd
import configparser
import datetime
import os
import jwt
import libs.api as f_api
import libs.dataframe as f_df


class Adobe_API(f_api.API):
    rs_fotocasaes = "vrs_schibs1_fcall"
    rs_habitaclia = "schibstedspainrehabitacliaprod"
    rs_uniquetool = "schibstedspainreuniquetoolprod"
    rs_cochesnet = "vrs_schibs1_motorcochesnet"
    rs_motosnet = "vrs_schibs1_motormotosnet"
    rs_carfactory = "schibstedspainmotorcarfactorywebprod"
    rs_milanuncioscom = "vrs_schibs1_generalistmilanuncio"
    rs_infojobsnet = "vrs_schibs1_jobsinfojobs"
    rs_infojobsit = "vrs_schibs1_jobsitalyall"
    rs_infojobs_epreselec = "schibstedspainjobsepreselecprod"

    def __init__(self, method, url, payload):
        # adobe configuration
        config_parser = configparser.ConfigParser()
        config_parser.read(os.path.join(os.path.dirname(__file__), "../adobe/credentials/credentials.ini"))
        self.config = dict(config_parser["default"])

        # access token
        self.access_token = self.get_access_token()
        # self.access_token = 'eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2Nzg4MjY2ODQ1NDZfODVkYzhlMzEtYjdkNy00MzI0LTlhOTEtOThlOTNiZjY0NmZkX2V3MSIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiI1YThkY2MyY2ZhNzE0NzJjYmZhNGZiNTM2NzFjNDVlZCIsInVzZXJfaWQiOiI3NjREN0Y4RDVFQjJDRDUwMEE0OTVFMUJAMmRkMjM0Mzg1ZTYxMDdkNzBhNDk1Y2E0Iiwic3RhdGUiOiIiLCJhcyI6Imltcy1uYTEiLCJhYV9pZCI6Ijc2NEQ3RjhENUVCMkNENTAwQTQ5NUUxQkAyZGQyMzQzODVlNjEwN2Q3MGE0OTVjYTQiLCJjdHAiOjAsImZnIjoiWElZWFk0VkVGUE41TVA0S0VNUVZaSFFBUVE9PT09PT0iLCJzaWQiOiIxNjc4ODI2Njg0MjY2XzBmM2RhMDQ0LTllM2UtNGVlNS1hYWQ4LTA2YTg4MDcwMDI3NF91ZTEiLCJydGlkIjoiMTY3ODgyNjY4NDU0Nl8wZWJlOWY3Zi0xOTUxLTQwZTItYmRmNC03YTI4YzQ0ZTQ3M2RfZXcxIiwibW9pIjoiYzZhNmE1MiIsInBiYSI6Ik1lZFNlY05vRVYsTG93U2VjIiwicnRlYSI6IjE2ODAwMzYyODQ1NDYiLCJleHBpcmVzX2luIjoiODY0MDAwMDAiLCJzY29wZSI6Im9wZW5pZCxBZG9iZUlELHJlYWRfb3JnYW5pemF0aW9ucyxhZGRpdGlvbmFsX2luZm8ucHJvamVjdGVkUHJvZHVjdENvbnRleHQsYWRkaXRpb25hbF9pbmZvLmpvYl9mdW5jdGlvbiIsImNyZWF0ZWRfYXQiOiIxNjc4ODI2Njg0NTQ2In0.J5pRFhX7VriDl6OzhqD6FQmY6lB7YG6zZ4r6P3sBjYTVtlYu_-6DpMJvX-VZOabF04Vz7EsZqCkHRcwL3TdgyoJ1tfS4v8YASdCQR81qUfnTmMoGXA8Jk8YPBgdi0bUYyZct8m29dtQm0hL82NLn7Xl9DnSGV33SAaAqO6UslmO7F9wgXNChzAopBTNY9itul2OmNMMwqAMxbL6hpbn9zTxsZ7NkFbqCyemShhxd81wZJa-Idm98r_cy-QQj5TQtRRMDsuBUiJMqJffi-S1eUe9UipOOFWVF0eSdWvK6Vks3wGH7MN8VJM0_RbkK93fTnFAl4WsL53C2euzPLyAUdQ'

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.access_token,
            "x-api-key": self.config["apikey"],
        }
        super().__init__(method, url, headers, payload)

    def get_access_token(self):
        def get_jwt_token():
            # with open(os.path.join(os.path.expanduser('~'), '.ssh/', config["private_key"]), 'r') as file:
            with open(
                os.path.join(os.path.dirname(__file__), "../adobe/credentials/" + self.config["private_key"]), "r"
            ) as file:
                private_key = file.read()

            payload = {
                "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=30),
                "iss": self.config["orgid"],
                "sub": self.config["technicalaccountid"],
                "aud": "https://{}/c/{}".format(self.config["imshost"], self.config["apikey"]),
            }
            for x in self.config["metascopes"].split(","):
                payload["https://{}/s/{}".format(self.config["imshost"], x)] = True

            return jwt.encode(payload, private_key, algorithm="RS256")

        def get_access_token():
            post_body = {
                "client_id": self.config["apikey"],
                "client_secret": self.config["secret"],
                "jwt_token": jwt_token,
            }
            request = f_api.API("POST", self.config["imsexchange"], headers={}, payload=post_body)
            response = request.request()
            return response["access_token"]

        # JWT Tokens
        jwt_token = get_jwt_token()

        # Access Token
        return get_access_token()


class Adobe_Report_API(Adobe_API):
    def __init__(self, rs, url_request, date_from, to_date):
        # endpoint
        url = "https://analytics.adobe.io/api/schibs1/reports"

        # date
        date_from = datetime.datetime.strptime(date_from, "%Y-%m-%d")
        date_from = date_from.strftime("%Y-%m-%dT00:00:00.000")
        to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d")
        to_date = to_date + datetime.timedelta(days=1)
        to_date = to_date.strftime("%Y-%m-%dT00:00:00.000")
        date = date_from + "/" + to_date

        # payload
        with open(os.path.join(os.getcwd(), url_request), "r") as file:
            payload = file.read()
            payload = payload.replace("{{rs}}", rs)
            payload = payload.replace("{{dt}}", date)

        super().__init__("POST", url, payload)

    def request(self):
        df = pd.DataFrame()
        response = super().request()
        if "rows" in response:
            rows = response["rows"]
            if len(rows) > 0:
                df = pd.DataFrame.from_dict(rows)
                df_values = pd.DataFrame(df["data"].tolist(), index=df.index)
                df = pd.merge(left=df, right=df_values, left_index=True, right_index=True, how="inner")
        return df


class Adobe_Report_Suite_API(Adobe_API):
    def __init__(self):
        url = "https://analytics.adobe.io/api/schibs1/collections/suites?limit=100&page=0"
        payload = {}
        super().__init__("GET", url, payload)

    def request(self):
        df = pd.DataFrame()
        response = super().request()
        if "content" in response:
            rows = response["content"]
            if len(rows) > 0:
                df = pd.DataFrame.from_dict(rows)
        return df


class Adobe_Dimensions_API(Adobe_API):
    def __init__(self, rs):
        self.rs = rs
        url = f"https://analytics.adobe.io/api/schibs1/dimensions?rsid={rs}&locale=en_US&classifiable=false"
        payload = {}
        super().__init__("GET", url, payload)

    def request(self):
        # df = pd.DataFrame()
        response = super().request()
        df = pd.DataFrame.from_dict(response)
        if not f_df.Dataframe.is_empty(df):
            df = df.loc[df["extraTitleInfo"].notnull()]
            df = df.loc[df["extraTitleInfo"].str.contains("evar")]
            df["evar"] = df["extraTitleInfo"].str.replace("evar", "")
            df["evar"] = df["evar"].astype("int")
            df = df[["evar", "name"]]
            df_evars = pd.DataFrame(pd.Series(range(1, 201)), columns=["evar"])
            df_evars["rsid"] = self.rs
            # merge
            df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_evars, df, "evar", "left", ("", ""))
        return df


class usage_logs(Adobe_API):
    def __init__(self, config):
        url = f'https://analytics.adobe.io/api/schibs1/auditlogs/usage?startDate={config["from_date"]}&endDate={config["to_date"]}&eventType=2&limit=1000&page=PAGE_VAR'
        payload = {}
        super().__init__("GET", url, payload)

    def request(self):
        url = self.url
        self.url = url.replace("PAGE_VAR", "0")
        response = super().request()
        if "content" in response:
            rows = response["content"]
            if len(rows) > 0:
                df_total = pd.DataFrame(rows)
                pagination = int(response["totalPages"])
                for i in range(1, pagination):
                    self.url = url.replace("PAGE_VAR", str(i))
                    response = super().request()
                    rows = response["content"]
                    df = pd.DataFrame(rows)
                    df_total = pd.concat([df, df_total])
        return df_total
