import os
import pandas as pd

# import datetime

from libs import api as f_api, datetime_formatter as f_dt, files as f_files


class AdobeAPI(f_api.API):
    SITES = {
        "mnet": {"str": "mnet", "id": "vrs_schibs1_motormotosnet"},
        "cnet": {"str": "cnet", "id": "vrs_schibs1_motorcochesnet"},
        "car_factory": {"str": "car_factory", "id": "schibstedspainmotorcarfactorywebprod"},
        "ma": {"str": "ma", "id": "vrs_schibs1_generalistmilanuncio"},
        "ijes": {"str": "ijes", "id": "vrs_schibs1_jobsinfojobs"},
        "ijit": {"str": "ijit", "id": "vrs_schibs1_jobsitalyall"},
        "ij_epreselec": {"str": "ij_epreselec", "id": "schibstedspainjobsepreselecprod"},
        "fc": {"str": "fc", "id": "vrs_schibs1_fcall"},
        "hab": {"str": "fc", "id": "schibstedspainrehabitacliaprod"},
    }

    def __init__(self, config):
        # configuration
        self.config = config["adobe"]
        self.site = self.config["site"]
        self.site_id = self.SITES[self.site]["id"]
        self.platform = self.config["platform"]

        # date
        self.date = self._process_dates()

        # authentication
        self.access_token = self.__authentication()

        # initialization constructor api
        file = self.config["__file__"]
        super().__init__(file)

    def _process_dates(self):
        # from_date
        from_date = self.config["from_date"]
        if from_date == "7daysAgo":
            from_date = f_dt.DatetimeFormatter.datetime_add_days(days=-7)
        else:
            from_date = f_dt.DatetimeFormatter.str_to_datetime(from_date, "%Y-%m-%d")
        from_date = f_dt.DatetimeFormatter.datetime_to_str(from_date, "%Y-%m-%dT00:00:00.000")

        # to_date
        to_date = self.config["to_date"]
        if to_date == "today":
            to_date = f_dt.DatetimeFormatter.get_current_datetime()
        else:
            to_date = f_dt.DatetimeFormatter.str_to_datetime(to_date, "%Y-%m-%d")
        to_date = f_dt.DatetimeFormatter.datetime_add_days(days=1)
        to_date = f_dt.DatetimeFormatter.datetime_to_str(to_date, "%Y-%m-%dT00:00:00.000")
        return f"{from_date}/{to_date}"

    def __authentication(self):
        url = "https://ims-na1.adobelogin.com/ims/token/v3"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        post_body = {
            "grant_type": "client_credentials",
            "client_id": self.config["credentials"]["apiKey"],
            "client_secret": self.config["credentials"]["secret"],
            "scope": "openid,AdobeID,read_organizations,additional_info.projectedProductContext",
        }
        response = self.request("POST", url, headers, payload=post_body)
        return response["access_token"]

    def reports(self, file_request):
        url = "https://analytics.adobe.io/api/schibs1/reports"

        # payload
        url_request = f_files.Directory.get_directory(os.path.realpath(self.file))
        url_request = os.path.join(url_request, file_request)
        with open(url_request, "r") as file:
            payload = file.read()
            payload = payload.replace("{{rs}}", self.site_id)
            payload = payload.replace("{{dt}}", self.date)

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.access_token,
            "x-api-key": self.config["credentials"]["apiKey"],
        }

        df = pd.DataFrame()
        response = super().request("POST", url, headers, payload)
        if "rows" in response:
            rows = response["rows"]
            if len(rows) > 0:
                df = pd.DataFrame.from_dict(rows)
                df_values = pd.DataFrame(df["data"].tolist(), index=df.index)
                df = pd.merge(left=df, right=df_values, left_index=True, right_index=True, how="inner")
        return df


# class Adobe_Report_Suite_API(Adobe_API):
#     def __init__(self):
#         url = "https://analytics.adobe.io/api/schibs1/collections/suites?limit=100&page=0"
#         payload = {}
#         super().__init__("GET", url, payload)

#     def request(self):
#         df = pd.DataFrame()
#         response = super().request()
#         if "content" in response:
#             rows = response["content"]
#             if len(rows) > 0:
#                 df = pd.DataFrame.from_dict(rows)
#         return df


# class Adobe_Dimensions_API(Adobe_API):
#     def __init__(self, rs):
#         self.rs = rs
#         url = f"https://analytics.adobe.io/api/schibs1/dimensions?rsid={rs}&locale=en_US&classifiable=false"
#         payload = {}
#         super().__init__("GET", url, payload)

#     def request(self):
#         # df = pd.DataFrame()
#         response = super().request()
#         df = pd.DataFrame.from_dict(response)
#         if not f_df.Dataframe.is_empty(df):
#             df = df.loc[df["extraTitleInfo"].notnull()]
#             df = df.loc[df["extraTitleInfo"].str.contains("evar")]
#             df["evar"] = df["extraTitleInfo"].str.replace("evar", "")
#             df["evar"] = df["evar"].astype("int")
#             df = df[["evar", "name"]]
#             df_evars = pd.DataFrame(pd.Series(range(1, 201)), columns=["evar"])
#             df_evars["rsid"] = self.rs
#             # merge
#             df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_evars, df, "evar", "left", ("", ""))
#         return df


# class usage_logs(Adobe_API):
#     def __init__(self, config):
#         url = f'https://analytics.adobe.io/api/schibs1/auditlogs/usage?startDate={config["from_date"]}&endDate={config["to_date"]}&eventType=2&limit=1000&page=PAGE_VAR'
#         payload = {}
#         super().__init__("GET", url, payload)

#     def request(self):
#         url = self.url
#         self.url = url.replace("PAGE_VAR", "0")
#         response = super().request()
#         if "content" in response:
#             rows = response["content"]
#             if len(rows) > 0:
#                 df_total = pd.DataFrame(rows)
#                 pagination = int(response["totalPages"])
#                 for i in range(1, pagination):
#                     self.url = url.replace("PAGE_VAR", str(i))
#                     response = super().request()
#                     rows = response["content"]
#                     df = pd.DataFrame(rows)
#                     df_total = pd.concat([df, df_total])
#         return df_total
