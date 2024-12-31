import os
import pandas as pd

# adding libraries folder to the system path
from libs import (
    api as f_api,
    csv as f_csv,
)
from libs import api as f_api

# importing GA4 API class
from google.analytics.admin import AnalyticsAdminServiceClient


class AdminAPI(f_api.API):
    def __init__(self, config, transport: str = None):
        # configuration
        self.config = config["google"]
        self.file = self.config["__file__"]

        # authentication
        self.__authentication()

        # initialization constructor admin api
        # transport(str): The transport to use. For example, "grpc" or "rest". If set to None, a transport is chosen automatically.
        self.client = AnalyticsAdminServiceClient(transport=transport)

        # initialization constructor api
        file = self.config["__file__"]
        super().__init__(file)

    def __authentication(self):
        file_creds = self.config["credentials"]["path_service_account"]
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_creds

    def list_accounts(self) -> pd.DataFrame:
        output = []
        response = self.client.list_account_summaries()
        for row in response:
            for property in row.property_summaries:
                dict_row = {
                    "account": row.account,
                    "account_name": row.display_name,
                    "property": property.property,
                    "property_name": property.display_name,
                }
                output.append(dict_row)

        return pd.DataFrame(output)
