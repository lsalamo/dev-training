#!/usr/bin/env python

# Copyright 2020 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Google Analytics Admin API sample quickstart application.
Example usage:
    python quickstart.py

This application demonstrates the usage of the Analytics Admin API using
service account credentials. For more information on service accounts, see

https://cloud.google.com/iam/docs/understanding-service-accounts

The following document provides instructions on setting service account
credentials for your application:

  https://cloud.google.com/docs/authentication/production

In a nutshell, you need to:
1. Create a service account and download the key JSON file.

https://cloud.google.com/docs/authentication/production#creating_a_service_account

2. Provide service account credentials using one of the following options:
- set the GOOGLE_APPLICATION_CREDENTIALS environment variable, the API
client will use the value of this variable to find the service account key
JSON file.

https://cloud.google.com/docs/authentication/production#setting_the_environment_variable

OR
- manually pass the path to the service account key JSON file to the API client
by specifying the keyFilename parameter in the constructor:
https://cloud.google.com/docs/authentication/production#passing_the_path_to_the_service_account_key_in_code

To install the latest published package dependency, execute the following:
  pip install google-analytics-admin
"""

import os
from google.analytics.admin import AnalyticsAdminServiceClient


# [START ga_admin_list_accounts]
def list_accounts(transport: str = None):
    """
    Lists the available Google Analytics accounts.

    Args:
        transport(str): The transport to use. For example, "grpc"
            or "rest". If set to None, a transport is chosen automatically.
    """

    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = AnalyticsAdminServiceClient(transport=transport)

    results = client.list_accounts()

    # Displays the configuration information for all Google Analytics accounts
    # available to the authenticated user.
    print("Result:")
    for account in results:
        print(account)


# [END ga_admin_list_accounts]


if __name__ == "__main__":
    # configuration
    from libs import json as f_json

    file_config = os.path.join(os.getcwd(), "src/google/config.json")
    config = f_json.JSON.load_json(file_config)

    # authentication
    file_creds = config["google"]["credentials"]["path_service_account"]
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_creds

    list_accounts()
