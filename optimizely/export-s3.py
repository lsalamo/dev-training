# HELP LIST OBJECT
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects

#!/usr/bin/env python3

from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from datetime import datetime
import requests as r
import boto3
import pytz
import sys
import os
import pandas as pd
import pyarrow.parquet as pq
import shutil
import re

class BearerAuth(r.auth.AuthBase):
    """
    Bearer authentication class that integrates with requests'
    native authentication mechanism.
    """

    def __init__(self, token):
        """
        Args:
            token (str): Bearer token used to authenticate requests to an API
        """
        self.token = token

    def __call__(self, request):
        """Sets the Authorization header on the current HTTP request.
        Args:
            request (request): The current HTTP request object.
        Returns:
            request: The current HTTP request with additional
                     Authorization header
        """
        request.headers['Authorization'] = f"Bearer {self.token}"
        return request


class OptimizelyS3Client(object):
    """
    AWS S3 client using credentials from Optimizely
    """

    EXPORT_CREDENTIALS_URL = 'https://api.optimizely.com/v2/export/credentials'
    CREDENTIALS_IDENTIFIER = 'Optimizely'
    EVENT_EXPORT_BUCKET_REGION = 'us-east-1'

    def __init__(self, token: str):
        """
        Args:
            token (str): Optimizely personal access token
        """
        self.token = token

    def get_bucket_prefix(self, account_id: str) -> str:
        """Get the bucket prefix for a given Optimizely account ID
        Args:
            account_id (str): Optimizely account ID
        Returns:
            str: The S3 bucket prefix to access events data
        """
        return f"v1/account_id={account_id}/"

    def get_creds(self) -> dict:
        """Get AWS credentials from the Optimizely public API
        Returns:
            dict: The AWS credentials for a given Optimizely account
        """
        response = r.get(
            OptimizelyS3Client.EXPORT_CREDENTIALS_URL,
            auth=BearerAuth(self.token)
        )

        node = response.json()
        return node['credentials']

    def as_boto3_s3_client(self):
        """
        Convert the Optimizely S3 client to a standard boto s3
        client with refreshable credentials
        Returns:
            botocore.client.S3: Standard boto3 S3 client
        """
        creds = self.get_creds()

        # The API response is in milliseconds
        expiry_time = int(creds['expiration'] / 1000)

        # boto expects the expiry time to be a UTC datetime
        expiry_time = datetime.fromtimestamp(expiry_time, pytz.utc)

        opz_refreshable_credentials = RefreshableCredentials(
            creds['accessKeyId'],
            creds['secretAccessKey'],
            creds['sessionToken'],
            expiry_time,
            self.get_creds,
            OptimizelyS3Client.CREDENTIALS_IDENTIFIER
        )

        session = get_session()
        session._credentials = opz_refreshable_credentials
        session.set_config_variable('region', OptimizelyS3Client.EVENT_EXPORT_BUCKET_REGION)
        opz_session = boto3.Session(botocore_session=session)
        s3_client = opz_session.client('s3')

        return s3_client


def main():
    EXPERIMENT_ID = '20212520676'
    DATE_START = '2021-05-01'
    DATE_END = '2021-05-07'
    DIR_EXPORT = './export'
    EVENT_EXPORT_BUCKET_NAME = 'optimizely-events-data'
    OPTIMIZELY_ACCOUNT_ID = '2001130706'
    OPTIMIZELY_PERSONAL_ACCESS_TOKEN = '2:oJVKE5YeozQX-nsgp-1AzRzTC4nMbf9ioYw3PtgrZs99rBVccGiQ'

    optimizely_s3_client = OptimizelyS3Client(OPTIMIZELY_PERSONAL_ACCESS_TOKEN)
    s3_client = optimizely_s3_client.as_boto3_s3_client()

    path = optimizely_s3_client.get_bucket_prefix(OPTIMIZELY_ACCOUNT_ID)+"type=decisions/"
    # path = optimizely_s3_client.get_bucket_prefix(OPTIMIZELY_ACCOUNT_ID)+"type=decisions/date=2021-"
    # path = optimizely_s3_client.get_bucket_prefix(OPTIMIZELY_ACCOUNT_ID)+"type=decisions/date=2021-02-01/"
    # path = optimizely_s3_client.get_bucket_prefix(OPTIMIZELY_ACCOUNT_ID)+"type=decisions/date=2021-02-01/experiment=10135051401"

# =============================================================================
#     download s3 files to localstorage
# =============================================================================
    bucket_object_list = []
    
    # delete export directory
    if os.path.isdir(DIR_EXPORT):
        shutil.rmtree(DIR_EXPORT)
        
    paginator = s3_client.get_paginator('list_objects')
    operation_parameters = {
        'Bucket': EVENT_EXPORT_BUCKET_NAME,
        'Prefix': path
    }
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        if "Contents" in page:
            for key in page["Contents"]:
                s3_key = key["Key"]
                date_path = re.search('date=(.+?)/', s3_key)
                if date_path:
                    current_date = datetime.strptime(date_path.group(1), '%Y-%m-%d').date()
                    date_start = datetime.strptime(DATE_START, '%Y-%m-%d').date()
                    date_end = datetime.strptime(DATE_END, '%Y-%m-%d').date()
                    if (current_date - date_start).days >= 0 and (current_date - date_end).days <= 0:
                        if 'experiment='+EXPERIMENT_ID in s3_key:   
                            # donwload s3 file to localstorage
                            filename = os.path.basename(s3_key) 
                            foldername = os.path.dirname(s3_key)
                            s3_file = 's3://'+EVENT_EXPORT_BUCKET_NAME+'/'+s3_key
                            print('s3_file > ' + s3_file)
                            s3_key_download = DIR_EXPORT + s3_key.replace('v1/account_id='+ OPTIMIZELY_ACCOUNT_ID+'/type=decisions','')
                            os.makedirs(os.path.dirname(s3_key_download))
                            s3_client.download_file(EVENT_EXPORT_BUCKET_NAME, s3_key, s3_key_download)
                            # append experiment into list
                            bucket_object_list.append(s3_key_download)   
                    
    print('finish donwload files s3 for experiment ' + EXPERIMENT_ID)
    
# =============================================================================
#     save dataframe
# =============================================================================
    df = pd.DataFrame();
    for item in bucket_object_list:
        print(item)
        df = df.append(pd.read_parquet(item, engine='pyarrow'))
    
    
# =============================================================================
#     result
# =============================================================================
    result = { 'bucket_object_list': bucket_object_list, 'dataframe': df }
    return result


if __name__ == '__main__':
    result = main()

result1000 = result["dataframe"].head(1000)
    