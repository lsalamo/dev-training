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
    EVENT_EXPORT_BUCKET_NAME = 'optimizely-events-data'
    OPTIMIZELY_ACCOUNT_ID = '2001130706'
    OPTIMIZELY_PERSONAL_ACCESS_TOKEN = '2:oJVKE5YeozQX-nsgp-1AzRzTC4nMbf9ioYw3PtgrZs99rBVccGiQ'

    optimizely_s3_client = OptimizelyS3Client(OPTIMIZELY_PERSONAL_ACCESS_TOKEN)
    s3_client = optimizely_s3_client.as_boto3_s3_client()


    #luis
    # path = optimizely_s3_client.get_bucket_prefix(OPTIMIZELY_ACCOUNT_ID)
    # path = optimizely_s3_client.get_bucket_prefix(OPTIMIZELY_ACCOUNT_ID)+"type=decisions/date=2021-02-01/"
    # path = optimizely_s3_client.get_bucket_prefix(OPTIMIZELY_ACCOUNT_ID)+"type=decisions/date=2021-02-01/experiment=10135051401"
    path = optimizely_s3_client.get_bucket_prefix(OPTIMIZELY_ACCOUNT_ID)+"type=decisions/date=2021"
    print(path)
    
    all_objects = s3_client.list_objects(
        Delimiter="/experiment=10135051401",
        Bucket=EVENT_EXPORT_BUCKET_NAME,
        Prefix=path,
        MaxKeys=10000
    )
    # s3 = s3_client.bucket(EVENT_EXPORT_BUCKET_NAME)
    # all_objects2 = s3.bucket.
    # print(all_objects2)
    
    
    # print(all_objects)
    creds = optimizely_s3_client.get_creds()
    print('============')
    print('export AWS_ACCESS_KEY_ID=' + creds['accessKeyId'])
    print('export AWS_SECRET_ACCESS_KEY=' + creds['secretAccessKey'])
    print('export AWS_SESSION_TOKEN=' + creds['sessionToken'])
    print('============')
    
    # s3 = boto3.client('s3')
    # print(all_objects)
    for file in all_objects['Contents']:
        # file = 's3://optimizely-events-data/' + file['Key']
        s3_file = file['Key']
        # print('path > ' + s3_file)
        file = os.path.split(s3_file)[1]
        # print('file > ' + file)
        # s3_client.download_file(EVENT_EXPORT_BUCKET_NAME, s3_file, 'temp/'+file)
        
        # s3_client.download_dir(EVENT_EXPORT_BUCKET_NAME, file['Key'], '~luis.salamo/Downloads/temp/'+file['Key'])
        # s3_client.download_file(EVENT_EXPORT_BUCKET_NAME, s3_file, file)
        # pd.read_parquet(file, engine='pyarrow')
    
    
    return all_objects


if __name__ == '__main__':
    a = main()
    # b = pd.read_parquet('s3://optimizely-events-data/v1/account_id=2001130706/type=decisions/date=2021-02-01/experiment=8801702869/part-00000-f58956e2-7fff-4092-92db-65a06fbc7136.c000.snappy.parquet', engine='pyarrow')
    b = pd.read_parquet('temp/part-00000-a8ae7344-657f-4da3-b023-61e3786ce74b.c000.snappy.parquet', engine='pyarrow')
# df = pq.read_table('dataset.parq').to_pandas()