"""
https://docs.aws.amazon.com/code-library/latest/ug/python_3_s3_code_examples.html
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
"""
import boto3
import json
from datetime import date


def list_buckets():
    print("List your buckets:")
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        print(f"\t{bucket.name}")


# List objects in an Amazon S3 bucket
def list_objects():
    print("List objects in an Amazon S3 bucket:")
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('data-sch-digitalanalytics-dev')
    for obj in bucket.objects.all():
        print(f"\t{obj.key}")


def put_object():
    print("Put object in an Amazon S3 bucket:")
    s3 = boto3.client('s3')

    data = {
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964
    }
    data_to_str = json.dumps(data)
    today = date.today()
    date_string = today.strftime('%Y-%m-%d')
    s3.put_object(
        Body=data_to_str,
        Bucket='data-sch-digitalanalytics-dev',
        Key=f'adobe-datafeed/{date_string}/test.json',
    )


if __name__ == '__main__':
    list_buckets()
    list_objects()
    put_object()

