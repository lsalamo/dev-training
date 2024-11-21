import requests
import json
import sys
import boto3
from datetime import date
import os


def lambda_handler(event, context):
    client = boto3.client('s3')

    # get tracking plans
    url = "https://api.segmentapis.com/tracking-plans"
    headers = {"Authorization": "Bearer " + os.environ['API_TOKEN']}
    payload = {"pagination[count]": "200"}

    print("We retrieve all tracking plans!")

    try:
        response = requests.get(url, headers=headers, params=payload)
        response.raise_for_status()
    except requests.exceptions.HTTPError as error:
        print('Trackings plans could not be downloaded')
        print(error)
        sys.exit(1)

    # Tracking plans
    tracking_plans = response.json()["data"]["trackingPlans"]

    for tracking_plan in tracking_plans:

        # Traking plan Id
        trackingPlanID = tracking_plan["id"]

        url = "https://api.segmentapis.com/tracking-plans/{}/rules".format(trackingPlanID)
        headers = {"Authorization": "Bearer " + os.environ['API_TOKEN']}
        payload = {"pagination[count]": "200"}

        print("We retrieve all events from the tracking plan {}".format(trackingPlanID))

        try:
            response = requests.get(url, headers=headers, params=payload)
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            print('Tracking plan could not be downloaded')
            print(error)
            sys.exit(1)

        response_API = response.json()

        data = response_API['data']

        # Get the rules
        rules = data.get('rules')

        # Get next pagination to iterate
        pagination = data.get('pagination')
        nextPagination = pagination.get('next')

        # Iterate with the pagination
        while nextPagination:
            # We have to pass the next page as a parameter
            payload = {"pagination[count]": "200", "pagination[cursor]": nextPagination}
            response_API = requests.get(url, headers=headers, params=payload).json()
            nextData = response_API['data']

            # Add the new rules
            rules.extend(nextData.get('rules'))

            # Get the new next page
            pagination = nextData['pagination']
            nextPagination = pagination.get('next')

        # Remove pagination not needed more
        del data['pagination']

        # Convert the data to a string
        json_str = json.dumps(data)

        # Write to s3 bucket
        today = date.today()
        date_string = today.strftime('%Y-%m-%d')
        client.put_object(Body=json_str, Bucket='data-sch-digitalanalytics-dev',
                          Key=f'segment-backups/TrackingPlans/{date_string}/{trackingPlanID}({tracking_plan["name"]}).json')

    return {
        'statusCode': 200,
        'body': json.dumps("Tracking plan backup done!")
    }