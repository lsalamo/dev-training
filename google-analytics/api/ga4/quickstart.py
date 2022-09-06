"""
Google Analytics Data API sample quickstart application.
This application demonstrates the usage of the Analytics Data API using
service account credentials.
API:
  https://github.com/googleapis/python-analytics-data
Usage:
  pip3 install --upgrade google-analytics-data
  python3 quickstart.py
"""
import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import OrderBy
from google.analytics.data_v1beta.types import RunReportRequest


def sample_run_report(property_id):
    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f'properties/{property_id}',
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="sessions"), Metric(name="totalUsers")],
        date_ranges=[DateRange(start_date='2022-08-01', end_date="today")],
        # order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="date"), desc=True)]
        order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"), desc=False)]
    )
    response = client.run_report(request)

    # save dataframe
    output = []
    for row in response.rows:
        print(row.dimension_values[0].value, row.metric_values[0].value, row.metric_values[1].value)
        output.append({"date": row.dimension_values[0].value, "sessions": row.metric_values[0].value, "visitors": row.metric_values[1].value})
    df = pd.DataFrame(output)
    df.to_csv("csv/df.csv")


if __name__ == "__main__":
    # export GOOGLE_APPLICATION_CREDENTIALS = / path / to / credentials.json
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/luis.salamo/Documents/github enterprise/python-training/google-analytics/google-application-credentials.json"
    property_id_motos = "273930537"
    sample_run_report(property_id_motos)

print('> END EXECUTION')


