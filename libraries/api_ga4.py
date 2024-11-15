import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import OrderBy
from google.analytics.data_v1beta.types import Filter
from google.analytics.data_v1beta.types import FilterExpression
from google.analytics.data_v1beta.types import FilterExpressionList
from google.analytics.data_v1beta.types import RunReportRequest


class GA4_API:
    property_fotocasaes = '296810976'
    property_motosnet = '273930537'
    property_cochesnet = '313836548'
    property_milanuncioscom = '330577361'
    property_infojobsnet = '330615843'
    property_infojobsit = '330589193'

    def __init__(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/luis.salamo/Documents/github/python-training/google/google-credentials.json"
        # Using a default constructor instructs the client to use the credentials
        # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
        self.client = BetaAnalyticsDataClient()

    def request(self, property_id: str, dimensions: str, metrics: str, date_ranges: dict, dimension_filter) -> pd.DataFrame:
        # Dimensions
        list_dimensions = dimensions.split(',')
        for index, dimension in enumerate(list_dimensions):
            list_dimensions[index] = Dimension(name=dimension)

        # Metrics
        list_metrics = metrics.split(',')
        for index, metric in enumerate(list_metrics):
            list_metrics[index] = Metric(name=metric)

        # Date Ranges
        date_ranges = [DateRange(start_date=date_ranges['start_date'], end_date=date_ranges['end_date'])]

        # Order bys
        # if order_bys['type'] == 'dimension':
        #     order_bys = [OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name=order_bys['dimension']), desc=order_bys['desc'])]
        # else:
        #     order_bys = [OrderBy(dimension=OrderBy.MetricOrderBy(metric_name=order_bys['metric']), desc=order_bys['desc'])]

        # Dimension Filter
        if dimension_filter:
            dimension_filter = FilterExpression(
                    and_group=FilterExpressionList(
                        expressions=[
                            FilterExpression(
                                filter=Filter(
                                    field_name=dimension_filter['dimension'],
                                    string_filter=Filter.StringFilter(
                                        match_type=Filter.StringFilter.MatchType.EXACT,
                                        value=dimension_filter['value'],
                                    ),
                                )
                            )
                        ]
                    )
                )

        request = RunReportRequest(
            property=f'properties/{property_id}',
            dimensions=list_dimensions,
            metrics=list_metrics,
            date_ranges=date_ranges,
            # order_bys=order_bys,
            dimension_filter=dimension_filter
        )
        response = self.client.run_report(request)

        # save dataframe
        output = []
        for row in response.rows:
            dict_row = {}
            for index, dimension in enumerate(dimensions.split(',')):
                dict_row[dimension] = row.dimension_values[index].value
            for index, metric in enumerate(metrics.split(',')):
                dict_row[metric] = row.metric_values[index].value
            output.append(dict_row)

        df = pd.DataFrame(output)
        return df




