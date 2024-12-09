import os
import pandas as pd
import array

# importing GA4 API class
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    Filter,
    FilterExpression,
    FilterExpressionList,
    OrderBy,
    RunReportRequest,
)

class GA4_API:
    def __init__(self, property):
        # Public properties
        self.property_fotocasaes = '296810976'
        # self.property_motosnet = '273930537'
        self.property_motosnet = '468831764'
        self.property_cochesnet = '313836548'
        self.property_milanuncioscom = '330577361'
        self.property_infojobsnet = '330615843'
        self.property_infojobsit = '330589193'
        self.platform_web = 'web'
        self.platform_android = 'android'
        self.platform_ios = 'ios'

        properties = {
            'mnet': {'str': 'mnet', 'ga': self.property_motosnet},
            'cnet': {'str': 'cnet', 'ga': self.property_cochesnet},
            'ma': {'str': 'ma', 'ga': self.property_milanuncioscom},
            'ijes': {'str': 'ijes', 'ga': self.property_infojobsnet},
            'ijit': {'str': 'ijit', 'ga': self.property_infojobsit},
            'fc': {'str': 'fc', 'ga': self.property_fotocasaes}
        }  
        self.property = property     
        self.property_id = properties[property]['ga']    

        # export GOOGLE_APPLICATION_CREDENTIALS = / path / to / credentials.json
        path_creds = os.path.join(os.getcwd(), "src/google/google_analytics/credentials.json")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_creds        

        # Initialize client specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
        self.client = BetaAnalyticsDataClient()

    def request(self, dimensions: str, metrics: str, date_ranges: dict, dimension_filter:list, order_bys: dict) -> pd.DataFrame:
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
        if order_bys:
            if order_bys['type'] == 'dimension':
                order_bys = [OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name=order_bys['value']), desc=order_bys['desc'])]
            elif order_bys['type'] == 'metric':
                order_bys = [OrderBy(dimension=OrderBy.MetricOrderBy(metric_name=order_bys['value']), desc=order_bys['desc'])]       

        # Dimension Filter
        if dimension_filter:
            filter_expressions = []
            for filter in dimension_filter:
                filter_expressions.append(
                    FilterExpression(
                        filter=Filter(
                            field_name=filter['dimension'],
                            string_filter=Filter.StringFilter(
                                match_type=Filter.StringFilter.MatchType.EXACT,
                                value=filter['value'],
                            ),
                        )
                    )
                )
            dimension_filter = FilterExpression(
                and_group=FilterExpressionList(
                    expressions=filter_expressions
                )
            )

        request = RunReportRequest(
            property=f'properties/{self.property_id}',
            dimensions=list_dimensions,
            metrics=list_metrics,
            date_ranges=date_ranges,
            dimension_filter=dimension_filter,
            order_bys=order_bys,
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




