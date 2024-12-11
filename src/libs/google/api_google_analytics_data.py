import os
import pandas as pd
from typing import List, Dict

# adding libraries folder to the system path
from libs import api as f_api

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


class GoogleAnalyticsData(f_api.API):
    PROPERTIES = {
        "mnet": {"str": "mnet", "ga": "468831764"},
        "cnet": {"str": "cnet", "ga": "313836548"},
        "ma": {"str": "ma", "ga": "330577361"},
        "ijes": {"str": "ijes", "ga": "330615843"},
        "ijit": {"str": "ijit", "ga": "330589193"},
        "fc": {"str": "fc", "ga": "296810976"},
    }

    def __init__(self, config):
        # configuration
        self.config = config
        self.property = config["property"]
        self.property_id = self.PROPERTIES[self.property]["ga"]
        self.platform = config["platform"]

        # authentication
        self.__authentication()

        # initialization constructor analytics data
        self.client = BetaAnalyticsDataClient()

    def __authentication(self):
        file_creds = self.config["credentials"]["path_service_account"]
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_creds

    def request(
        self,
        dimensions: str,
        metrics: str,
        date_ranges: Dict[str, str],
        dimension_filter: List[Dict[str, str]] = None,
        order_bys: Dict[str, str] = None,
    ) -> pd.DataFrame:
        google_dimensions = [Dimension(name=dimension.strip()) for dimension in dimensions.split(",")]
        google_metrics = [Metric(name=metric.strip()) for metric in metrics.split(",")]
        google_date_ranges = [
            DateRange(
                start_date=date_ranges["start_date"],
                end_date=date_ranges["end_date"],
            )
        ]
        google_dimension_filter = self.__get_dimension_filter(dimension_filter) if dimension_filter else None
        google_order_bys = self.__get_order_bys(order_bys) if order_bys else None

        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=google_dimensions,
            metrics=google_metrics,
            date_ranges=google_date_ranges,
            dimension_filter=google_dimension_filter,
            order_bys=google_order_bys,
        )
        response = self.client.run_report(request)

        # save dataframe
        output = []
        for row in response.rows:
            dict_row = {}
            for index, dimension in enumerate(dimensions.split(",")):
                dict_row[dimension] = row.dimension_values[index].value
            for index, metric in enumerate(metrics.split(",")):
                dict_row[metric] = row.metric_values[index].value
            output.append(dict_row)

        df = pd.DataFrame(output)
        return df

    def __get_dimension_filter(self, dimension_filter: List[Dict[str, str]]) -> FilterExpression:
        filter_expressions = [
            FilterExpression(
                filter=Filter(
                    field_name=filter["dimension"],
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.EXACT,
                        value=filter["value"],
                    ),
                )
            )
            for filter in dimension_filter
        ]
        return FilterExpression(and_group=FilterExpressionList(expressions=filter_expressions))

    def __get_order_bys(self, order_bys: Dict[str, str]) -> List[OrderBy]:
        if order_bys["type"] == "dimension":
            return [
                OrderBy(
                    dimension=OrderBy.DimensionOrderBy(dimension_name=order_bys["value"]),
                    desc=order_bys["desc"],
                )
            ]
        elif order_bys["type"] == "metric":
            return [
                OrderBy(
                    metric=OrderBy.MetricOrderBy(metric_name=order_bys["value"]),
                    desc=order_bys["desc"],
                )
            ]
        return None
