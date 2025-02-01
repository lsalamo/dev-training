import os
import pandas as pd
from datetime import datetime
from typing import List, Dict

# adding libraries folder to the system path
from libs import (
    api as f_api,
    datetime_formatter as dtf,
)

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


class DataAPI(f_api.API):
    SITES = {
        "mnet": {"str": "mnet", "id": "468831764"},
        "cnet": {"str": "cnet", "id": "468895087"},
        "cnet_pro": {"str": "cnet_pro", "id": "468862598"},
        "ma": {"str": "ma", "id": "468813852"},
        "ijes": {"str": "ijes", "id": "469909922"},
        "ijit": {"str": "ijit", "id": "468741124"},
        "ij_epreselec": {"str": "ij_epreselec", "id": "468833127"},
        "fc": {"str": "fc", "id": "468839303"},
        "hab": {"str": "hab", "id": "468883032"},
        "fc_pro": {"str": "fc_pro", "id": "468820556"},
    }

    def __init__(self, config: Dict[str, str]):
        super().__init__()

        # configuration
        self.config = config

        # authentication
        self.__authentication()

        # initialization constructor analytics data
        self.client = BetaAnalyticsDataClient()

    def __authentication(self):
        file_creds = self.config["google"]["credentials"]["path_service_account"]
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_creds

    def _process_site(self, site: str):
        site_id = self.SITES[site]["id"]
        self.log.print("DataAPI._process_site", f"site_id: {site_id}")
        return site_id

    def _process_dates(self, from_date: str, to_date: str) -> str:
        from_datetime = dtf.DatetimeFormatter.str_to_datetime(from_date, "%Y-%m-%d")
        to_datetime = dtf.DatetimeFormatter.str_to_datetime(to_date, "%Y-%m-%d")

        from_date = dtf.DatetimeFormatter.datetime_to_str(from_datetime, "%Y-%m-%d")
        to_date = dtf.DatetimeFormatter.datetime_to_str(to_datetime, "%Y-%m-%d")

        date_range = f"{from_date}/{to_date}"
        self.log.print("DataAPI._process_dates", f"date: {date_range}")
        return from_date, to_date

    def reports(
        self,
        site: str,
        dimensions: str,
        metrics: str,
        date_ranges: Dict[str, str],
        dimension_filter: List[Dict[str, str]] = None,
        order_bys: Dict[str, str] = None,
    ) -> pd.DataFrame:
        google_dimensions = [Dimension(name=dimension.strip()) for dimension in dimensions.split(",")]
        google_metrics = [Metric(name=metric.strip()) for metric in metrics.split(",")]

        # date ranges
        from_date, to_date = self._process_dates(date_ranges["start_date"], date_ranges["to_date"])
        google_date_ranges = [
            DateRange(
                start_date=from_date,
                end_date=to_date,
            )
        ]
        google_dimension_filter = self.__get_dimension_filter(dimension_filter) if dimension_filter else None
        google_order_bys = self.__get_order_bys(order_bys) if order_bys else None

        request = RunReportRequest(
            property=f"properties/{self._process_site(site)}",
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

        self.log.print("DataAPI.reports", "report requested successfully!")
        return pd.DataFrame(output)

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
