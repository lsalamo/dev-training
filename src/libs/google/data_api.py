import os
import pandas as pd
from datetime import datetime
from typing import List, Dict

# adding libraries folder to the system path
from libs import (
    api as f_api,
    datetime_formatter as df,
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
        "car_factory": {"str": "car_factory", "id": ""},
        "ma": {"str": "ma", "id": "330577361"},
        "ijes": {"str": "ijes", "id": "330615843"},
        "ijit": {"str": "ijit", "id": "330589193"},
        "ij_epreselec": {"str": "ij_epreselec", "id": ""},
        "fc": {"str": "fc", "id": "296810976"},
        "hab": {"str": "hab", "id": ""},
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
        config = self.config["google"]["credentials"]
        file_creds = config["path_service_account"]
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_creds

    def _process_site(self, site: str):
        site_id = self.SITES[site]["id"]
        self.log.print("DataAPI._process_site", f"site_id: {site_id}")
        return site_id

    def _process_dates(self, from_date: str, to_date: str) -> str:
        def parse_date(date_str: str) -> datetime:
            if date_str == "today":
                return df.DatetimeFormatter.get_current_datetime()
            elif date_str == "yesterday":
                return df.DatetimeFormatter.datetime_add_days(days=-1)
            elif date_str == "7daysAgo":
                return df.DatetimeFormatter.datetime_add_days(days=-7)
            else:
                return df.DatetimeFormatter.str_to_datetime(to_date, "%Y-%m-%d")

        from_datetime = parse_date(from_date)
        to_datetime = parse_date(to_date)

        from_date = df.DatetimeFormatter.datetime_to_str(from_datetime, "%Y-%m-%d")
        to_date = df.DatetimeFormatter.datetime_to_str(to_datetime, "%Y-%m-%d")

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
