import sys
sys.path.insert(0, '/Users/luis.salamo/Documents/github enterprise/python-training/libraries')
import constants
import api_ga4 as f_api_ga4


def sample_run_report():
    api = f_api_ga4.API_GA4()
    df = api.request(property_id, dimensions, metrics, date_ranges, order_bys, dimension_filter)
    return df


if __name__ == "__main__":
    property_id = constants.GA4.property.motosnet
    dimensions = 'date'
    metrics = 'sessions,totalUsers'
    date_ranges = {'start_date': '2022-08-01', 'end_date': 'today'}
    order_bys = {'type': 'dimension', 'dimension': 'date', 'desc': False}
    # order_bys = {'type': 'metric', 'metric': 'sessions', 'desc': False}
    dimension_filter = {'dimension': 'platform', 'value': constants.GA4.platform.web}
    sample_run_report()

print('> END EXECUTION')


