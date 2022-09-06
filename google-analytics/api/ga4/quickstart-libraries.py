import sys
sys.path.insert(0, '/Users/luis.salamo/Documents/github enterprise/python-training/libraries')
import constants
import api_ga4 as f_api


def sample_run_report():
    api = f_api.API_GA4()
    df = api.request(property_id, dimensions, metrics, date_ranges, order_bys)
    return df


if __name__ == "__main__":
    property_id = constants.GA4.property.motosnet
    dimensions = 'date'
    metrics = 'sessions,totalUsers'
    date_ranges = {'start_date': '2022-08-01', 'end_date': 'today'}
    order_bys = {'type': 'dimension', 'dimension': 'date', 'desc': False}
    # order_bys = {'type': 'metric', 'metric': 'sessions', 'desc': False}
    sample_run_report()

print('> END EXECUTION')


