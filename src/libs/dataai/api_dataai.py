import pandas as pd
import libs.api as api
import urllib
import time


class DataaiApi(api.API):
    def __init__(self, method, url, payload, token):
        self.date_from = ''
        self.to_date = ''
        self.max_retry = 60

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip',
            'Authorization': 'Bearer ' + token
        }
        super().__init__(method, url, headers, payload)


class report_downloads(DataaiApi):
    def __init__(self, token):
        url = ''
        payload = {}
        super().__init__('GET', url, payload, token)

    def request(self, vertical, products):
        parameters = {
            'product_id': products,
            # 'company_id': 'XXXXX',
            'granularity': 'monthly',  # daily, monthly, yearly
            'start_date': self.date_from,
            'end_date': self.to_date,
            'countries': 'ES',  # US,JP,CN'
            'devices': 'android-all,ios-all',
            'bundles': 'all_supported'  # all_supported,download_channel
        }

        # STEP-1: SCHEDULE A REPORT
        df = pd.DataFrame(columns=['date', 'vertical', 'platform', 'product', 'product_id', 'downloads', 'downloads_organic', 'downloads_paid'])
        self.url = f'https://api.data.ai/v2.0/portfolio/download-channel?{urllib.parse.urlencode(parameters)}'
        response = super().request()
        if 'report_id' in response:
            report_id = response['report_id']
            print(f'REQUEST SUBMITTED, REPORT_ID: <{report_id}>')
            for i in range(self.max_retry):
                if not df.empty:
                    break
                # STEP-2: DOWNLOAD REPORT
                self.url = f'https://api.data.ai/v2.0/portfolio/fetch-data?report_id={report_id}'
                response = super().request()
                report_status = response['report_status']
                if report_status == 'progressing':
                    print('NOT YET READY, RETRYING... [{}]'.format(i + 1))
                    time.sleep(10)
                elif report_status == 'done':
                    print('REPORT IS READY, DOWNLOADING...')
                    products = response['products']
                    if len(products) > 0:
                        for product in products:
                            product_id = product['product_id']
                            platform = str(product['device_code']).replace('-all', '')
                            product_name = str(product["unified_product_name"]).lower()
                            for product_detail in product['download_channel']:
                                downloads = product_detail["est_download"]
                                downloads_paid = product_detail["est_paid_ads_download"] + product_detail["est_paid_search_download"]
                                row_values = [{
                                    'date': product_detail["start_date"],
                                    'vertical': vertical,
                                    'platform': platform,
                                    'product': product_name,
                                    'product_id': product_id,
                                    'downloads': downloads,
                                    'downloads_organic': downloads - downloads_paid,
                                    'downloads_paid': downloads_paid
                                }]
                                df_row = pd.DataFrame(row_values)
                                df = pd.concat([df, df_row], ignore_index=True)
                else:
                    raise RuntimeError('FAILED TO REQUEST REPORT DATA... [{}]'.format(i + 1))
        return df


class ratings_history(DataaiApi):
    def __init__(self, token):
        url = ''
        payload = {}
        super().__init__('GET', url, payload, token)

    def request_and(self, vertical, products):
        url = 'https://api.data.ai/v1.3/intelligence/apps/google-play/app/ratings_history'
        device = 'android-all'
        return self.request(url, device, vertical, products)

    def request_ios(self, vertical, products):
        url = 'https://api.data.ai/v1.3/intelligence/apps/ios/app/ratings_history'
        device = 'ios-all'
        return self.request(url, device, vertical, products)

    def request(self, url, device, vertical, products):
        parameters = {
            'product_ids': products,
            'granularity': 'monthly',  # daily | weekly | monthly
            'start_date': self.date_from,
            'end_date': self.to_date,
            'countries': 'ES',  # 'US,JP,CN'
            'device': device,
            'feeds': 'cumulative_ratings'
        }
        df = pd.DataFrame(columns=['date', 'vertical', 'platform', 'product_id', 'average', 'total_rating', 'rating_five', 'rating_four', 'rating_three', 'rating_two', 'rating_one'])
        self.url = f'{url}?{urllib.parse.urlencode(parameters)}'
        response = super().request()
        if 'list' in response:
            rows = response['list']
            if len(rows) > 0:
                for row in rows:
                    date = row["date"]
                    platform = device.replace('-all', '')
                    product_id = row["product_id"]
                    row_values = [{
                        'date': date,
                        'vertical': vertical,
                        'platform': platform,
                        'product_id': product_id,
                        'average': row["average"],
                        'total_rating': row['total_count'],
                        'rating_five': row['five'],
                        'rating_four': row['four'],
                        'rating_three': row["three"],
                        'rating_two': row["two"],
                        'rating_one': row["one"]
                    }]
                    df_row = pd.DataFrame(row_values)
                    df = pd.concat([df, df_row], ignore_index=True)
        return df


class report_active_users(DataaiApi):
    def __init__(self, token):
        url = ''
        payload = {}
        super().__init__('GET', url, payload, token)

    def request(self, vertical, products):
        parameters = {
            'product_id': products,
            # 'company_id': 'XXXXX',
            'granularity': 'monthly',  # daily, monthly, yearly
            'start_date': self.date_from,
            'end_date': self.to_date,
            'countries': 'ES',  # US,JP,CN'
            'devices': 'android-all,ios-all',
            'bundles': 'active_users'  # all_supported,download_revenue,active_users,engagement,install_metrics,demographics,retention,cross_app_usage
        }

        # STEP-1: SCHEDULE A REPORT
        df = pd.DataFrame(columns=['date', 'vertical', 'platform', 'product', 'product_id', 'active_users'])
        self.url = f'https://api.data.ai/v2.0/portfolio/app-performance?{urllib.parse.urlencode(parameters)}'
        response = super().request()
        if 'report_id' in response:
            report_id = response['report_id']
            print(f'REQUEST SUBMITTED, REPORT_ID: <{report_id}>')
            for i in range(self.max_retry):
                if not df.empty:
                    break
                # STEP-2: DOWNLOAD REPORT
                self.url = f'https://api.data.ai/v2.0/portfolio/fetch-data?report_id={report_id}'
                response = super().request()
                report_status = response['report_status']
                if report_status == 'progressing':
                    print('NOT YET READY, RETRYING... [{}]'.format(i + 1))
                    time.sleep(10)
                elif report_status == 'done':
                    print('REPORT IS READY, DOWNLOADING...')
                    products = response['products']
                    if len(products) > 0:
                        for product in products:
                            product_id = product['product_id']
                            platform = str(product['device_code']).replace('-all', '')
                            product_name = str(product["unified_product_name"]).lower()
                            for product_detail in product['app_performance']:
                                row_values = [{
                                    'date': product_detail["start_date"],
                                    'vertical': vertical,
                                    'platform': platform,
                                    'product': product_name,
                                    'product_id': product_id,
                                    'active_users': product_detail["est_average_active_users"]
                                }]
                                df_row = pd.DataFrame(row_values)
                                df = pd.concat([df, df_row], ignore_index=True)
                else:
                    raise RuntimeError('FAILED TO REQUEST REPORT DATA... [{}]'.format(i + 1))
        return df


