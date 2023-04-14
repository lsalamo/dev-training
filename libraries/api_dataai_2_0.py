import pandas as pd
import libraries.api as api
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
            # 'company_id': '1000200000061645',
            'granularity': 'monthly',  # daily | weekly | monthly
            'start_date': self.date_from,
            'end_date': self.to_date,
            'countries': 'ES',  # 'US,JP,CN'
            'devices': 'android-all,ios-all',
            'bundles': 'all_supported'
        }

        # STEP-1: SCHEDULE A REPORT
        df = pd.DataFrame(columns=['day', 'vertical', 'platform', 'product', 'downloads'])
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
                    print('REPORT IS READY, DOWNLOADING...\n')
                    rows = response['products']
                    if len(rows) > 0:
                        for row in rows:
                            platform = str(row['device_code']).replace('-all', '')
                            product_name = str(row["unified_product_name"]).lower()
                            for downloads in row['download_channel']:
                                print(f'plaform:{platform} - product_name:{product_name} - fecha:{downloads["start_date"]} - downloads:{downloads["est_download"]}')
                                # df = df.append({'day': downloads["start_date"], 'vertical': vertical, 'platform': platform, 'product': product_name, 'downloads': downloads["est_download"]}, ignore_index=True)
                                row_values = [{'day': downloads["start_date"], 'vertical': vertical, 'platform': platform, 'product': product_name, 'downloads': downloads["est_download"]}]
                                df_row = pd.DataFrame(row_values)
                                df = pd.concat([df, df_row], ignore_index=True)
                    # df = pd.pivot_table(df, values=['downloads'], index=['day'], columns=['product'])
                    # df = df.reset_index()
                    # df.columns = df.columns.droplevel(level=0)
                    # df = f_df.Dataframe.Cast.columns_regex_to_int64(df, '-(android|ios)$')
                else:
                    raise RuntimeError('FAILED TO REQUEST REPORT DATA... [{}]'.format(i + 1))
        return df



