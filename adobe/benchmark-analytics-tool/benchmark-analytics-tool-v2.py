import pandas as pd
import sys
import os

# adding libraries folder to the system path
sys.path.insert(0, '/Users/luis.salamo/Documents/github enterprise/python-training/libraries')

import constants
import functions as f
import api_adobe_analytics2_0 as f_api_adobe
import api_ga4 as f_api_ga4
import dataframe as f_df
import logs as f_log


def init():
    # args
    log.print('init', 'Total arguments passed: ' + str(len(sys.argv)))
    log.print('init', 'Name of Python script:: ' + sys.argv[0])
    for i in range(1, len(sys.argv)):
        log.print('init', 'Argument: ' + sys.argv[i])

    # directory
    os.chdir('/Users/luis.salamo/Documents/github enterprise/python-training/adobe/benchmark-analytics-tool')
    log.print('directory', os.getcwd())
    f.Directory.create_directory('csv')


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================
class Adobe:
    def __init__(self):
        self.site_id = variables['site_aa']
        self.payload = variables['payload_aa'].replace('{{platform}}', variables['platform'])
        self.column_join = variables['column_join']
        self.columns = variables['columns']
        self.from_date = variables['from_date']
        self.to_date = variables['to_date']
        self.platform = variables['platform']

    def get_adobe(self):
        # request
        access_token = f_api_adobe.Adobe_JWT.get_access_token()
        api = f_api_adobe.Adobe_Report_API(self.site_id, self.payload, self.from_date, self.to_date, access_token)
        df = api.request()
        if not f_df.Dataframe.is_empty(df):
            df = df[['value', 0, 1, 2]]
            # transform
            df.columns = self.columns.replace('{{platform}}', self.platform).split(',')
            df = f_df.Dataframe.Cast.columns_to_datetime(df, [variables['column_join']], '%Y%m%d')
            df = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(web-|and-|ios-)')
            df.sort_values(by=variables['column_join'], ascending=True, inplace=True)
        # log
        log.print('get_adobe', 'dataframe loaded <' + self.payload + '>')
        return df


# =============================================================================
#   REQUEST GOOGLE ANALYTICS
# =============================================================================
class Google:
    def __init__(self):
        self.site_id = variables['site_ga']
        self.column_join = variables['column_join']
        self.columns = variables['columns']
        self.from_date = variables['from_date']
        self.to_date = variables['to_date']
        self.google_csv_file = variables['google_csv_file']
        self.platform = variables['platform']
        self.app_version = variables['app_version']

    def get_google(self):
        # request
        api = f_api_ga4.GA4_API()
        dimensions = 'date,platform'
        metrics = 'sessions,totalUsers,screenPageViews'
        date_ranges = {'start_date': self.from_date, 'end_date': self.to_date}
        if self.app_version:
            dimension_filter = {'dimension': 'appVersion', 'value': self.app_version}
        df = api.request(self.site_id, dimensions, metrics, date_ranges, dimension_filter)
        if not f_df.Dataframe.is_empty(df):
            if self.platform == constants.PLATFORM_WEB:
                platform = constants.GA4.platform.web
            elif self.platform == constants.PLATFORM_ANDROID:
                platform = constants.GA4.platform.android
            elif self.platform == constants.PLATFORM_IOS:
                platform = constants.GA4.platform.ios
            # transform
            df = df.loc[df['platform'].str.lower() == platform]
            df = f_df.Dataframe.Columns.drop(df, ['platform'])
            df.columns = self.columns.replace('{{platform}}', self.platform).split(',')
            df = f_df.Dataframe.Cast.columns_to_datetime(df, [variables['column_join']], '%Y%m%d')
        # log
        log.print('get_google', 'dataframe loaded')
        return df

    def get_google_csv(self):
        df = f.CSV.csv_to_dataframe(self.google_csv_file)
        f_df.Dataframe.Columns.drop_from_index(df, 4, True)
        df.columns = self.columns.replace('{{platform}}', self.platform).split(',')
        return df


def merge_adobe_google(df_aa, df_ga):
    # merge
    df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_aa, df_ga, [variables['column_join']], 'outer', ('-aa', '-ga'))
    # transform
    df = df.fillna(0)
    df = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(web-|and-|ios-)')
    columns = variables['columns_tools'].replace('{{platform}}', variables['platform']).split(',')
    df = df[columns]
    # log
    log.print('merge_adobe_google', 'data loaded')
    return df


# =============================================================================
#   MAIN
# =============================================================================

# main function
if __name__ == '__main__':
    result = {}
    site = {
        'mnet': {'str': 'mnet', 'aa': f_api_adobe.Adobe_API.rs_motosnet, 'ga': f_api_ga4.GA4_API.property_motosnet},
        'cnet': {'str': 'cnet', 'aa': f_api_adobe.Adobe_API.rs_cochesnet, 'ga': f_api_ga4.GA4_API.property_cochesnet},
        'ma': {'str': 'ma', 'aa': f_api_adobe.Adobe_API.rs_milanuncioscom, 'ga': f_api_ga4.GA4_API.property_milanuncioscom},
        'ijes': {'str': 'ijes', 'aa': f_api_adobe.Adobe_API.rs_infojobsnet, 'ga': f_api_ga4.GA4_API.property_infojobsnet},
        'ijit': {'str': 'ijit', 'aa': f_api_adobe.Adobe_API.rs_infojobsit, 'ga': f_api_ga4.GA4_API.property_infojobsit},
        'fc': {'str': 'fc', 'aa': f_api_adobe.Adobe_API.rs_fotocasaes, 'ga': f_api_ga4.GA4_API.property_fotocasaes}
    }
    site = site[sys.argv[1]]
    variables = {
        'from_date': sys.argv[2],
        'to_date': sys.argv[3],
        'site_aa': site['aa'],
        'site_ga': site['ga'],
        'platform': sys.argv[4],
        'app_version': sys.argv[5] if len(sys.argv) == 6 else '',
        'payload_aa': 'aa/request-{{platform}}.json',
        'column_join': 'day',
        'google_csv_file': '/Users/luis.salamo/Downloads/1.csv',
        'columns': 'day,{{platform}}-visits,{{platform}}-visitors,{{platform}}-views',
        'columns_tools': 'day,{{platform}}-visits-aa,{{platform}}-visits-ga,{{platform}}-visitors-aa,{{platform}}-visitors-ga,{{platform}}-views-aa,{{platform}}-views-ga'
    }

    # Logging
    log = f_log.Logging()

    init()
    log.print('site', site['str'])

    adobe = Adobe()
    result['df_aa'] = adobe.get_adobe()
    google = Google()
    result['df_ga'] = google.get_google()
    # result['df_ga_csv'] = google.get_google_csv()

    result['df'] = merge_adobe_google(result['df_aa'], result['df_ga'])
    # result['df_csv'] = merge_adobe_google(result['df_aa'], result['df_ga_csv'])

    # export csv
    f.CSV.dataframe_to_file(result['df_aa'], 'df_aa_' + site['str'] + '_' + variables['platform'] + '.csv')
    f.CSV.dataframe_to_file(result['df_ga'], 'df_ga_' + site['str'] + '_' + variables['platform'] + '.csv')
    f.CSV.dataframe_to_file(result['df'], 'df_' + site['str'] + '_' + variables['platform'] + '.csv')
    # f.CSV.dataframe_to_file(result['df_csv'], 'df_csv_' + variables['platform'] + '.csv')

    log.print('================ END ================', '')
