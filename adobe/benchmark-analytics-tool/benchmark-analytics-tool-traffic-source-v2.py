import pandas as pd
import sys
import os

# adding libraries folder to the system path
sys.path.insert(0, '/Users/luis.salamo/Documents/github/python-training/libraries')

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
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)
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
        # transform
        df = f_df.Dataframe.Rows.replace(df, 'Unspecified', 'Internal')
        if not f_df.Dataframe.is_empty(df):
            df = df[['value', 0, 1, 2, 3]]
            # transform
            df.columns = self.columns.replace('{{platform}}', self.platform).split(',')
            df = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(web-|and-|ios-)')
            df.sort_values(by=self.platform + '-visits', ascending=False, inplace=True)
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
        dimensions = 'sessionDefaultChannelGrouping,sessionMedium,sessionSource,platform'
        metrics = 'sessions,totalUsers,screenPageViews,conversions'
        date_ranges = {'start_date': self.from_date, 'end_date': self.to_date}
        dimension_filter = {'dimension': 'appVersion', 'value': self.app_version} if self.app_version else None
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
            df.loc[(df['sessionMedium'] == 'display') & (df['sessionSource'].str.startswith('retargeting-')), 'sessionDefaultChannelGrouping'] = 'Retargeting'
            df = f_df.Dataframe.Columns.drop(df, ['sessionMedium', 'sessionSource', 'platform'])
            df.columns = self.columns.replace('{{platform}}', self.platform).split(',')
            df = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(web-|and-|ios-)')
            df = df.groupby(self.column_join, as_index=False).sum()
            df.sort_values(by=self.platform + '-visits', ascending=False, inplace=True)
        # log
        log.print('get_google', 'dataframe loaded')
        return df

    def get_google_by_medium_source_csv(self):
        df = f.CSV.csv_to_dataframe(self.google_csv_file)
        df.loc[(df[1].str.lower() == 'display') & (df[2].str.lower().str.startswith('retargeting-')), 0] = 'Retargeting'
        df = f_df.Dataframe.Columns.drop(df, [1, 2])
        df.columns = self.columns.replace('{{platform}}', self.platform).split(',')
        df = df.groupby([variables['column_join']], as_index=False).sum()
        df.sort_values(by=self.platform + '-visits', ascending=False, inplace=True)
        # log
        log.print('get_google_by_medium_source_csv', 'dataframe loaded')
        return df


def merge_adobe_google(df_aa, df_ga):
    # transform
    df_ga = f_df.Dataframe.Rows.replace(df_ga, 'Organic Search', 'Natural Search')
    df_ga = f_df.Dataframe.Rows.replace(df_ga, 'Organic Social', 'Social Media')
    df_ga = f_df.Dataframe.Rows.replace(df_ga, 'Paid Social', 'Social Paid')
    df_ga = f_df.Dataframe.Rows.replace(df_ga, 'Mobile Push Notifications', 'Push Notification')
    df_ga = f_df.Dataframe.Rows.replace(df_ga, 'Cross-network', 'Cross Sites')
    df_ga = f_df.Dataframe.Rows.replace(df_ga, 'Referral', 'Referring Domains')
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
        'payload_aa': 'aa/request-{{platform}}-marketing-channel.json',
        'column_join': 'medium',
        'google_csv_file': '/Users/luis.salamo/Downloads/1.csv',
        'columns': 'medium,{{platform}}-visits,{{platform}}-visitors,{{platform}}-views,{{platform}}-conversions',
        'columns_tools': 'medium,{{platform}}-visits-aa,{{platform}}-visits-ga,{{platform}}-visitors-aa,{{platform}}-visitors-ga,{{platform}}-views-aa,{{platform}}-views-ga,{{platform}}-conversions-aa,{{platform}}-conversions-ga'
    }

    # Logging
    log = f_log.Logging()

    init()
    log.print('site', site['str'])

    adobe = Adobe()
    result['df_aa'] = adobe.get_adobe()
    google = Google()
    result['df_ga'] = google.get_google()
    result['df_ga_by_session_medium'] = google.get_google_by_medium_source_csv()

    result['df'] = merge_adobe_google(result['df_aa'], result['df_ga'])
    result['df_by_session_medium'] = merge_adobe_google(result['df_aa'], result['df_ga_by_session_medium'])

    # export csv
    f.CSV.dataframe_to_file(result['df'], 'df_traffic_source_' + site['str'] + '_' + variables['platform'] + '.csv')
    f.CSV.dataframe_to_file(result['df_by_session_medium'], 'df_session_medium_' + site['str'] + '_' + variables['platform'] + '.csv')

    log.print('================ END ================', '')


