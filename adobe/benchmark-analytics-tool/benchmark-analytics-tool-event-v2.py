import pandas as pd
import sys

# adding libraries folder to the system path
sys.path.insert(0, '/Users/luis.salamo/Documents/github enterprise/python-training/libraries')

import constants
import functions as f
import api_adobe_analytics2_0 as f_api_adobe
import api_ga4 as f_api_ga4
import dataframe as f_df
import logs as f_log


def init():
    f.Directory.create_directory('csv')


def clean_columns(df, platform, to_columns):
    to_columns = to_columns.replace('{{platform}}', platform).split(',')
    if not f_df.Dataframe.is_empty(df):
        df.columns = to_columns
        df['event'] = df['event'].str.lower().replace(' ', '_', regex=True)
    return df


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================
class Adobe:
    def __init__(self):
        self.site_id = variables['site_aa']
        self.columns = variables['columns']
        self.from_date = variables['from_date']
        self.to_date = variables['to_date']

    def get_adobe(self, file_payload, columns_join):
        # request
        access_token = f_api_adobe.Adobe_JWT.get_access_token()
        api = f_api_adobe.Adobe_Report_API(self.site_id, file_payload, self.from_date, self.to_date, access_token)
        df = api.request()
        # web
        from_columns = ('value', 0, 1, 2)
        df_web = clean_columns(df.loc[:, from_columns], constants.PLATFORM_WEB, self.columns)
        # android
        from_columns = ['value', 3, 4, 5]
        df_and = clean_columns(df.loc[:, from_columns], constants.PLATFORM_ANDROID, self.columns)
        # ios
        from_columns = ['value', 6, 7, 8]
        df_ios = clean_columns(df.loc[:, from_columns], constants.PLATFORM_IOS, self.columns)
        # join dataframes
        frames = [df_web, df_and, df_ios]
        df = f_df.Dataframe.Columns.join_by_columns(frames, columns_join, 'outer')

        log.print('get_adobe', 'dataframe loaded <' + file_payload + '>')
        return df


# =============================================================================
#   REQUEST GOOGLE ANALYTICS
# =============================================================================
class Google:
    def __init__(self):
        self.site_id = variables['site_ga']
        self.columns = variables['columns']
        self.from_date = variables['from_date']
        self.to_date = variables['to_date']

    def get_google(self):
        def get_platform(platform):
            df_platform = df.loc[df['platform'] == platform]
            # change column names
            df_platform = f_df.Dataframe.Columns.drop(df_platform, ['customEvent:custom_link', 'platform'])
            df_platform.columns = self.columns.replace('{{platform}}', platform[0:3].lower()).split(',')
            return df_platform

        api = f_api_ga4.GA4_API()
        dimensions = 'eventName,customEvent:custom_link,platform'
        metrics = 'eventCount,sessions,totalUsers'
        date_ranges = {'start_date': self.from_date, 'end_date': self.to_date}

        # Request
        df = api.request(self.site_id, dimensions, metrics, date_ranges)

        # Cast
        df["customEvent:custom_link"] = df["customEvent:custom_link"].replace("(not set)", None)
        df.loc[df['customEvent:custom_link'].notnull(), 'eventName'] = df.loc[df['customEvent:custom_link'].notnull(), 'customEvent:custom_link']
        df['eventName'] = df['eventName'].str.lower().replace(' ', '_', regex=True)

        # Get Platform
        df_web = get_platform(constants.GA4.platform.web)
        df_and = get_platform(constants.GA4.platform.android)
        df_ios = get_platform(constants.GA4.platform.ios)

        # join dataframes
        frames = [df_web, df_and, df_ios]
        df = f_df.Dataframe.Columns.join_by_columns(frames, 'event', 'outer')

        log.print('get_google', 'dataframe loaded')
        return df


def merge_adobe_google(df_aa, df_ga):
    df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_aa, df_ga, ['event'], 'outer', ('-aa', '-ga'))
    df = df.fillna(0)
    df_values = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(web-|and-|ios-)')
    df[df_values.columns] = df_values
    log.print('merge_adobe_google', 'data loaded')
    return df


def get_csv_by_platform(df):
    def get_platform(platform):
        columns = variables['columns_tools'].replace('{{platform}}', platform).split(',')
        df_platform = df[columns]
        df_platform = df_platform.loc[~((df[platform + '-count-aa'] == 0) & (df[platform + '-count-ga'] == 0))]
        df_platform.reset_index(drop=True, inplace=True)
        # Sort
        df_platform.sort_values(by=platform + '-count-aa', ascending=False, inplace=True)
        f.CSV.dataframe_to_file(df_platform, 'df_' + platform + '.csv')
        return df_platform

    result['df_web'] = get_platform(constants.PLATFORM_WEB)
    log.print('get_csv_by_platform', 'web - data loaded')
    result['df_and'] = get_platform(constants.PLATFORM_ANDROID)
    log.print('get_csv_by_platform', 'and - data loaded')
    result['df_ios'] = get_platform(constants.PLATFORM_IOS)
    log.print('get_csv_by_platform', 'ios - data loaded')


# =============================================================================
#   MAIN
# =============================================================================

# main function
if __name__ == '__main__':
    result = {}
    site = {
        'motosnet': {'str': 'motosnet', 'aa': f_api_adobe.Adobe_API.rs_motosnet, 'ga': f_api_ga4.GA4_API.property_motosnet},
        'cochesnet': {'str': 'cochesnet', 'aa': f_api_adobe.Adobe_API.rs_cochesnet, 'ga': f_api_ga4.GA4_API.property_cochesnet},
    }
    site = site['motosnet']
    variables = {
        'from_date': '2022-09-14',
        'to_date': '2022-09-14',
        'site_aa': site['aa'],
        'site_ga': site['ga'],
        'columns': 'event,{{platform}}-count,{{platform}}-visits,{{platform}}-visitors',
        'columns_tools': 'event,{{platform}}-count-aa,{{platform}}-count-ga,{{platform}}-visits-aa,{{platform}}-visits-ga,{{platform}}-visitors-aa,{{platform}}-visitors-ga'
    }

    # Logging
    log = f_log.Logging()
    log.print('=====================================  BEGIN EXECUTION =====================================', '')

    init()
    log.print('site', site['str'])

    adobe = Adobe()
    result['df_aa'] = adobe.get_adobe('aa/request-events.json', ['event'])
    google = Google()
    result['df_ga'] = google.get_google()

    result['df'] = merge_adobe_google(result['df_aa'], result['df_ga'])
    get_csv_by_platform(result['df'])

log.print('=====================================  END EXECUTION =====================================', '')


