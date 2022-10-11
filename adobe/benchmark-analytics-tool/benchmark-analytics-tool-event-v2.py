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
        self.payload = variables['payload_aa']
        self.column_join = variables['column_join']
        self.columns = variables['columns']
        self.from_date = variables['from_date']
        self.to_date = variables['to_date']

    def get_adobe(self):
        # request
        access_token = f_api_adobe.Adobe_JWT.get_access_token()
        api = f_api_adobe.Adobe_Report_API(self.site_id, self.payload, self.from_date, self.to_date, access_token)
        df = api.request()
        if not f_df.Dataframe.is_empty(df):
            # web
            df_web = df[['value', 0, 1, 2]]
            df_web.columns = self.columns.replace('{{platform}}', constants.PLATFORM_WEB).split(',')
            # android
            df_and = df[['value', 3, 4, 5]]
            df_and.columns = self.columns.replace('{{platform}}', constants.PLATFORM_ANDROID).split(',')
            # ios
            df_ios = df[['value', 6, 7, 8]]
            df_ios.columns = self.columns.replace('{{platform}}', constants.PLATFORM_IOS).split(',')
            # join dataframes
            frames = [df_web, df_and, df_ios]
            df = f_df.Dataframe.Columns.join_by_columns(frames, self.column_join, 'outer')
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

    def get_google(self):
        def get_platform(platform):
            df_platform = df.loc[df['platform'] == platform]
            # change column names
            # df_platform = f_df.Dataframe.Columns.drop(df_platform, ['customEvent:custom_link', 'platform'])
            df_platform = f_df.Dataframe.Columns.drop(df_platform, ['platform'])
            df_platform.columns = self.columns.replace('{{platform}}', platform[0:3].lower()).split(',')
            return df_platform

        # request
        api = f_api_ga4.GA4_API()
        # dimensions = 'eventName,customEvent:custom_link,platform'
        dimensions = 'eventName,platform'
        metrics = 'eventCount,sessions,totalUsers'
        date_ranges = {'start_date': self.from_date, 'end_date': self.to_date}
        df = api.request(self.site_id, dimensions, metrics, date_ranges)
        if not f_df.Dataframe.is_empty(df):
            # transform
            # df['eventName'] = df['eventName'].str.lower().replace(' ', '_', regex=True)
            # platform
            df_web = get_platform(constants.GA4.platform.web)
            df_and = get_platform(constants.GA4.platform.android)
            df_ios = get_platform(constants.GA4.platform.ios)
            # join dataframes
            frames = [df_web, df_and, df_ios]
            df = f_df.Dataframe.Columns.join_by_columns(frames, [self.column_join], 'outer')
        # log
        log.print('get_google', 'dataframe loaded')
        return df


def merge_adobe_google(df_aa, df_ga):
    # transform
    df_aa.loc[(df_aa[variables['column_join']].str.lower().str.endswith('viewed')) & (df_aa[variables['column_join']].str.lower() != 'experiment viewed'), variables['column_join']] = 'Page View'
    df_aa[variables['column_join']] = df_aa[variables['column_join']].str.lower().replace(' ', '_', regex=True)
    df_aa[variables['column_join']] = df_aa[variables['column_join']].str.slice(0, 40)
    df_aa = df_aa.groupby([variables['column_join']], as_index=False).sum()
    # merge
    df = f_df.Dataframe.Columns.join_two_frames_by_columns(df_aa, df_ga, [variables['column_join']], 'outer', ('-aa', '-ga'))
    # transform
    df = df.fillna(0)
    df = f_df.Dataframe.Cast.columns_regex_to_int64(df, '^(web-|and-|ios-)')
    # log
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
        f.CSV.dataframe_to_file(df_platform, 'df_event_' + platform + '.csv')
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
        'mnet': {'str': 'mnet', 'aa': f_api_adobe.Adobe_API.rs_motosnet, 'ga': f_api_ga4.GA4_API.property_motosnet},
        'cnet': {'str': 'cnet', 'aa': f_api_adobe.Adobe_API.rs_cochesnet, 'ga': f_api_ga4.GA4_API.property_cochesnet},
        'ma': {'str': 'ma', 'aa': f_api_adobe.Adobe_API.rs_milanuncioscom, 'ga': f_api_ga4.GA4_API.property_milanuncioscom},
        'ijes': {'str': 'ijes', 'aa': f_api_adobe.Adobe_API.rs_fotocasaes, 'ga': f_api_ga4.GA4_API.property_infojobsnet},
        'ijit': {'str': 'ijit', 'aa': f_api_adobe.Adobe_API.rs_infojobsit, 'ga': f_api_ga4.GA4_API.property_infojobsit},
        'fc': {'str': 'fc', 'aa': f_api_adobe.Adobe_API.rs_fotocasaes, 'ga': f_api_ga4.GA4_API.property_fotocasaes}
    }
    site = site[sys.argv[1]]
    variables = {
        'from_date': sys.argv[2],
        'to_date': sys.argv[3],
        'site_aa': site['aa'],
        'site_ga': site['ga'],
        'payload_aa': 'aa/request-events.json',
        'column_join': 'event',
        'columns': 'event,{{platform}}-count,{{platform}}-visits,{{platform}}-visitors',
        'columns_tools': 'event,{{platform}}-count-aa,{{platform}}-count-ga,{{platform}}-visits-aa,{{platform}}-visits-ga,{{platform}}-visitors-aa,{{platform}}-visitors-ga'
    }

    # Logging
    log = f_log.Logging()

    init()
    log.print('site', site['str'])

    adobe = Adobe()
    result['df_aa'] = adobe.get_adobe()
    google = Google()
    result['df_ga'] = google.get_google()

    result['df'] = merge_adobe_google(result['df_aa'], result['df_ga'])
    get_csv_by_platform(result['df'])
    log.print('================ END ================', '')


