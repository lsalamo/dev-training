import pandas as pd
import sys
import os
import numpy as np
import logging
import api_adobe_analytics2_0 as f_api_adobe


def print_log(method, info):
    log.info("Method: {} - {}".format(method, info))


def init():
    # args
    print_log('init', 'Total arguments passed: ' + str(len(sys.argv)))
    print_log('init', 'Name of Python script:: ' + sys.argv[0])
    for i in range(1, len(sys.argv)):
        print_log('init', 'Argument: ' + sys.argv[i])
    # directory
    os.chdir('/Users/luis.salamo/Documents/github/python-training/adobe/api-license-digital')
    print_log('directory', os.getcwd())
    directory = os.path.join(os.getcwd(), 'csv')
    if not os.path.isdir(directory):
        os.makedirs(directory)


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================
class App:
    def __init__(self):
        self.payload = variables['payload']
        self.from_date = variables['from_date']
        self.to_date = variables['to_date']
        self.access_token = variables['access_token']
        self.columns = variables['columns']
        # args
        print_log('init', 'Total arguments passed: ' + str(len(sys.argv)))
        print_log('init', 'Name of Python script:: ' + sys.argv[0])
        for i in range(1, len(sys.argv)):
            print_log('init', 'Argument: ' + sys.argv[i])
        # directory
        os.chdir('/Users/luis.salamo/Documents/github/python-training/adobe/api-license-digital')
        print_log('directory', os.getcwd())
        directory = os.path.join(os.getcwd(), 'csv')
        if not os.path.isdir(directory):
            os.makedirs(directory)

    @staticmethod
    def dataframe_to_file(df, file):
        directory = os.path.join(os.getcwd(), 'csv/')
        if os.path.isdir(directory):
            file = os.path.join(directory, file)
            df.to_csv(file, index=False)

    def get_adobe_report_suite(self):
        # request
        api = f_api_adobe.Adobe_Report_Suite_API(self.access_token)
        df = api.request()
        df = df.loc[df['collectionItemType'] == 'reportsuite']
        print_log('get_adobe_report_suite', 'dataframe loaded')
        return df

    def get_adobe(self):
        df = pd.DataFrame()
        for index, row in result['df_rs'].iterrows():
            print_log('get_adobe', str(index) + '::rsid::' + row['rsid'])
            # request
            api = f_api_adobe.Adobe_Report_API(row['rsid'], self.payload, self.from_date, self.to_date, self.access_token)
            df_request = api.request()
            if not df_request.empty:
                df_request = df_request[['value', 0, 1]]
                # transform
                df_request.insert(loc=0, column='rs', value=row['rsid'])
                df_request.columns = self.columns.split(',')
                df_values = df_request[['month']].apply(pd.to_datetime)
                df_values = df_values.apply(lambda x: x.dt.strftime('%Y-%m'))
                df_request[df_values.columns] = df_values
                df_values = df_request.filter(regex='^(page_).*$', axis=1)
                df_values = df_values.astype(np.int64)
                df_request[df_values.columns] = df_values
                df_request.sort_values(by='month', ascending=True, inplace=True)
                frames = [df, df_request]
                df = pd.concat(frames, ignore_index=True)
        # log
        print_log('get_adobe', 'dataframe loaded <' + self.payload + '>')
        return df

    def get_adobe_by_site(self):
        df = result['df']
        df['site'] = 'others'
        df['total'] = df['page_views'] + df['page_events']
        df.loc[df['report_suite'].str.contains("fotocasa"), 'site'] = 'realestate'
        df.loc[df['report_suite'].str.contains("habitaclia"), 'site'] = 'realestate'
        df.loc[df['report_suite'].str.contains("inmofactory"), 'site'] = 'realestate'
        df.loc[df['report_suite'].str.contains("uniquetool"), 'site'] = 'realestate'
        df.loc[df['report_suite'].str.contains("jobs"), 'site'] = 'jobs'
        df.loc[df['report_suite'].str.contains("motor"), 'site'] = 'motor'
        df.loc[df['report_suite'].str.contains("milanuncios"), 'site'] = 'generalist'
        # transform
        df = df.groupby(['site', 'month'], as_index=False).sum()
        df = pd.pivot_table(df, values=['total'], index=['month'], columns=['site'], aggfunc=np.sum)
        df.columns = df.columns.droplevel(level=0)
        df = df.reset_index()
        df = df[['month', 'realestate', 'jobs', 'motor', 'generalist', 'others']]
        # log
        print_log('get_adobe_by_site', 'dataframe loaded')
        return df


# main function
if __name__ == '__main__':
    result = {}
    variables = {
        'from_date': sys.argv[1],
        'to_date': sys.argv[2],
        'payload': 'aa/request.json',
        'access_token': f_api_adobe.Adobe_JWT.get_access_token(),
        'columns': 'report_suite,month,page_views,page_events'
    }

    # Logging
    logging.basicConfig(level="INFO")
    log = logging.getLogger()
    # app
    app = App()
    result['df_rs'] = app.get_adobe_report_suite()
    result['df'] = app.get_adobe()
    result['df_by_site'] = app.get_adobe_by_site()

    # export csvx
    app.dataframe_to_file(result['df_by_site'], 'df.csv')

    print_log('================ END ================', '')
