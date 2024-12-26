# AA > https://adobedocs.github.io/analytics-2.0-apis/

import pandas as pd
import sys
import os
import numpy as np

# adding libraries folder to the system path
sys.path.insert(0, '/Users/luis.salamo/Documents/github/python-training/libraries')

import functions as f
import api_adobe_analytics2_0 as f_api_adobe
import dataframe as f_df
import logs as f_log


# =============================================================================
# REQUEST ADOBE ANALYTICS
# =============================================================================
class App:
    def __init__(self):
        self.access_token = variables['access_token']
        self.columns = variables['columns']
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

    @staticmethod
    def get_adobe_report_suite():
        rs = [
            f_api_adobe.Adobe_API.rs_milanuncioscom,
            f_api_adobe.Adobe_API.rs_infojobsnet,
            f_api_adobe.Adobe_API.rs_infojobsit,
            f_api_adobe.Adobe_API.rs_infojobs_epreselec,
            f_api_adobe.Adobe_API.rs_cochesnet,
            f_api_adobe.Adobe_API.rs_motosnet,
            f_api_adobe.Adobe_API.rs_carfactory,
            f_api_adobe.Adobe_API.rs_fotocasaes,
            f_api_adobe.Adobe_API.rs_habitaclia,
            f_api_adobe.Adobe_API.rs_uniquetool
        ]
        log.print('get_adobe_report_suite', 'dataframe loaded')
        return rs

    def get_adobe(self):
        df = pd.DataFrame()
        for index in range(len(result['df_rs'])):
            rs = result['df_rs'][index]
            log.print('get_adobe', f'{index}::rsid::{rs}')
            # request
            api = f_api_adobe.Adobe_Dimensions_API(rs, self.access_token)
            df_request = api.request()
            if not f_df.Dataframe.is_empty(df_request):
                df = f_df.Dataframe.Rows.concat_two_frames(df, df_request)
        # transform
        df = pd.pivot_table(df, values=['name'], index=['evar'], columns=['rsid'], aggfunc=np.sum)
        df.columns = df.columns.droplevel(level=0)
        df = df[result['df_rs']]
        df.replace(0, np.nan, inplace=True)
        df.reset_index(inplace=True)
        # log
        log.print('get_adobe', 'dataframe loaded')
        return df


# main function
if __name__ == '__main__':
    result = {}
    variables = {
        'access_token': f_api_adobe.Adobe_JWT.get_access_token(),
        'columns': 'report_suite,month,page_views,page_events'
    }

    # Logging
    log = f_log.Logging()
    # app
    app = App()
    result['df_rs'] = app.get_adobe_report_suite()
    result['df'] = app.get_adobe()
    # export csvx
    f.CSV.dataframe_to_file(result['df'], 'df.csv')

