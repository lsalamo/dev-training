import pandas as pd
import os.path

# adding libraries folder to the system path
from libs import (
    api as f_api,
    files as f_files,
    dataframe as f_df,
)
from libs.google import api_google_authentication as google_authentication

class GoogleAnalyticsRest(f_api.API):
    def __init__(self, config):
        # Properties
        self.property_fotocasaes = '296810976'
        # self.property_motosnet = '273930537'
        self.property_motosnet = '468831764'
        self.property_cochesnet = '313836548'
        self.property_milanuncioscom = '330577361'
        self.property_infojobsnet = '330615843'
        self.property_infojobsit = '330589193'
        self.platform_web = 'web'
        self.platform_android = 'android'
        self.platform_ios = 'ios'
        properties = {
            'mnet': {'str': 'mnet', 'ga': self.property_motosnet},
            'cnet': {'str': 'cnet', 'ga': self.property_cochesnet},
            'ma': {'str': 'ma', 'ga': self.property_milanuncioscom},
            'ijes': {'str': 'ijes', 'ga': self.property_infojobsnet},
            'ijit': {'str': 'ijit', 'ga': self.property_infojobsit},
            'fc': {'str': 'fc', 'ga': self.property_fotocasaes}
        }  
        self.property = config['property']     
        self.property_id = properties[self.property]['ga']  
        self.platform = config['platform']
        self.app_version = config['app_version'] if 'app_version' in config else None
       
        # authentication
        file_creds = config['credentials']['path_oauth2_desktop']
        creds = google_authentication.GoogleAuthentication.oauth2(file_creds)
        self.token = creds.token
        
        # headers
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.token
        }        
    
    def run_report(self, file_payload:str):
        # url
        url = f'https://analyticsdata.googleapis.com/v1beta/properties/{self.property_id}:runReport'    
        # payload
        payload = f_files.File.read_file(file_payload)
        payload = payload.replace('{{dt_from}}', '2024-12-01')
        payload = payload.replace('{{dt_to}}', 'today')  
        payload = payload.replace('{{platform}}', self.platform) 

        df = pd.DataFrame()
        super().__init__('POST', url, self.headers, payload) 
        response = super().request()
        if 'rows' in response:
            rows = response['rows']
            if len(rows) > 0:
                for row in rows:
                    # dimension
                    dimension_values = ''
                    for dimension in row['dimensionValues']:
                        dimension_values += dimension.get('value') + ','
                    row['dimensionValues'] = dimension_values.rstrip(',')

                    # metrics
                    metrics_values = ''
                    for metric in row['metricValues']:
                        metrics_values += metric.get('value') + ','
                    metrics_values
                    row['metricValues'] = metrics_values.rstrip(',')
                df = pd.DataFrame.from_dict(rows)
                df = f_df.Dataframe.Columns.split_column_string_into_columns(df, 'dimensionValues', ',')
                df = f_df.Dataframe.Columns.split_column_string_into_columns(df, 'metricValues', ',')
                f_df.Dataframe.Columns.drop_to_index(df, 2, True)

                # columns
                columns = 'date,platform,version,' if self.app_version else 'date,platform,'
                columns += f'{self.platform}-visits,{self.platform}-visitors,{self.platform}-views'
                df.columns = columns.split(',')           

        return df           


