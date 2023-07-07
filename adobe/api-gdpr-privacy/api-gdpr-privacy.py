import pandas as pd
import sys
import os
import argparse
import libraries.functions as f
import libraries.api_adobe_privacy as api_adobe
import libraries.dt as f_dt
import libraries.logs as f_log


class App:
    def __init__(self):
        self.from_date = variables['from_date']
        self.to_date = variables['to_date']
        self.log = f_log.Logging()

        # args
        self.log.print('init', 'Total arguments passed: ' + str(len(sys.argv)))
        self.log.print('init', 'Name of Python script: ' + sys.argv[0])
        for i in range(1, len(sys.argv)):
            self.log.print('init', 'Argument: ' + sys.argv[i])

        # directory
        self.directory = os.path.dirname(os.path.realpath(__file__))
        f.Directory.set_working_directory(self.directory)
        f.Directory.create_directory('csv')
        self.log.print('init', f'working_directory: {f.Directory.get_working_directory()}')

    def request(self):
        try:
            api = api_adobe.Jobs(self.from_date, self.to_date)
            df = api.request()
            # clean
            df = df[df["userKey"].str.startswith("Analytics-") & df["userKey"].str.contains("sdrn:")]
            # transform
            df['realm'] = df["userKey"].str.extract('Analytics-(?:delete|access)-(sdrn:.*):user:', expand=False)
            df = df.groupby(["realm", "action", "status"], as_index=False).size()
            self.log.print(f'request', 'DONE!!')
            return df
        except Exception as e:
            self.log.print_error(str(e))


if __name__ == '__main__':
    # args
    parser = argparse.ArgumentParser()
    dt_default = f_dt.Datetime.get_current_datetime()
    parser.add_argument("-i", "--initdate", help="Initial Date (YY-MM-DD)", default=f_dt.Datetime.datetime_to_str(dt_default, '%Y-%m-01'))
    parser.add_argument("-e", "--enddate", help="End Date (YY-MM-DD)", default=f_dt.Datetime.datetime_to_str(dt_default, '%Y-%m-31'))
    args = parser.parse_args()

    result = {}
    variables = {
        'from_date': args.initdate,
        'to_date': args.enddate
    }
    # app
    app = App()
    # csv
    df_result = app.request()
    f.CSV.dataframe_to_file(df_result, 'df.csv')


# # =============================================================================
# #   ADD COLUMNS
# # =============================================================================#

# df_clean['createdDate_datetime'] = pd.to_datetime(pd.to_datetime(df_clean['createdDate']).dt.strftime('%Y-%m-%d %H:%M:%S'))
# df_clean['lastModifiedDate_datetime'] = pd.to_datetime(pd.to_datetime(df_clean['lastModifiedDate']).dt.strftime('%Y-%m-%d %H:%M:%S'))
# df_clean["createdDate_datetime_ymd"] = df_clean["createdDate_datetime"].dt.strftime('%Y-%m-%d')
# df_clean["diffDate"] = df_clean["lastModifiedDate_datetime"] - df_clean["createdDate_datetime"]

# # =============================================================================
# #   RESULT
# # =============================================================================

# # Summary by realm
# df_summary_by_realm = df_clean.groupby(["realm", "action", "status"]).agg(
#     count = pd.NamedAgg(column="jobId", aggfunc="count"),
#     min = pd.NamedAgg(column="diffDate", aggfunc="min"),
#     max = pd.NamedAgg(column="diffDate", aggfunc="max"),
#     mean = pd.NamedAgg(column="diffDate", aggfunc=lambda x: x.mean())
# )
# df_summary_by_realm['mean2'] = df_summary_by_realm["mean"] - timedelta(microseconds=data["mean"].microseconds)
# df_summary_by_realm.info()
# data.info()
#
# # Get first job to validate in postman
# df_grouping_first_row = df_clean.groupby(["realm", "action", "status"]).head(1).reset_index(drop=True)
#
# # Summary total
# def get_summary():
#     count = df_clean.shape[0]
#     count_delete = df_clean[df_clean["action"] == "delete"].shape[0]
#     count_access = df_clean[df_clean["action"] == "access"].shape[0]
#     date_from = datetime.strptime(FROM_DATE, '%Y-%m-%d').date()
#     date_to = datetime.strptime(TO_DATE, '%Y-%m-%d').date()
#     date_diff = (date_to - date_from).days + 1
#     data = {
#         'count': count,
#         'count_delete': count_delete,
#         'count_access': count_access,
#         '%_count_delete': round(count_delete / count * 100, 2),
#         '%_count_access': round(count_access / count * 100, 2),
#         'date_from': date_from,
#         'date_to': date_to,
#         'date_diff': (date_to - date_from).days + 1,
#         'count / day': round(count / date_diff),
#     }
#     df_delete_complete = df_clean[(df_clean["action"] == "delete") & (df_clean["status"] == "complete")]
#     if len(df_delete_complete) > 0:
#         data['time_min_delete'] = df_delete_complete["diffDate"].min().to_pytimedelta()
#         data['time_max_delete'] = df_delete_complete["diffDate"].max().to_pytimedelta()
#         data['time_mean_delete'] = df_delete_complete["diffDate"].mean().to_pytimedelta()
#         data["time_mean_delete"] = data["time_mean_delete"] - timedelta(microseconds=data["time_mean_delete"].microseconds)
#
#     df_access_complete = df_clean[(df_clean["action"] == "access") & (df_clean["status"] == "complete")]
#     if len(df_access_complete) > 0:
#         data['time_min_access'] = df_access_complete["diffDate"].min().to_pytimedelta()
#         data['time_max_access'] = df_access_complete["diffDate"].max().to_pytimedelta()
#         data['time_mean_access'] = df_access_complete["diffDate"].mean().to_pytimedelta()
#         data["time_mean_access"] = data["time_mean_access"] - timedelta(microseconds=data["time_mean_access"].microseconds)
#     return data
#
# result = {
#     'total_records': df_clean.shape[0],
#     'df': df,
#     'df_clean': df_clean,
#     # 'df_summary': get_summary(),
#     'df_summary_by_realm': df_summary_by_realm,
#     # 'df_grouping_first_row': df_grouping_first_row
# }


# # =============================================================================
# #   FILTER BY USERKEY
# # =============================================================================
#
# df_filter = result['df_clean'][result['df_clean']['userKey'].str.contains(
#     'sdrn:fotocasa.es:user:3625292|sdrn:coches.net:user:1773804'
# , regex=True) == True]
# df_filter.to_csv(dir + "/data_filter.csv")
#
# df_filter = result['df_clean'][result['df_clean']['userKey'].str.contains(
#     '13504170770'
# , regex=True) == True]
# df_filter.to_csv(dir + "/data_filter.csv")

# # =============================================================================
# #   FIRST ROW OF EACH GROUP > VERIFY RESULTS
# # =============================================================================

# df = pd.DataFrame();
# url = 'https://platform.adobe.io/data/core/privacy/jobs/{{jobId}}'
# for index, row in result['df_grouping_first_row'].iterrows():
#     response = requests.request("GET", url.replace("{{jobId}}", str(row['jobId'])), headers=headers, data=payload)
#     response = response.json()
#     df = df.append(pd.DataFrame.from_dict(response))
