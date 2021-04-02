#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 23:26:52 2021

@author: luis.salamo
"""

    # df.head(10)
    # df[:3]
    # df.tail(10)
    # df.shape
    # df["timestamp"].shape
    # df.columns
    # df.dtypes
    # type(df["timestamp"])
    # select columns
    # df["timestamp"]
    # df[["timestamp", "process_timestamp"]]
    # Convert the date to datetime64 
    # df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d')
    # How to manipulate textual data
    # df[df["process_timestamp"].dt.strftime('%Y-%m-%d') == '2021-03-16']
    # select rows
    # Filter data between two dates 
    # df.loc[(df['process_timestamp'] >= '2021-03-05') & (df['process_timestamp'] < '2021-03-06')] 
    # d = df.query("process_timestamp >= '2021-03-05' and process_timestamp < '2021-03-06'")
    # Filter data for specific weekday (tuesday) 
    # df.loc[df['process_timestamp'].dt.weekday == 2] 

 
    # print(all_objects)
    # creds = optimizely_s3_client.get_creds()
    # print('============')
    # print('export AWS_ACCESS_KEY_ID=' + creds['accessKeyId'])
    # print('export AWS_SECRET_ACCESS_KEY=' + creds['secretAccessKey'])
    # print('export AWS_SESSION_TOKEN=' + creds['sessionToken'])
    # print('============')