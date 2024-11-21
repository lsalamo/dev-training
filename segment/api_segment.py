#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  1 22:54:38 2021

@author: luis.salamo
"""

import requests
import pandas as pd

tracking_plan_name = 'Fotocasa'  
output_path = '/folder_path/' 

url_base = "https://platform.segmentapis.com/v1beta/"
token = 'XXXXXXXX'
payload = {}
headers = {
  'Authorization': 'Bearer ' + token, 
  'Content-Type': 'application/json'
}

# List Tracking Plans
url = url_base + 'workspaces/adevinta-spain/tracking-plans/'
response = requests.request("GET", url, headers=headers, data=payload)
response = response.json()['tracking_plans']
list_tracking_plans_df = pd.DataFrame.from_dict(response)

# Get Tracking Plan
name = "Fotocasa"
tracking_plan = list_tracking_plans_df[list_tracking_plans_df.display_name == name]
if len(tracking_plan) == 1:
    url = url_base + tracking_plan['name'].values[0]
    response = requests.request("GET", url, headers=headers, data=payload)
    response = response.json()
    tracking_plan_df = pd.DataFrame.from_dict(response['rules']['events'])

