#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 13:15:05 2021

@author: appleuser
"""

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import Myfunction as dsf
from datetime import datetime
import yfinance as yf
# setup Google BigQuery credentials
key_path = '/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/baruch-cis-4511ac487eed.json'
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)


####1.COVID related
#download covid dataset from bigquery
covid_raw = dsf.Extract_Covid_Data();

#add new column
dsf.Append_Region(covid_raw)
#upload the dataset to my bigquery, making it easier to manipulate
dsf.load_df_to_bigquery(covid_raw, 'covid_append_column')
#Using SQL to transform the data and downloading the dataset
covid_transformed = dsf.Transform_Country_Region()

####2. extract sub_rev 
rev_sub = dsf.Extract_rev_sub_Data()

###3. extract trend data
trend_region = dsf.Extract_Trend_Data()


###4. extract stock data
stock = dsf.Extract_Stock_df(NFLX,start = '2016-01-01',
                              end  = '2021-04-23')

###5. create date_dim
date_dim = dsf.create_date_dimension()


###6. Create_region_dimension
region_dim = dsf.create_region_dimension(covid_transformed)

###7. Create covid fact
covid_fact = dsf.Create_Covid_Fact(covid_transformed, region_dim)

###8.Create rev_sub fact
rev_sub_fact = dsf.Create_NFLX_rev_sub_Fact(rev_sub, region_dim)

###9. Create stock fact
stock_fact = dsf.Create_Stock_Fact(stock)

###10.create trend fact
trend_fact = dsf.Create_trend_Fact(trend_region, region_dim)

###11.load fact/dim to bigquery
dsf.load_df_to_bigquery(df= covid_fact, table_name= 'covid_fact')
dsf.load_df_to_bigquery(df= rev_sub_fact, table_name= 'rev_sub_fact')

dsf.load_df_to_bigquery(df= stock_fact, table_name= 'stock_fact')

dsf.load_df_to_bigquery(df= trend_fact, table_name= 'trend_fact')
dsf.load_df_to_bigquery(df= date_dim, table_name= 'date_dim')
dsf.load_df_to_bigquery(df= region_dim, table_name= 'region_dim')
'''
#Export File
date_dim.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\date_dim.xlsx', index = False)
covid_fact.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\covid_fact.xlsx', index = False)
region_dim.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\region_dim.xlsx', index = False)
rev_sub_fact.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\rev_sub_fact.xlsx', index = False)
trend_fact.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\trend_fact.xlsx', index = False)
'''


        
