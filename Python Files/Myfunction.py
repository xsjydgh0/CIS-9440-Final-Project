#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 22:50:05 2021

@author: appleuser
"""
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# setup Google BigQuery credentials
key_path = '/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/baruch-cis-4511ac487eed.json'
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)


### 1a. extract COVID data
def Extract_Covid_Data():
    # SQL query to run in BigQuery to extract COVID data
    sql_query = """
                SELECT date, country_name, SUM(new_confirmed) as new_confirmed
FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
WHERE  new_confirmed IS NOT NULL
AND date BETWEEN '2020-01-01' AND '2021-04-19'
AND country_name IN (SELECT country_name FROM `bigquery-public-data.covid19_open_data.covid19_open_data` WHERE country_name LIKE 'United States' 
or country_name LIKE 'Canada' 
or country_name LIKE 'United States of America'
or country_name LIKE'United Kingdom' 
or country_name LIKE 'Ireland'
or country_name LIKE'Denmark' 
or country_name LIKE'Finland' 
or country_name LIKE'Norway' 
or country_name LIKE'Sweden' 
or country_name LIKE'Netherlands'
or country_name LIKE'France'
or country_name LIKE'Germany' 
or country_name LIKE'South Africa' 
or country_name LIKE'Brazil' 
or country_name LIKE'Argentina' 
or country_name LIKE'Chile'
or country_name LIKE'Colombia' 
or country_name LIKE'Mexico' 
or country_name LIKE'Australia' 
or country_name LIKE'New Zealand' 
or country_name LIKE'Japan' 
or country_name LIKE'India'
or country_name LIKE' South Korea') 
GROUP BY date, country_name
ORDER BY country_name , date
                """
    
    # store extracted data in new dataframe
    Covid_df = client.query(sql_query).to_dataframe()
    
    # validate that >0 stories have been extracted and return dataframe
    if len(Covid_df) > 0:
        print(len(Covid_df), "Covid-19 data is extracted")
        return Covid_df
    else:
        print("Extraction FAILED")

### 1b.Clean COVID data
def Clean_Covid_Data(df):
    # check if table exists in your database
    try:
        table_id = 'CIS_9440_project.covid_fact'
        table = client.get_table(table_id)
        print("Covid Fact table exists, number of rows: ", table.num_rows)
        print("now filtering for only new data")
        
    # if the table is not already in the database, clean all data
    except:
        print("Covid Fact table is not in database, cleaning all data")
        # drop rows with null values
        if df.isnull().sum()[4] > 0: #4 has to be changed!!!!
            df.dropna()
            print(df.isnull().sum()[4], "null values dropped")
        else:
            print("Covid data has no null values")
            
        # drop duplicate values
        if len(df[df.duplicated()]) > 0:
            df.duplicated(keep = 'first')
            print(len(df[df.duplicated()])/2.0, "duplicate values dropped")
        else:
            print("Covid data has no duplicate rows")
        
        print("Covid data cleaning successful")

        return df
### 1c. add region column
def Append_Region(df):
    df.insert(0, 'region_name', 0)
    covid_region = []
    L = 'Latin America'
    US = 'United States and Canada'
    EMA = 'Europe, Middle East, and Africa'
    AP = 'Asia-Pacific'
    for name in df['country_name']:
        if name == 'United States of America':
            covid_region.append(US)
        elif name == 'Canada':
            covid_region.append(US)
        elif name == 'United Kingdom' :
            covid_region.append(EMA)
        elif name == 'Ireland':
            covid_region.append(EMA)
        elif name =='Denmark':
            covid_region.append(EMA)
        elif name =='Finland':
            covid_region.append(EMA)
        elif name =='Norway':
            covid_region.append(EMA)
        elif name =='Sweden' :
            covid_region.append(EMA)
        elif name =='Netherlands':
            covid_region.append(EMA)
        elif name =='France':
            covid_region.append(EMA)
        elif name =='Germany' :  
            covid_region.append(EMA)
        elif name == 'South Africa':
            covid_region.append(EMA)
        elif name == 'Argentina':
            covid_region.append(L)
        elif name == 'Brazil':
            covid_region.append(L)
        elif name == 'Chile':
            covid_region.append(L)
        elif name =='Colombia' :
            covid_region.append(L)
        elif name =='Mexico' :
            covid_region.append(L)
        elif name == 'Australia':
            covid_region.append(AP)
        elif name == 'New Zealand' :
            covid_region.append(AP)
        elif name == 'Japan' :
            covid_region.append(AP)
        elif name == 'India':
            covid_region.append(AP)
        elif name == 'South Korea':
            covid_region.append(AP)
    
    df['region_name'] = covid_region
    
    
### 1d. Transform country into region
def Transform_Country_Region():
    sql_query = '''
    SELECT date, region_name, SUM(new_confirmed) as new_confirmed
FROM `baruch-cis.CIS_9440_project.covid_append_column` 
GROUP BY date, region_name
ORDER BY region_name, date
    '''
    transform_df = client.query(sql_query).to_dataframe()
    if len(transform_df) > 0:
        print(len(transform_df), "transformation data is extracted")
        return transform_df
    else:
        print("Transformation FAILED")


### 2a.extract rev_sub
def Extract_rev_sub_Data():
    # SQL query to run in BigQuery to extract revenue and subscribers related data
    sql_query = """
                select *
                from `baruch-cis.CIS_9440_project.sub_rev_2016_2020`
                order by region_name;
                """
    
    # store extracted data in new dataframe
    rev_sub_df = client.query(sql_query).to_dataframe()
    
    # validate that >0 stories have been extracted and return dataframe
    if len(rev_sub_df) > 0:
        print(len(rev_sub_df), "rev_sub extracted")
        return rev_sub_df
    else:
        print("rev_sub extraction FAILED")


### 2b. Clean
def Clean_Sub_Rev_Data(df):
    # check if table exists in your database
    try:
        table_id = 'CIS_9440_project.sub_rev_2016_2020'
        table = client.get_table(table_id)
        print("Sub_Rev Fact table exists, number of rows: ", table.num_rows)
        print("now filtering for only new data")
        
    # if the table is not already in the database, clean all data
    except:
        print("Sub_Rev Fact table is not in database, cleaning all data")
        # drop rows with null values
        if df.isnull().sum()[4] > 0: # 4 has to be changed
            df.dropna()
            print(df.isnull().sum()[4], "null values dropped")
        else:
            print("Sub_Rev data has no null values")
            
        # drop duplicate values
        if len(df[df.duplicated()]) > 0:
            df.duplicated(keep = 'first')
            print(len(df[df.duplicated()])/2.0, "duplicate values dropped")
        else:
            print("Sub_Rev data has no duplicate rows")
        
        print("Sub_Rev data cleaning successful")

        return df
###3a.extract trend data
def Extract_Trend_Data():
    # SQL query to run in BigQuery to extract trend data
    sql_query = """
               SELECT Date, region, avg(netflix_Trend) as popularity
FROM `baruch-cis.CIS_9440_project.trend_5years` 
GROUP BY Date, region
ORDER BY region, Date;

                """
    
    # store extracted data in new dataframe
    Google_trend_df = client.query(sql_query).to_dataframe()
    
    # validate that >0 stories have been extracted and return dataframe
    if len(Google_trend_df) > 0:
        print(len(Google_trend_df), "trend extracted")
        return Google_trend_df
    else:
        print("trend extraction FAILED")



###3b.#clean trend data
        
def Clean_Trend_Data(df):
    # check if table exists in your database
    try:
        table_id = 'CIS_9440_Project.trend_fact'
        table = client.get_table(table_id)
        print("trend Fact table exists, number of rows: ", table.num_rows)
        print("now filtering for only new data")
        
    # if the table is not already in the database, clean all data
    except:
        print("trend Fact table is not in database, cleaning all data")
        # drop rows with null values
        if df.isnull().sum()[2] > 0:
            df.dropna()
            print(df.isnull().sum()[2], "null values dropped")
        else:
            print("trend data has no null values")
            
        # drop duplicate values
        if len(df[df.duplicated()]) > 0:
            df.duplicated(keep = 'first')
            print(len(df[df.duplicated()])/2.0, "duplicate values dropped")
        else:
            print("trend data has no duplicate rows")
        
        print("trend data cleaning successful")

        return df


### 4 Extract/Create stock fact
def Extact_Stock_df(stock, start, end):
    
    # get the data from yahoo finance
    df = yf.download(stock,
                     start=start,
                     end=end,
                     progress=False)
    # add extra columns for day, stock title,
    # simple moving average, and closing price average difference
    df['day'] = range(1, len(df) + 1)
    df['stock'] = stock
    df = df.reset_index()
    return df

def Create_Stock_Fact(df)
    df= pd.DataFrame(data=df)
    df['date_id'] = df['Date'].apply(lambda x: x.strftime("%Y%m%d"))
    df2 = df[["date_id", "Adj Close"]]

        
### 5.create date dimension
def create_date_dimension():
    sql_query = """
                SELECT
                  CONCAT (FORMAT_DATE("%Y",d),FORMAT_DATE("%m",d),FORMAT_DATE("%d",d)) as date_id,
                  d AS full_date,
                  EXTRACT(YEAR FROM d) AS year,
                  EXTRACT(WEEK FROM d) AS year_week,
                  EXTRACT(DAY FROM d) AS year_day,
                  EXTRACT(YEAR FROM d) AS fiscal_year,
                  FORMAT_DATE('%Q', d) as fiscal_qtr,
                  EXTRACT(MONTH FROM d) AS month,
                  FORMAT_DATE('%B', d) as month_name,
                  FORMAT_DATE('%w', d) AS week_day,
                  FORMAT_DATE('%A', d) AS day_name,
                FROM (
                  SELECT
                    *
                  FROM
                    UNNEST(GENERATE_DATE_ARRAY('2016-01-01', '2021-05-01', INTERVAL 1 DAY)) AS d )
                """
    
    # store extracted data in new dataframe
    date_df = client.query(sql_query).to_dataframe()
    
    # validate that >0 stories have been extracted and return dataframe
    if len(date_df) > 0:
        print("date dimension created")
        return date_df
    else:
        print("date dimension FAILED")
        


### 6. Create_region_dimension
def create_region_dimension(covid_df):

    unique_region = covid_df['region_name'].unique().tolist()
    # create blank list of dimension rows
    dimension_rows = []
    
    # create author dimension with a surrogate key
    for region_id, region_name in enumerate(unique_region, start = 100):
        temp_list = [region_id, region_name]
        dimension_rows.append(temp_list)
    
    region_dim = pd.DataFrame(data=dimension_rows,
                             columns = ['region_id', 'region_name'])

    
    print("region dimension create")
    print(dimension_rows)
    return region_dim 

### 7. Create COVID_fact
def Create_Covid_Fact(df, region_dim):
    #create date_id
    df['date_id'] = df['date'].apply(lambda x: x.strftime("%Y%m%d"))
    
# create region_id column #left -> df's column #right->region_dim's column, inner join
    df = df.merge(region_dim, left_on='region_name' , right_on='region_name',
                  how='inner')

    # drop unneeded columns
    for c in ['date','region_name']:
        df.drop(c, axis = 1, inplace=True)
    
    return df

### 8. Create rev Fact
def Create_NFLX_rev_sub_Fact(df, region_dim):
    # create date_id column
    
    new_dateform=[] #transform string to dateform
    for c in df['date']:
        new_dateform.append(c)

    df['date'] = new_dateform 
    print(df['date'])
    df['date_id'] = df['date'].apply(lambda x: x.strftime("%Y%m%d"))
    
    # create region_id column #left -> df的欄位 #right->region_dim的欄位 合併！
    df = df.merge(region_dim, left_on='region_name' , right_on='region_name',
                  how='inner')

    # drop unneeded columns
    for c in ['fiscal_year', 'fiscal_qtr','date','region_name']:
        df.drop(c, axis = 1, inplace=True)
    
    return df

### 9. Create stock fact
def Create_Stock_Fact(df):
    # create date_id column
    
    new_dateform=[] #transform string to dateform
    for c in df['date']:
        new_dateform.append(c)

    df['date'] = new_dateform 
    print(df['date'])
    df['date_id'] = df['date'].apply(lambda x: x.strftime("%Y%m%d"))

    # drop unneeded columns
    for c in ['date']:
        df.drop(c, axis = 1, inplace=True)
    
    return df


###10.create trend fact
def Create_trend_Fact(df, region_dim):
    # create date_id column
    
    new_dateform=[] #transform string to dateform
    for c in df['Date']:
        new_dateform.append(c)

    df['Date'] = new_dateform 
    print(df['Date'])
    df['date_id'] = df['Date'].apply(lambda x: x.strftime("%Y%m%d"))
    
    # create region_id column #left -> df's column #right->region_dim's column！
    df = df.merge(region_dim, left_on='region' , right_on='region_name',
                  how='inner')

    # drop unneeded columns
    for c in ['Date','region']:
        df.drop(c, axis = 1, inplace=True)
    
    return df

#666.Load dim/fact to bigquery
def load_df_to_bigquery(df, table_name):
    
    dataset_id =  'baruch-cis.CIS_9440_project' 
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"
    
    upload_table_name = 'CIS_9440_project.'+str(table_name)
    
    load_job = client.load_table_from_dataframe(df, upload_table_name,
                                                job_config=job_config)
    
    print("Starting job {}".format(load_job))

'''
#Export File
date_dim.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\date_dim.xlsx', index = False)
covid_fact.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\covid_fact.xlsx', index = False)
region_dim.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\region_dim.xlsx', index = False)
rev_sub_fact.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\rev_sub_fact.xlsx', index = False)
stock_fact.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\stock_fact.xlsx', index = False)
trend_fact.to_excel(r'/Users/appleuser/Desktop/W4 Data Warehousing and Analytics9440/Final Project/milestone3-ETL\trend_fact.xlsx', index = False)
'''


