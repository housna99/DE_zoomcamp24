#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[3]:


data_zone=pd.read_csv('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv')
data_zone.head()


# In[41]:


df=pd.read_csv('green_tripdata_2019-09.csv')
df.head()


# In[5]:


df.describe()


# In[6]:


df.columns


# In[42]:


print(pd.io.sql.get_schema(df, name="taxi_data"))


# # INGEST DATA

# In[ ]:


import argparse
import pandas as pd
from sqlalchemy import create_engine, inspect
from time import time
import requests
from io import BytesIO
import gzip


# In[ ]:


# Example usage


# In[ ]:


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    #url = params.url
    
    
    # Read CSV from content
    df_iter = pd.read_csv('green_tripdata_2019-09.csv', iterator=True, chunksize=100000)

    # Initialize database connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')        
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    # Check if the table exists

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Append data to the table
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    #parser.add_argument('--url', required=True, help='url of the csv file')
    args = parser.parse_args()

    main(args)

