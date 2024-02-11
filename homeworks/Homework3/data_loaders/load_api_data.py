import io
import pandas as pd
import requests
import pyarrow.parquet as pq
import pyarrow.fs

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    months = [1,2,3,4,5,6,7,8,9,10, 11, 12]
    
    

    dataframes = []
    for month in months:
        file_url = f'{url}green_tripdata_2022-{month:02d}.parquet'

        # Download the Parquet file locally
        response = requests.get(file_url)
        if response.status_code == 200:
            # Read the compressed CSV file directly into a Pandas DataFrame
            df = pd.read_parquet(file_url, engine='pyarrow')
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime).dt.date
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime).dt.date
            dataframes.append(df)
        else:
            print(f"Failed to fetch data for month {month}")
        

# Step 2: Concatenate DataFrames
    final_dataframe = pd.concat(dataframes, axis=0, ignore_index=True)
   
    return final_dataframe
# Now 'final_dataframe' contains the data for the last quarter of 2020