import io
import pandas as pd
import requests
from io import BytesIO
import gzip
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    #url = ''
    #response = requests.get(url)

    #return pd.read_csv(io.StringIO(response.text), sep=',')

 

# Specify the URL and the months for the last quarter of 2020
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
    months = [10, 11, 12]
    taxi_dtypes = {

       'VendorID': pd.Int64Dtype(),

        'passenger_count': pd.Int64Dtype(),

       'trip_distance': float,

        'RatecodeID': pd.Int64Dtype(),

       'store_and_fwd_flag': str,

        'PULocationID': pd.Int64Dtype(),

        'DOLocationID': pd.Int64Dtype(),

        'payment_type': pd.Int64Dtype(),

        'fare_amount': float,

        'extra': float,

       'mta_tax': float,

       'tip_amount': float,

        'tolls_amount': float,

        'improvement_surcharge': float,

        'total_amount': float,

        'congestion_surcharge': float 

    }
    parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    
# Step 1: Fetch and read data for the last quarter of 2020
    dataframes = []
    for month in months:
        file_url = f'{url}green_tripdata_2020-{month:02d}.csv.gz'
        response = requests.get(file_url)
        
    # Check if the response is successful (status code 200)
        if response.status_code == 200:
            # Read the compressed CSV file directly into a Pandas DataFrame
            df=pd.read_csv(file_url, sep=",", compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
            #with gzip.GzipFile(fileobj=BytesIO(response.content), mode='rb') as f:
             #   df = pd.read_csv(f, dtype=taxi_dtypes, parse_dates=parse_dates)
            dataframes.append(df)
        else:
            print(f"Failed to fetch data for month {month}")

# Step 2: Concatenate DataFrames
    final_dataframe = pd.concat(dataframes, axis=0, ignore_index=True)
   
    return final_dataframe
# Now 'final_dataframe' contains the data for the last quarter of 2020


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
