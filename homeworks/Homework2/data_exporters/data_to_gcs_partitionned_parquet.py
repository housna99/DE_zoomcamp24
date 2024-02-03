import pyarrow as pa 
import pyarrow.parquet as pq 
import os 
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/home/src/dtc-de-keys.json"
bucket_name = 'dtc-de-410519_bucket'
project_id = 'dtc-de-410519_bucket'
table_name="green_taxi"

root_path=f'{bucket_name}/{table_name}'
@data_exporter
def export_data(data, *args, **kwargs):
    """
    partitionning by dates is very useful !!
    """
    #data['tpep_pickup_date']=data['tpep_pickup_datetime'].dt.date

    table=pa.Table.from_pandas(data) #reading dataframe into pyarrow table
    gcs=pa.fs.GcsFileSystem() #define gcs object



    pq.write_to_dataset (
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'], #list!!
        filesystem=gcs
    )
    # Spec=pa.fsify your data exporting logic here


