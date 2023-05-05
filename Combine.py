from datetime import timedelta, datetime

from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
import pandas as pd
from datetime import datetime, timedelta
import pandas as pd

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "beatles-380723"
GS_PATH = "data/"
BUCKET_NAME = 'big_data_beatles'
LOCATION = "us-central1"

# define DAG parameters
default_args = {
    'owner': 'Vijen Mehta',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5)
}

# define DAG
with DAG('Combine', schedule_interval=timedelta(days=15), default_args=default_args) as dag:
    
    # define tasks
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    def load_data():
        start_row_loc = 1
        width_loc = 7
        prefix_loc = 'LOC'

        start_row_dea = 1
        width_dea = 7
        prefix_dea = 'DEA'

        start_row_lis = 1
        width_lis = 7
        prefix_lis = 'LIS'
        
        # Read used_cars_data.csv and drop unnecessary columns
        df1= pd.read_csv('gs://big_data_beatles/data/us_used_cars_data.csv')
        df1 = df1.drop(columns=['bed', 'bed_height','bed_length','combine_fuel_economy','engine_type','fleet','is_certified','is_cpo','is_oemcpo','main_picture_url','theft_title','trimId','vehicle_damage_category'])
        
        # Read vehicles.csv and drop unnecessary columns, rename columns, and format wheel_system_display
        df2= pd.read_csv('gs://big_data_beatles/data/vehicles.csv')
        df2 = df2.drop(columns=['id','url','region_url','condition','cylinders','odometer','title_status','size','county','state','image_url'])
        df2.rename(columns={'region': 'city', 'manufacturer': 'make_name','model': 'model_name','fuel': 'fuel_type','VIN': 'vin','drive':'wheel_system_display','type':'body_type','paint_color':'exterior_color','lat':'latitude','long':'longitude','posting_date':'listed_date'}, inplace=True)
        df2['wheel_system_display'] = df2['wheel_system_display'].str.upper()

        # Merge dataframes and create new columns Location_ID, Dealer_ID, and listing_id
        list = ["city", "price", "year", "make_name","model_name","fuel_type","transmission","vin","wheel_system_display","body_type","exterior_color","description","latitude","longitude","listed_date"]
        df_combine = df1.merge(df2, on=list, how='outer')
        df_combine = df_combine.dropna(axis=0, subset=['vin'])
        df_combine = df_combine[df_combine['vin'].str.contains('[A-Za-z]')]
        df_combine = df_combine.drop_duplicates(subset=['vin'])

        df_combine['Location_ID'] = prefix_loc + (df_combine.index + start_row_loc).map(str).str.zfill(width_loc)
        df_combine['Dealer_ID'] = prefix_dea + (df_combine.index + start_row_dea).map(str).str.zfill(width_dea)
        df_combine['listing_id'] = prefix_lis + (df_combine.index + start_row_lis).map(str).str.zfill(width_lis)

        df_combine.to_csv('gs://big_data_beatles/data/iused_car_data.csv', index=False)


    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    save_data_to_gcs = GCSToGCSOperator(
        task_id='save_data_to_gcs',
        source_bucket='<big_data_beatles/data/>',
        source_object=['us_used_car_data.csv','vehicles.csv'],
        destination_bucket='<big_data_beatles/data/>',
        destination_object='iused_car_data.csv',
        dag=dag
    )
    # set dependencies
    start_pipeline >> load_data_task >> save_data_to_gcs