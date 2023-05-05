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


GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="beatles-380723"
GS_PATH = "data/"
BUCKET_NAME = 'big_data_beatles'
STAGING_DATASET = "data_staging_dataset"
DATASET = "tables_data"
LOCATION = "us-central1"

default_args = {
    'owner': 'Vijen Mehta',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

# check to see if this schedule_interval is correct 
with DAG('DataWarehouse', schedule_interval=timedelta(days=15), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )


    load_staging_dataset = DummyOperator(
        task_id = 'load_staging_dataset',
        dag = dag
        )    
    
    load_dataset_used_car = GCSToBigQueryOperator(
        task_id = 'load_dataset_used_car_dataset',
        bucket = BUCKET_NAME,
        source_objects = ['data/used_cars_data.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.used_cars_data',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'vin', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'back_legroom', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'body_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city_fuel_economy', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'daysonmarket', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'dealer_zip', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'engine_cylinders', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'engine_displacement', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'exterior_color', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'frame_damaged', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'franchise_dealer', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'franchise_make', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'front_legroom', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'fuel_tank_volume', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'fuel_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'has_accidents', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'height', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'highway_fuel_economy', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'horsepower', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'interior_color', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'isCab', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'is_new', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'latitude', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'length', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'listed_date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'listing_color', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'longitude', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'major_options', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'make_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'maximum_seating', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'mileage', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'model_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'owner_count', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'power', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'salvage', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'savings_amount', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'seller_rating', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sp_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sp_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'torque', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'transmission', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'transmission_display', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'trim_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'wheel_system', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'wheel_system_display', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'wheelbase', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'width', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'year', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Location_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Dealer_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'listing_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )

    check_dataset_used_car = BigQueryCheckOperator(
        task_id = 'check_used_cars_dataset',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.used_cars_data`'
        )
 

    create_D_Table = DummyOperator(
        task_id = 'Create_D_Table',
        dag = dag
        )

    create_D_dataset_dealer = BigQueryOperator(
        task_id = 'create_D_dataset_dealer',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/Dealer.sql'
        )

    create_D_dataset_listing = BigQueryOperator(
        task_id = 'create_D_dataset_listing',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/Listing.sql'
        )   

    create_D_dataset_specifications = BigQueryOperator(
        task_id = 'create_D_dataset_specifications',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/Specifications.sql'
        )

    create_D_dataset_location = BigQueryOperator(
        task_id = 'create_D_dataset_location',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/Location.sql'
        )

    create_F_Table = DummyOperator(
        task_id = 'Create_F_Table',
        dag = dag
        )

    create_F_dataset_vehicle = BigQueryOperator(
        task_id = 'create_F_dataset_vehicle',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/Vehicle.sql'
        )

    check_F_dataset_vehicle = BigQueryCheckOperator(
        task_id = 'check_F_dataset_vehicle',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.F_dataset_vehicle`'
        ) 

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 
    
start_pipeline >>  load_staging_dataset

load_staging_dataset >> [load_dataset_used_car]

load_dataset_used_car >> check_dataset_used_car


[check_dataset_used_car] >> create_D_Table

create_D_Table >> [create_D_dataset_dealer, create_D_dataset_listing, create_D_dataset_specifications, create_D_dataset_location]

[create_D_dataset_dealer, create_D_dataset_listing, create_D_dataset_specifications, create_D_dataset_location] >> create_F_dataset_vehicle

[create_F_dataset_vehicle] >> check_F_dataset_vehicle >> finish_pipeline