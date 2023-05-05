CREATE OR REPLACE TABLE
  `beatles-380723.tables_data.D_dataset_location` AS
SELECT
Location_ID,
city,
dealer_zip,
latitude,
longitude
FROM
  `beatles-380723.data_staging_dataset.used_cars_data`;