CREATE OR REPLACE TABLE
  `beatles-380723.tables_data.D_dataset_dealer` AS
SELECT
Dealer_ID,
sp_id,
sp_name,
seller_rating,
franchise_dealer,
franchise_make
FROM
  `beatles-380723.data_staging_dataset.used_cars_data`;