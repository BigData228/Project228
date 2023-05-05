CREATE OR REPLACE TABLE
  `beatles-380723.tables_data.D_dataset_listing` AS
SELECT
listing_id,
listed_date,
listing_color,
daysonmarket,
description,
owner_count,
price,
year
FROM
  `beatles-380723.data_staging_dataset.used_cars_data`;