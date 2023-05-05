CREATE OR REPLACE TABLE
  `beatles-380723.tables_data.F_dataset_vehicle` AS
SELECT
vin,
Location_ID,
Dealer_ID,
listing_ID,
back_legroom,
body_type,
city_fuel_economy,
exterior_color,
frame_damaged,
front_legroom,
fuel_type,
has_accidents,
highway_fuel_economy,
interior_color,
isCab,
is_new,
make_name,
maximum_seating,
mileage,
model_name,
trim_name
FROM
  `beatles-380723.data_staging_dataset.used_cars_data`;
