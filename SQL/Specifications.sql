CREATE OR REPLACE TABLE
  `beatles-380723.tables_data.D_dataset_specifications` AS
SELECT
vin,
engine_cylinders,
engine_displacement,
fuel_tank_volume,
height,
horsepower,
width,
length,
major_options,
power,
torque,
transmission,
transmission_display,
wheel_system,
wheel_system_display,
wheelbase
FROM
  `beatles-380723.data_staging_dataset.used_cars_data`;