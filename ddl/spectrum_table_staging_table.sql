create external table spectrum_schema.staging_psi_table (
timestamp VARCHAR(100),
update_timestamp VARCHAR(100),
"readings.pm25_one_hourly.west" bigint,
"readings.pm25_one_hourly.east" bigint,
"readings.pm25_one_hourly.central" bigint,
"readings.pm25_one_hourly.south" bigint,
"readings.pm25_one_hourly.north" bigint
)
stored as PARQUET 
location 's3://psi-sg/';