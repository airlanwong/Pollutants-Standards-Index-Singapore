
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

select
MD5(NVL(cast("readings.pm25_one_hourly.west" as varchar),'') || '-' ||
NVL(cast("readings.pm25_one_hourly.east" as varchar),'') || '-' ||
NVL(cast("readings.pm25_one_hourly.central" as varchar),'') || '-' ||
NVL(cast("readings.pm25_one_hourly.south" as varchar),'') || '-' ||
NVL(cast("readings.pm25_one_hourly.north" as varchar),'')) as fact_guid,
"readings.pm25_one_hourly.west" as pm_west,
"readings.pm25_one_hourly.east" as pm_east,
"readings.pm25_one_hourly.central" as pm_central,
"readings.pm25_one_hourly.south" as pm_south,
"readings.pm25_one_hourly.north" as pm_north,
cast(timestamp as timestamptz) as created_timestamp,
cast(update_timestamp as timestamptz) as update_timestamp
from
spectrum_schema_2.psi_table_5
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
