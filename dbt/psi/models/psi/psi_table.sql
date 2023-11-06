
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

select
MD5(NVL(cast("PM25_WEST" as varchar),'') || '-' ||
NVL(cast("PM25_EAST" as varchar),'') || '-' ||
NVL(cast("PM25_NORTH" as varchar),'') || '-' ||
NVL(cast("PM25_SOUTH" as varchar),'') || '-' ||
NVL(cast("PM25_CENTRAL" as varchar),'')) as fact_guid,
"PM25_WEST" as PM25_WEST,
"PM25_EAST" as PM25_EAST,
"PM25_NORTH" as PM25_NORTH,
"PM25_SOUTH" as PM25_SOUTH,
"PM25_CENTRAL" as PM25_CENTRAL,
TIMESTAMP,
UPDATE_TIMESTAMP
from
psi_table
)


select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
