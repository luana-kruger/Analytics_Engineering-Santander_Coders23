{{config(schema=generate_schema_name("bronze"),alias="calendar",order=3)}}
with source_data as (
        select *  from {{ source('datalake_dbt', 'calendar') }}
    )

select
    cast(listing_id as bigint) as listing_id,
    date::date as date,
    available::text as available,
    price::text as price,
    cast(adjusted_price as double precision) as adjusted_price,
    cast(minimum_nights as double precision) as minimum_nights,
    cast(maximum_nights as double precision) as maximum_nights
from source_data