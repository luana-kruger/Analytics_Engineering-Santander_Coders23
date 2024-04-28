{{config(schema=generate_schema_name("silver"),alias="calendar",order=6)}}
with source_calendar as (
        select *  from {{ source('calendar_bronze', 'calendar') }}
    )

select
    cast(listing_id as bigint) as listing_id,
    date::date as date,
    case when available is null then 0 else case when available = 't' then 1 else 0 end end as available,
    case when price is null then '0.0' else regexp_replace(replace(trim(price), '$', ''), ',', '') end::float as price,
    cast(minimum_nights as int) as minimum_nights,
    cast(maximum_nights as int) as maximum_nights
from source_calendar