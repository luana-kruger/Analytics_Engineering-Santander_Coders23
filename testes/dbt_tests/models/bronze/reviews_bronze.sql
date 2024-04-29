{{config(schema=generate_schema_name("bronze"),alias="reviews",order=2)}}

with source_data as (
        select *  from {{ source('datalake_dbt', 'reviews') }}
    )

select
    cast(listing_id as bigint) as listing_id,
    cast(id as bigint) as id,
    date::date as date,
    cast(reviewer_id as bigint) as reviewer_id,
    reviewer_name::text as reviewer_name,
    comments::text as comments
from source_data