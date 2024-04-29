{{config(schema=generate_schema_name("silver"),alias="reviews",order=5)}}

with source_reviews as (
        select *  from {{ source('bronze', 'reviews') }}
    )

select
    cast(listing_id as bigint) as listing_id,
    cast(id as bigint) as id,
    date::date as date,
    cast(reviewer_id as bigint) as reviewer_id,
    reviewer_name::text as reviewer_name,
    comments::text as comments
from source_reviews