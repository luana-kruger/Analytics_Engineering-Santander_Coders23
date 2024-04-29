{{config(schema=generate_schema_name("gold"),alias="avg_price_by_property_agg",order=8)}}

with listings_silver as (
    select * from {{ source('silver', 'listings') }}
)
,
calendar_silver as (
    select * from {{ source('silver', 'calendar') }}
)


SELECT listings.id as listing_id
	, AVG(coalesce(calendar.price,0))::float avg_price
FROM listings_silver listings
	LEFT JOIN calendar_silver calendar
		ON calendar.listing_id = listings.id

group by listings.id

