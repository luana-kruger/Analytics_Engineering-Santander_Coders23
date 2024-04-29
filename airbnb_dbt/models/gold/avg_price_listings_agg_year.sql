{{config(schema=generate_schema_name("gold"),alias="avg_price_listings_agg_year",order=8)}}

with listings_silver as (
    select *  from {{ source('Silver', 'listings') }}
)
,
calendar_silver as (
    select *  from {{ source('Silver', 'calendar') }}
)

SELECT listings.id as listing_id
	, extract(year from calendar.date) AS year
	, AVG(coalesce(calendar.price,0)) avg_price
	, MIN(coalesce(calendar.price,0)) min_price
	, MAX(coalesce(calendar.price,0)) max_price

	FROM listings_silver listings
	LEFT JOIN calendar_silver calendar
		ON calendar.listing_id = listings.id

group by listings.id
	, extract(year from calendar.date)


