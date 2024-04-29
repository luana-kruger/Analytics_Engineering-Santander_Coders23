{{config(schema=generate_schema_name("gold"),alias="number_of_reviews_agg_year_month_gold",order=7)}}

with listings_silver as (
    select *  from {{ source('silver', 'listings') }}
)
,
reviews_silver as (
    select *  from {{ source('silver', 'reviews') }}
)


SELECT listings_silver.id as listing_id
	, price
	, number_of_reviews as number_of_reviews_total
	, number_of_reviews_l30d
	, COUNT(reviews_silver.listing_id) number_of_reviews_month
	, to_char(reviews_silver.dates,'YYYY-MM' )  year_month
	, extract(month from reviews_silver.dates) as month
	, extract(year from reviews_silver.dates) as year
	, date_trunc('month', reviews_silver.dates) + interval '1 month' - interval '1 day' as last_day_of_month
	FROM listings_silver
	INNER JOIN reviews_silver
		ON reviews_silver.listing_id = listings_silver.id
	GROUP BY listings_silver.id
	, price
	, number_of_reviews
	, number_of_reviews_l30d
	, to_char(reviews_silver.dates,'YYYY-MM' )
	, extract(month from reviews_silver.dates)
	, extract(year from reviews_silver.dates)
	, date_trunc('month', reviews_silver.dates) + interval '1 month' - interval '1 day'