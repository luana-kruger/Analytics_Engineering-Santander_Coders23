{{config(schema=generate_schema_name("gold"),alias="avg_per_period",order=9)}}

with calendar as (
    select *  from {{ source('silver', 'calendar') }}
)


SELECT
	periodo,
	avg(price) as media_de_preco
	from (
	SELECT 
		case
			when extract(month from date) between 1 and 3 then 'Jan - Mar'
			when extract(month from date) between 4 and 6 then 'Abr - Jul'
			when extract(month from date) between 7 and 9 then 'Ago - Out'
			when extract(month from date) between 10 and 12 then 'Nov - Dez'
		else
			'Não Identificado'
		end as periodo,
		listing_id,
		date,
		available, 
		price, 
		minimum_nights, 
		maximum_nights
	FROM silver.calendar) as calendar
group by 1