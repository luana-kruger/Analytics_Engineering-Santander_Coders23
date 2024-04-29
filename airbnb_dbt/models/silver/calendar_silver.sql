-- models/silver_calendar.sql

{{config(schema=generate_schema_name("silver"),alias="calendar",order=3)}}

WITH source_bronze AS (
    SELECT *
    FROM {{ source('bronze', 'calendar') }}
),

clean_price AS (
    SELECT
        listing_id,
        date::date AS date,
        available,
        -- Limpeza de caracteres especiais da coluna `price` e conversão para float.
        CAST(REGEXP_REPLACE(price, '[$,]', '', 'g') AS FLOAT) AS price,
        -- Coluna 'adjusted_price' pode precisar de limpeza ou cálculo semelhante
        adjusted_price,
        COALESCE(minimum_nights, (SELECT MIN(minimum_nights) FROM source_bronze)) AS minimum_nights,
        COALESCE(maximum_nights, (SELECT MAX(maximum_nights) FROM source_bronze)) AS maximum_nights
    FROM source_bronze
),

bool_conversion AS (
    SELECT
        listing_id,
        date,
        -- Conversão de tipos booleanos (assumindo que 'available' é 't' ou 'f')
        CASE WHEN available = 't' THEN 1 ELSE 0 END AS available_numeric,
        price,
        adjusted_price,
        minimum_nights,
        maximum_nights
    FROM clean_price
)

SELECT 
    listing_id,
    date,
    available_numeric AS available,
    price,
    -- Aqui assumimos que a coluna 'adjusted_price' precisa apenas de limpeza.
    -- Se ela precisar de cálculos adicionais, isso precisaria ser incluído.
    minimum_nights,
    maximum_nights
FROM bool_conversion
