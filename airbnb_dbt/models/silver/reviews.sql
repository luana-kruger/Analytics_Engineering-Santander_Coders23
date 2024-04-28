-- models/silver_reviews.sql

{{ config(schema="silver",alias="reviews",order=2) }}

WITH source_bronze AS (
    SELECT *
    FROM {{ source('bronze', 'reviews') }}
),

clean_comments AS (
    SELECT
        *,
        -- Usando REGEXP_REPLACE para limpar caracteres especiais dos comentários
        REGEXP_REPLACE(COALESCE(comments, 'no_info'), '[^\w\s]+', '', 'g') AS comments_clean
    FROM source_bronze
),

type_conversion AS (
    SELECT
        *,
        date::date AS dates,
        comments_clean AS comment
    FROM clean_comments
),

final_reviews AS (
    SELECT
        id,
        listing_id,
        reviewer_id,
        dates,
        comment
    FROM type_conversion
)

SELECT * FROM final_reviews
