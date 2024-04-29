{{config(schema=generate_schema_name("silver"),alias="listings",order=4)}}

with bronze_data as (
    select
        *
        --tratando a coluna bathrooms_text, fazendo replaces
        , regexp_replace(regexp_replace( lower(trim(bathrooms_text)), 'baths', 'bath'), 'half-bath', '0.5 bath') as bathrooms_text_lower
    from {{ source('bronze', 'listings') }}
)
select id,
    listing_url,
    scrape_id,
    last_scraped::date as last_scraped,
    source,
    case when neighborhood_overview is null then 'nao informado' else regexp_replace(neighborhood_overview, E'\\s+', ' ', 'g') end as neighborhood_overview,
    picture_url,
    host_id,
    host_url,
    case when host_name is null then 'nao informado' else host_name end as host_name,
    coalesce(to_date(host_since, 'YYYY-MM-DD'), to_date('1990-01-01', 'YYYY-MM-DD')) as host_since,
    case when host_location is null then 'nao informado' else host_location end as host_location,
    case when host_about is null then 'nao informado' else regexp_replace(host_about, E'\\s+|\\n|\\t', ' ', 'g') end as host_about,
    case when host_response_time is null then 'nao informado' else host_response_time end as host_response_time,
    regexp_replace(case when host_response_rate is null then '0%' else host_response_rate end, '%', '')::float  as host_response_rate,
    regexp_replace(case when host_acceptance_rate is null then '0%' else host_acceptance_rate end, '%', '')::float  as host_acceptance_rate,
    case when host_is_superhost is null
        then 0
        else
            case when host_is_superhost = 't'
                then 1
                else 0
            end
    end as host_is_superhost,
    case when host_thumbnail_url is null then 'nao informado' else host_thumbnail_url end as host_thumbnail_url,
    case when host_picture_url is null then 'nao informado' else host_picture_url end as host_picture_url,
    case when host_neighbourhood is null then 'nao informado' else regexp_replace(host_neighbourhood, E'\\s+', ' ', 'g') end as host_neighbourhood,
    coalesce(cast(host_listings_count as float), 0.0) as host_listings_count,
    coalesce(cast(host_total_listings_count as float), 0.0) as host_total_listings_count,
    case when host_verifications is null or host_verifications = '[]' then '[nao informado]' else host_verifications end as host_verifications,
    case when host_has_profile_pic is null then 0 else case when host_has_profile_pic = 't' then 1 else 0 end end as host_has_profile_pic,
    case when host_identity_verified is null then 0 else case when host_identity_verified = 't' then 1 else 0 end end as host_identity_verified,
    COALESCE(neighbourhood,'Rio de Janeiro, Brazil') as neighbourhood,
    trim(neighbourhood_cleansed) as neighbourhood_cleansed,
    latitude,
    longitude,
    property_type,
    room_type,
    accommodates,
    --case
        --when 1=1 --regexp_matches(bathrooms_text_lower, '\\d*\\.?\\d+')[1] is not null
        --    then 1.0--trim(regexp_matches(bathrooms_text_lower, '\\d*\\.?\\d+')[1])
       -- else 0.0
    --end::float as bathroom_quantity,
    case when substring(bathrooms_text_lower from '\\d*\\.?\\d+') <> ''
        then substring(bathrooms_text_lower from '\\d*\\.?\\d+')
        else '0.0'
    end::float as bathroom_quantity,
    case
        when 1=1 --regexp_matches(bathrooms_text_lower, '\\d*\\.?\\d+')[1] is not null
            then
                -- tentando padronizar os valores, alguns registro não possuem a palavras 'bath' o que acaba criando variações. Como 'private' e 'private bath'.
                -- por isso faço a verificação se a palavra bath existe, se não existir insiro ela no final
                -- se ja existir, eu removo ela e depois insiro no final, pra padronizar os registros.
                --trim( trim(regexp_replace(regexp_replace(bathrooms_text_lower, regexp_matches(bathrooms_text_lower, '\\d*\\.?\\d+')[1], ''), 'bath',''))  || ' bath')
                'teste'
        else 'bath'
    end as bathroom_type,
    case when beds is null then 0.0 else cast(beds as float) end as beds,
    case when price is null then '0.0' else regexp_replace(replace(trim(price), '$', ''), ',', '') end::float as price,
    minimum_nights,
    maximum_nights,
    minimum_minimum_nights,
    maximum_minimum_nights,
    minimum_maximum_nights,
    maximum_maximum_nights,
    minimum_nights_avg_ntm,
    maximum_nights_avg_ntm,
    case when has_availability is null then 0 else case when has_availability = 't' then 1 else 0 end end as has_availability,
    case when availability_30 is null then 0 else availability_30 end::int as availability_30,
    case when availability_60 is null then 0 else availability_60 end::int as availability_60,
    case when availability_90 is null then 0 else availability_90 end::int as availability_90,
    case when availability_365 is null then 0 else availability_365 end::int as availability_365,
    coalesce(to_date(calendar_last_scraped, 'YYYY-MM-DD'), to_date('1990-01-01', 'YYYY-MM-DD')) as calendar_last_scraped,
    number_of_reviews,
    number_of_reviews_ltm,
    number_of_reviews_l30d,
    coalesce(to_date(first_review, 'YYYY-MM-DD'), to_date('1990-01-01', 'YYYY-MM-DD')) as first_review,
    coalesce(to_date(last_review, 'YYYY-MM-DD'), to_date('1990-01-01', 'YYYY-MM-DD')) as last_review,
    case when review_scores_rating is null then 0.0 else cast(review_scores_rating as float) end as review_scores_rating,
    case when review_scores_accuracy is null then 0.0 else cast(review_scores_accuracy as float) end as review_scores_accuracy,
    case when review_scores_cleanliness is null then 0.0 else cast(review_scores_cleanliness as float) end as review_scores_cleanliness,
    case when review_scores_checkin is null then 0.0 else cast(review_scores_checkin as float) end as review_scores_checkin,
    case when review_scores_communication is null then 0.0 else cast(review_scores_communication as float) end as review_scores_communication,
    case when review_scores_location is null then 0.0 else cast(review_scores_location as float) end as review_scores_location,
    case when review_scores_value is null then 0.0 else cast(review_scores_value as float) end as review_scores_value,
    case when instant_bookable is null then 0 else case when instant_bookable = 't' then 1 else 0 end end as instant_bookable,
    case when calculated_host_listings_count is null then 0 else calculated_host_listings_count end as calculated_host_listings_count,
    case when calculated_host_listings_count_entire_homes is null then 0 else calculated_host_listings_count_entire_homes end as calculated_host_listings_count_entire_homes,
    case when calculated_host_listings_count_private_rooms is null then 0 else calculated_host_listings_count_private_rooms end as calculated_host_listings_count_private_rooms,
    case when calculated_host_listings_count_shared_rooms is null then 0 else calculated_host_listings_count_shared_rooms end as calculated_host_listings_count_shared_rooms,
    case when reviews_per_month is null then 0.0 else cast(reviews_per_month as float) end::float as reviews_per_month
from bronze_data



