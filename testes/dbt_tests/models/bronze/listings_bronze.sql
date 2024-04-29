{{config(schema=generate_schema_name("bronze"),alias="listings",order=1)}}

with source_data as (
        select *  from {{ source('datalake_dbt', 'listings') }}
    )


  select * from source_data