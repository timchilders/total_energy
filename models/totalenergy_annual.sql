{{ config(materialized="view") }}

select * from {{ source('staging','totalenergy_monthly')}}
limit 100