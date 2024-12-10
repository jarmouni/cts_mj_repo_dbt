{{ config(  materialized = 'table', 
            schema = env_var('DBT_STAGESCHEMA','STAGING_DEV'),
            transient = false) }}

select *
from {{ source('qwt_raw', 'products')}}