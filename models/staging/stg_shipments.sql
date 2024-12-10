{{ config(materialized = 'table', schema=env_var('DBT_STAGESCHEMA','STAGING_DEV')) }}

select
orderid,
lineno,
shipperid,
customerid,
productid,
employeeid,
split_part(shipmentdate,' ', 0)::date as shipmentdate,
status
from 
{{source('qwt_raw', 'shipments')}}