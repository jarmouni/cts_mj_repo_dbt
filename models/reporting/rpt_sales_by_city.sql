{{ config(materialized='view', schema = 'reporting')}}

select c.customerid, c.contactname, c.city,
        sum(o.linesaleamount) sales,
        sum(o.quantity) quantity,
        avg(o.margin) margin
from {{ref('fct_orders')}} o
left join {{ref('dim_customers')}} c
on o.customerid = c.customerid
where c.city = {{var('vcity', "'London'")}}
group by c.customerid, c.contactname, c.city
order by sales desc