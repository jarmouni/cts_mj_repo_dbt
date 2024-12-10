{{ config(materialized = 'view', schema = 'reporting') }}

{% set lineos = get_linenos() %}

select
orderid,
{% for lno in lineos %}
sum(case when lineno = {{lno}} then LINESALEAMOUNT end) as lineno{{lno}}_sales,
{% endfor %}
sum(LINESALEAMOUNT) as total_sales
from {{ ref('fct_orders') }}
group by 1
