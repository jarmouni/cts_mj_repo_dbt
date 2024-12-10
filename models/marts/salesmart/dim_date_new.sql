{{ config(materialized = 'view', schema = 'salesmart') }}

{% set minmax_orderdate = get_minmax_orderdate() %} 

{{ dbt_date.get_date_dimension(minmax_orderdate[0], minmax_orderdate[1]) }}