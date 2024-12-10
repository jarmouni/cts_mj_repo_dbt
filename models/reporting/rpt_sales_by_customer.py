import snowflake.snowpark.functions as F
import pandas as pandas
import holidays

def avgsales(x,y):
    return y/x

def is_holiday(date_col):
    # Chez Jaffle
    french_holidays = holidays.France()
    is_holiday = (date_col in french_holidays)
    return is_holiday

def model(dbt, session):
    dbt.config(materialized = "table", schema="reporting", packages=["holidays"])

    dim_customers_df = dbt.ref("dim_customers")
    fct_orders_df = dbt.ref('fct_orders')

    customer_orders_df = (
       fct_orders_df
       .group_by('customerid')
       .agg(
           F.min(F.col('orderdate')).alias('first_orderdate'),
           F.max(F.col('orderdate')).alias('most_recent_orderdate'),
           F.count(F.col('orderid')).alias('number_of_orders'),
           F.sum(F.col('linesaleamount')).alias("total_sales")
       )
   )
    final_df = (
       dim_customers_df
       .join(customer_orders_df, dim_customers_df.customerid == customer_orders_df.customerid, 'left')
       .select(dim_customers_df.customerid,
               dim_customers_df.companyname,
               dim_customers_df.contactname,
               customer_orders_df.first_orderdate,
               customer_orders_df.most_recent_orderdate,
               customer_orders_df.number_of_orders,
               customer_orders_df.total_sales
                )
        )

    final_df = final_df.withColumn("avg_salevale", avgsales(final_df["number_of_orders"], final_df["total_sales"]))

    final_df = final_df.filter(F.col("FIRST_ORDERDATE").isNotNull())

    final_df = final_df.to_pandas()

    final_df["IS_HOLIDAY"] = final_df["FIRST_ORDERDATE"].apply(is_holiday)

    return final_df

"""
    final_df = (
       dim_customers_df
       .join(customer_orders_df, dim_customers_df.customerid == customer_orders_df.customerid, 'left')
       .select(dim_customers_df.customerid.alias('customerid'),
               dim_customers_df.companyname.alias('companyname'),
               dim_customers_df.contactname.alias('contactname'),
               customer_orders_df.first_order.alias('first_order'),
               customer_orders_df.most_recent_order.alias('most_recent_order'),
               customer_orders_df.number_of_orders.alias('number_of_orders'),
                customer_orders_df.total_sales.alias("total_sales")
                )
        )
"""
