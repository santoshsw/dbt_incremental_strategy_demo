{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['sale_date', 'customer_id'],
    on_schema_change='fail'
  )
}}

SELECT
        sale_date,
        customer_id,
        COUNT(DISTINCT sale_id) AS num_purchases,
        SUM(total_amount) AS daily_spend,
        AVG(total_amount) AS avg_order_value,
        SUM(quantity) AS units_purchased,
        MAX(updated_at) AS last_transaction_update
FROM 
    {{ ref('sales_detail') }}

{% if is_incremental() %}
  
  WHERE sale_date >= DATEADD(day, -3, CURRENT_DATE)

{% endif %}

GROUP BY sale_date, customer_id
