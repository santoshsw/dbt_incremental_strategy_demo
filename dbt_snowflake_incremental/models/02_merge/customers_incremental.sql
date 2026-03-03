{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id',
    merge_update_columns = ['customer_name', 'email', 'phone', 'country', 'updated_at'],
    on_schema_change='fail'
  )
}}
SELECT
    customer_id,
    customer_name,
    email,
    phone,
    country,
    signup_date,
    updated_at,
    CURRENT_TIMESTAMP() AS last_processed_at
FROM 
    {{ ref('customers') }}

WHERE customer_name IS NOT NULL
  AND customer_id IS NOT NULL

{% if is_incremental() %}
  AND updated_at > ( SELECT MAX(updated_at) FROM {{ this }} )

{% endif %}
