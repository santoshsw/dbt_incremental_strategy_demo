{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_sk',
    on_schema_change='fail'
  )
}}
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_sk,
    customer_id,
    customer_name,
    email,
    phone,
    country,
    signup_date,
    updated_at,
    CURRENT_TIMESTAMP() AS last_processed_at
FROM {{ ref('customers') }}
WHERE customer_name IS NOT NULL
  AND customer_id IS NOT NULL

{% if is_incremental() %}
  AND updated_at > 
( SELECT COALESCE(MAX(updated_at), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }} )
{% endif %}
