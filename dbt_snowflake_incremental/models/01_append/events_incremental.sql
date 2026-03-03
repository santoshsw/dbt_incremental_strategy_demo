{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='fail'
  )
}}

SELECT
        event_id,
        user_id,
        event_name,
        event_timestamp,
        product_id,
        amount,
        event_date,
        CURRENT_TIMESTAMP() AS processed_at
FROM 
    {{ ref('events') }}

{% if is_incremental() %}

WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
AND event_timestamp IS NOT NULL

{% endif %}
