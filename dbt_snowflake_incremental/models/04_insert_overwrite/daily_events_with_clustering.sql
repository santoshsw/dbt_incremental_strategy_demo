{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    unique_key=['event_date', 'user_id', 'event_name'],
    on_schema_change='fail'
  )
}}

SELECT
    event_date,
    user_id,
    event_name,
    COUNT(*) AS event_count,
    SUM(amount) AS total_amount,
    MIN(event_timestamp) AS first_event,
    MAX(event_timestamp) AS last_event,
    CURRENT_TIMESTAMP() AS processed_at
FROM {{ ref('events') }}

{% if is_incremental() %}
  WHERE event_date >= DATEADD(day, -1, CURRENT_DATE)
{% endif %}

GROUP BY
    event_date, user_id, event_name