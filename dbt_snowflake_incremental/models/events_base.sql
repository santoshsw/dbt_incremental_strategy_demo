{{ 
    config(
        materialized='table'
        ) 
}}

SELECT
        event_id,
        user_id,
        event_name,
        event_timestamp,
        product_id,
        amount,
        event_date
FROM 
    {{ ref('events') }}
