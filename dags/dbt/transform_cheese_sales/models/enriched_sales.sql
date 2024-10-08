-- models/enriched_sales.sql

{{ config(
    materialized='incremental',
    unique_key='sale_id',
    on_schema_change='sync_all_columns'
) }}

{% if is_incremental() %}
  -- For incremental runs, only process new or updated records
  WITH new_sales AS (
    SELECT *
    FROM {{ source('raw', 'sales') }}
    WHERE sale_date > (SELECT MAX(sale_date) FROM {{ this }})
  )
{% else %}
  -- For full runs, process all records
  WITH new_sales AS (
    SELECT *
    FROM {{ source('raw', 'sales') }}
  )
{% endif %}

SELECT 
    s.sale_id,
    s.user_id,
    u.user_name,
    b.cheese_title AS cheese_name,
    b.cheese_genre AS cheese_type,
    s.quantity,
    s.sale_date,
    uparams.utm_source,
    uparams.utm_medium,
    uparams.utm_campaign,
    uparams.utm_term,
    uparams.utm_content,
    b.price * s.quantity AS total_revenue
FROM new_sales s
JOIN {{ source('raw', 'users') }} u ON s.user_id = u.user_id
JOIN {{ source('raw', 'cheeses') }} b ON s.cheese_id = b.cheese_id
JOIN {{ source('raw', 'utms') }} uparams ON s.utm_id = uparams.utm_id
