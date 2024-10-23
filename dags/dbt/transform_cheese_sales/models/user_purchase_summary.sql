{{ config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='sync_all_columns'
) }}

WITH user_summary AS (
    SELECT 
        user_id,
        user_name,
        COUNT(sale_id) AS total_purchases,
        SUM(total_revenue) AS total_spent,
        ARRAY_AGG(STRUCT(cheese_name, total_revenue) ORDER BY total_revenue DESC)[SAFE_OFFSET(0)].cheese_name AS favorite_cheese
    FROM {{ ref('enriched_sales') }}
    GROUP BY user_id, user_name
)

SELECT
    user_id,
    user_name,
    total_purchases,
    total_spent,
    favorite_cheese
FROM user_summary
