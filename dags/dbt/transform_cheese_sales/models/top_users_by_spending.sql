-- models/top_users_by_spending.sql

{{ config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='sync_all_columns'
) }}

WITH ranked_users AS (
    SELECT 
        user_id,
        user_name,
        total_spent,
        ROW_NUMBER() OVER (ORDER BY total_spent DESC) AS rn
    FROM {{ ref('user_purchase_summary') }}
)

SELECT
    user_id,
    user_name,
    total_spent
FROM ranked_users
WHERE rn <= 10
