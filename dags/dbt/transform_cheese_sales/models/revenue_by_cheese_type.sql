-- models/revenue_by_cheese_type.sql

{{ config(
    materialized='incremental',
    unique_key='cheese_type',
    on_schema_change='sync_all_columns'
) }}

WITH aggregated_revenue AS (
    SELECT 
        cheese_type,
        SUM(total_revenue) AS total_revenue
    FROM {{ ref('enriched_sales') }}
    GROUP BY cheese_type
)

SELECT
    *
FROM aggregated_revenue
