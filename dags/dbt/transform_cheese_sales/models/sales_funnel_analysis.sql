{{ config(
    materialized='incremental',
    unique_key='utm_key',
    on_schema_change='sync_all_columns'
) }}

WITH aggregated_sales AS (
    SELECT 
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(sale_id) AS total_sales,
        SUM(total_revenue) AS total_revenue
    FROM {{ ref('enriched_sales') }}
    GROUP BY utm_source, utm_medium, utm_campaign
),

aggregated_sales_with_key AS (
    SELECT
        *,
        CONCAT(utm_source, '|', utm_medium, '|', utm_campaign) AS utm_key
    FROM aggregated_sales
)

SELECT
    utm_source,
    utm_medium,
    utm_campaign,
    total_sales,
    total_revenue,
    utm_key
FROM aggregated_sales_with_key
