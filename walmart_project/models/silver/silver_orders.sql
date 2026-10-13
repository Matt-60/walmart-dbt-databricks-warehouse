{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='append_new_columns',
) }}

WITH source AS (
    SELECT * FROM {{ source('walmart_databricks', 'bronze_orders') }}
    {% if is_incremental() %}
    WHERE {{ incremental_window('updated_timestamp', 'order_updated_at') }}
    {% endif %}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        store_id,
        CAST(order_timestamp AS TIMESTAMP)        AS order_timestamp,
        CAST(order_timestamp AS DATE)             AS order_date,            -- key for a date dimension
        INITCAP(TRIM(payment_method))             AS order_payment_method,  -- controlled list
        INITCAP(TRIM(order_status))               AS order_status,          -- controlled list
        CAST(total_amount AS DECIMAL(12, 2))      AS order_total_amount,
        CASE is_active WHEN 'Y' THEN true WHEN 'N' THEN false END AS order_is_active,
        CAST(created_timestamp AS TIMESTAMP)      AS order_created_at,
        CAST(updated_timestamp AS TIMESTAMP)      AS order_updated_at,
        current_timestamp()                       AS silver_processed_at
    FROM source
)

SELECT * FROM cleaned
