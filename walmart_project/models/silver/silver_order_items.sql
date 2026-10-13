{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_item_id',
    on_schema_change='append_new_columns',
) }}

WITH source AS (
    SELECT * FROM {{ source('walmart_databricks', 'bronze_order_items') }}
    {% if is_incremental() %}
    WHERE {{ incremental_window('updated_timestamp', 'order_item_updated_at') }}
    {% endif %}
),

cleaned AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        CAST(quantity AS INT)                AS order_item_quantity,
        CAST(unit_price AS DECIMAL(10, 2))   AS order_item_unit_price,
        CAST(line_amount AS DECIMAL(12, 2))  AS order_item_line_amount,
        CASE is_active WHEN 'Y' THEN true WHEN 'N' THEN false END AS order_item_is_active,
        CAST(created_timestamp AS TIMESTAMP) AS order_item_created_at,
        CAST(updated_timestamp AS TIMESTAMP) AS order_item_updated_at,
        current_timestamp()                  AS silver_processed_at
    FROM source
)

SELECT * FROM cleaned
