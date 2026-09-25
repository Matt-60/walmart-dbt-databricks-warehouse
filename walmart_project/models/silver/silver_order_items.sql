{{ config(
    materialized='incremental',
    unique_key='order_item_id',
) }}


SELECT
    order_item_id,
    order_id,
    product_id,
    quantity as order_item_quantity,
    unit_price as order_item_unit_price,
    line_amount as order_item_line_amount,
    is_active AS order_item_is_active,
    created_timestamp AS order_item_created_at,
    updated_timestamp AS order_item_updated_at,
    current_timestamp() AS silver_processed_at
FROM {{ source('walmart_databricks', 'bronze_order_items') }}

{% if is_incremental() %}
    WHERE updated_timestamp > (SELECT COALESCE(MAX(order_item_updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
