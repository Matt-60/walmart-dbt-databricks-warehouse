{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    o.customer_id,
    o.store_id,
    oi.order_item_quantity,
    oi.order_item_unit_price,
    oi.order_item_line_amount,
    oi.order_item_is_active,
    o.order_timestamp,
    current_timestamp() AS gold_processed_at
FROM {{ ref('silver_order_items') }} oi
LEFT JOIN {{ ref('silver_orders') }} o
    ON oi.order_id = o.order_id

{% if is_incremental() %}
WHERE oi.silver_processed_at > (SELECT COALESCE(MAX(gold_processed_at), '1900-01-01') FROM {{ this }})
{% endif %}
