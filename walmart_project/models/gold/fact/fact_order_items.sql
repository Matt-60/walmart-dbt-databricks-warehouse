{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_item_id',
    on_schema_change='append_new_columns'
) }}

-- Same watermark and lookback window as Silver (macros/incremental_window.sql), on the source change time
-- stored in the fact itself, so late-arriving rows picked up by Silver also reach the fact.
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
    oi.order_item_updated_at,
    current_timestamp() AS gold_processed_at
FROM {{ ref('silver_order_items') }} oi
LEFT JOIN {{ ref('silver_orders') }} o
    ON oi.order_id = o.order_id

{% if is_incremental() %}
WHERE {{ incremental_window('oi.order_item_updated_at', 'order_item_updated_at') }}
{% endif %}
