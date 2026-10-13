{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_item_id',
    on_schema_change='append_new_columns'
) }}

-- Grain: one row per order line item.
-- *_sk columns point to the dimension VERSION that was valid when the order was placed (point-in-time join),
-- so reports can show attributes "as they were" (via *_sk) or "as they are now" (via *_id + is_current).
-- Half-open ranges [valid_from, valid_to) do not overlap, so each join matches exactly one version
-- and the fact is never multiplied.
-- Same watermark and lookback window as Silver (macros/incremental_window.sql).
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    o.customer_id,
    o.store_id,
    c.customer_sk,
    p.product_sk,
    s.store_sk,
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
LEFT JOIN {{ ref('dim_customers') }} c
    ON  c.customer_id = o.customer_id
    AND o.order_timestamp >= c.valid_from
    AND o.order_timestamp <  c.valid_to
LEFT JOIN {{ ref('dim_products') }} p
    ON  p.product_id = oi.product_id
    AND o.order_timestamp >= p.valid_from
    AND o.order_timestamp <  p.valid_to
LEFT JOIN {{ ref('dim_stores') }} s
    ON  s.store_id = o.store_id
    AND o.order_timestamp >= s.valid_from
    AND o.order_timestamp <  s.valid_to

{% if is_incremental() %}
WHERE {{ incremental_window('oi.order_item_updated_at', 'order_item_updated_at') }}
{% endif %}
