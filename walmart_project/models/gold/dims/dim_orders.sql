SELECT
    order_id,
    order_payment_method,
    order_status,
    order_is_active
FROM {{ ref('orders_snapshot') }}
WHERE dbt_valid_to IS NULL
