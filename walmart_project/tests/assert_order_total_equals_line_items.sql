-- Orders whose total differs from the sum of their line items by more than 1 cent.
-- Returns the failing rows; the test passes when the result is empty.
SELECT
    o.order_id,
    o.order_total_amount,
    SUM(oi.order_item_line_amount) AS line_items_total
FROM {{ ref('silver_orders') }} o
JOIN {{ ref('silver_order_items') }} oi
    ON oi.order_id = o.order_id
GROUP BY o.order_id, o.order_total_amount
HAVING ABS(o.order_total_amount - SUM(oi.order_item_line_amount)) > 0.01
