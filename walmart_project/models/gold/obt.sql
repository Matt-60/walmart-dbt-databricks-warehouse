{% set configs = [
    {
        "table" : ref('fact_order_items'),
        "columns": """ 
                    f.order_item_id,
                    f.order_id,
                    f.product_id,
                    f.customer_id,
                    f.store_id,
                    f.order_item_quantity,
                    f.order_item_unit_price,
                    f.order_item_line_amount,
                    f.order_item_is_active,
                    f.order_timestamp,
                    current_timestamp() AS obt_processed_at
                   """,
        "alias": "f",
    },
    {
        "table" : ref('dim_orders'),
        "columns": """
                    o.order_payment_method,
                    o.order_status,
                    o.order_is_active
                   """,
        "alias": "o",
        "join_condition": "f.order_id = o.order_id",
    },
    {
        "table" : ref('dim_customers'),
        "columns": """
                    c.customer_first_name,
                    c.customer_last_name,
                    c.full_name,
                    c.customer_email,
                    c.email_domain,
                    c.customer_phone,
                    c.customer_city,
                    c.customer_province,
                    c.customer_country,
                    c.customer_is_active
                   """,
        "alias": "c",
        "join_condition": "f.customer_id = c.customer_id",
    },
    {
        "table" : ref('dim_products'),
        "columns": """
                    p.product_name,
                    p.product_category,
                    p.product_brand,
                    p.product_price,
                    p.price_tier,
                    p.product_is_active
                   """,
        "alias": "p",
        "join_condition": "f.product_id = p.product_id",
    },
    {
        "table" : ref('dim_stores'),
        "columns": """
                    s.store_name,
                    s.store_city,
                    s.store_province,
                    s.store_country,
                    s.store_is_active
                   """,
        "alias": "s",
        "join_condition": "f.store_id = s.store_id",
    }
]
%}

SELECT
    {% for config in configs %}
        {{ config['columns'] }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM
    {% for config in configs %}
        {%if loop.first %}
            {{ config['table'] }} AS {{ config['alias'] }}
        {% else %}
LEFT JOIN
            {{ config['table'] }} AS {{ config['alias'] }}
            ON {{ config['join_condition'] }}
        {%endif%}
    {% endfor %}
