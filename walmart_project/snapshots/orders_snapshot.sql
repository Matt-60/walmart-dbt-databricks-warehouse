{% snapshot orders_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_id',
        strategy='timestamp',
        updated_at='order_updated_at',
        invalidate_hard_deletes=True,
    )
}}

SELECT * FROM {{ ref('silver_orders') }}

{% endsnapshot %}
