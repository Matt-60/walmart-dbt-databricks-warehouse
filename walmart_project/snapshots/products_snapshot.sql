{% snapshot products_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='product_updated_at',
        invalidate_hard_deletes=True,
    )
}}

SELECT * FROM {{ ref('silver_products') }}

{% endsnapshot %}
