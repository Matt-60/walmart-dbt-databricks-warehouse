{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='customer_updated_at',
        invalidate_hard_deletes=True,
    )
}}

SELECT * FROM {{ ref('silver_customers') }}

{% endsnapshot %}
