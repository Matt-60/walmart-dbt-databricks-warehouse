{% snapshot stores_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='store_id',
        strategy='timestamp',
        updated_at='store_updated_at',
        invalidate_hard_deletes=True,
    )
}}

SELECT * FROM {{ ref('silver_stores') }}

{% endsnapshot %}
