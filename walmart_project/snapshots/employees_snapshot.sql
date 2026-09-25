{% snapshot employees_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='employee_updated_at',
        invalidate_hard_deletes=True,
    )
}}

SELECT * FROM {{ ref('silver_employees') }}

{% endsnapshot %}
