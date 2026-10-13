{#
    Put every model in exactly the schema from its config (silver / gold / snapshots),
    instead of dbt's default "<target_schema>_<custom_schema>".
    Models without a custom schema fall back to the target schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
