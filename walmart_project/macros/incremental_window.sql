{#
    Filter for incremental models, used inside {% if is_incremental() %}.

    Normal run: rows changed since the newest row already loaded, minus a lookback window,
    so records that arrive late (with an older timestamp) are still picked up.
    Re-reading a few days is safe: the model MERGEs on unique_key, so nothing is duplicated.

    Backfill: pass a date range and only that range is reprocessed:
        dbt build --select silver_orders+ --vars "{backfill_from: '2026-10-01', backfill_to: '2026-10-05'}"
#}
{% macro incremental_window(source_column, target_column, lookback_days=3) -%}
    {%- if var('backfill_from', none) -%}
        {{ source_column }} >= TIMESTAMP '{{ var("backfill_from") }}'
        AND {{ source_column }} < TIMESTAMP '{{ var("backfill_to", "9999-12-31") }}'
    {%- else -%}
        {{ source_column }} > (
            SELECT COALESCE(MAX({{ target_column }}), TIMESTAMP '1900-01-01') - INTERVAL {{ lookback_days }} DAYS
            FROM {{ this }}
        )
    {%- endif -%}
{%- endmacro %}
