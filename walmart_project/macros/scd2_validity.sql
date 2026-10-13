{#
    Validity columns for a dimension built from a dbt snapshot (all versions, not only the current one).

    - valid_from: when this version started. The FIRST version of each key starts at 1900-01-01,
      otherwise facts that happened before the key was first snapshotted would match no version.
    - valid_to:   when it ended; the open (current) version ends at 9999-12-31.
    - is_current: the latest version.

    Ranges are half-open [valid_from, valid_to): join facts with
        fact_ts >= valid_from AND fact_ts < valid_to
    so every point in time matches exactly one version (BETWEEN would match two at a boundary).
#}
{% macro scd2_validity(business_key) -%}
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY {{ business_key }} ORDER BY dbt_valid_from) = 1
            THEN TIMESTAMP '1900-01-01'
        ELSE dbt_valid_from
    END                                            AS valid_from,
    COALESCE(dbt_valid_to, TIMESTAMP '9999-12-31') AS valid_to,
    dbt_valid_to IS NULL                           AS is_current
{%- endmacro %}
