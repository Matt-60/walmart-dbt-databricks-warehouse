# Walmart Retail Data Warehouse

An orchestrated ELT pipeline for multi-store retail sales analytics — CDC-ingested source data transformed on **Databricks with dbt**, historized with **SCD Type 2**, and run end-to-end every day by a self-hosted **Apache Airflow** deployment.

`Apache Airflow` · `Databricks` · `dbt` · `SQL` · `Medallion Architecture` · `Star Schema` · `SCD Type 2` · `CDC` · `Docker`

---

## 🎯 Business Goal

A multi-store retail chain needs one analytics-ready view of sales — who bought what, from which store, and how customer, product and order attributes evolve over time — refreshed automatically every day, tested before it reaches reporting, and resilient enough that a bad load never silently corrupts downstream dashboards. This warehouse delivers exactly that: CDC ingestion into a Databricks Lakehouse, dbt-modeled Silver/Gold layers with full SCD Type 2 history on every dimension, and a fully containerized Airflow deployment that runs the whole chain unattended.

## 🏗️ Architecture

```
Databricks Job (CDC) → walmart.bronze
        ↓
dbt Silver (cleaned; incremental where it earns its keep)
        ↓ dbt test — quality gate
dbt snapshot (SCD Type 2 dimension history)
        ↓
dbt Gold — Dimensions  ‖  Fact
        ↓
dbt Gold — One Big Table → BI / Analytics
```

| Layer | Where it lives | Purpose |
|---|---|---|
| **Bronze** | Databricks (`walmart.bronze`), populated by a Databricks Job triggered from Airflow | Raw CDC-ingested tables: customers, employees, orders, order_items, products, stores. Built and run entirely in the Databricks workspace, outside this dbt project |
| **Silver** | dbt models, `walmart.silver` | Cleaned, renamed columns. `orders` and `order_items` are incremental (watermarked); `customers`, `employees`, `products`, `stores` are small reference tables rebuilt in full every run |
| **Snapshots** | `dbt snapshot`, `walmart.snapshots` | SCD Type 2 history for all five dimension sources — timestamp strategy, hard deletes invalidated |
| **Gold** | dbt models, `walmart.gold` | Star schema — `DimCustomers`, `DimEmployees`, `DimOrders`, `DimProducts`, `DimStores` (current-version projections of the snapshots) + `FactOrderItems` (incremental) + a denormalized OBT for reporting |

## 🔄 Ingestion — CDC into Bronze

Bronze ingestion is a Databricks Job that performs CDC loading into `walmart.bronze`. Its implementation lives in the Databricks workspace, not in this repository — this project picks up from the moment Bronze data lands and owns everything from Silver onward. Airflow triggers and waits on the job through the Databricks SDK:

```python
ws = WorkspaceClient(host=Variable.get("databricks_host"), token=Variable.get("databricks_token"))

run = ws.jobs.run_now_and_wait(job_id=194882876441539, timeout=timedelta(minutes=30))

if run.state.result_state != RunResultState.SUCCESS:
    raise Exception(f"CDC job failed with state: {run.state.result_state}")
```

A non-`SUCCESS` result raises immediately, so a failed Bronze load stops the DAG before Silver ever touches bad data.

## 🥈 Silver — Cleaned & Selectively Incremental

`orders` and `order_items` are the two tables that actually grow every day, so they're incremental, filtered by an `updated_timestamp` watermark against the table's own last-loaded value:

```sql
{{ config(materialized='incremental', unique_key='order_item_id') }}

SELECT
    order_item_id, order_id, product_id,
    quantity AS order_item_quantity,
    unit_price AS order_item_unit_price,
    line_amount AS order_item_line_amount,
    is_active AS order_item_is_active,
    created_timestamp AS order_item_created_at,
    updated_timestamp AS order_item_updated_at,
    current_timestamp() AS silver_processed_at
FROM {{ source('walmart_databricks', 'bronze_order_items') }}

{% if is_incremental() %}
    WHERE updated_timestamp > (SELECT COALESCE(MAX(order_item_updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
```

`customers`, `employees`, `products` and `stores` are small reference tables — cheap enough to fully rebuild from source every run, so they are, rather than adding incremental-merge complexity where it buys nothing.

## 📸 Snapshots — SCD Type 2

Before Gold is built, `dbt snapshot` captures point-in-time history for every dimension source, using the `timestamp` strategy against each table's own `updated_at` column:

```sql
{% snapshot customers_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='customer_updated_at',
    invalidate_hard_deletes=True,
) }}
SELECT * FROM {{ ref('silver_customers') }}
{% endsnapshot %}
```

Gold's dimensions read from these snapshots rather than from Silver directly, filtered to the current row only (`WHERE dbt_valid_to IS NULL`) — so today's reporting always reflects the latest attribute values, while the full change history stays queryable separately for anyone who needs it. `order_items` is deliberately never snapshotted — it's a transactional fact, not a slowly-changing attribute.

## ⭐ Gold — Star Schema + One Big Table

| Table | Type | Notes |
|---|---|---|
| `FactOrderItems` | Fact | Grain: 1 row = 1 order line item. Incremental, built directly from Silver (not from snapshots) |
| `DimCustomers` | Dimension — current SCD2 version | Adds `full_name` and `email_domain` (derived from the customer's email) |
| `DimProducts` | Dimension — current SCD2 version | Adds a `price_tier` band (Budget / Mid / Premium) |
| `DimStores` | Dimension — current SCD2 version | |
| `DimOrders` | Dimension — current SCD2 version | Order-level attributes: payment method, status |
| `DimEmployees` | Dimension — current SCD2 version | Adds a `salary_band` (Entry / Mid / Senior); modeled for staffing analysis — see note in Known Limitations |
| `OBT` | Denormalized one-big-table | `FactOrderItems` joined to every dimension except `DimEmployees`, built for direct BI consumption |

<details>
<summary><b>🧩 OBT join pattern (click to expand)</b></summary>

Instead of a hand-written chain of joins, the OBT declares each join as data and loops over it to build the query — adding a new dimension later is a one-entry change:

```jinja
{% set configs = [
    { "table": ref('fact_order_items'), "columns": "f.order_item_id, f.order_id, ...", "alias": "f" },
    { "table": ref('dim_orders'), "columns": "o.order_payment_method, o.order_status",
      "alias": "o", "join_condition": "f.order_id = o.order_id" },
    { "table": ref('dim_customers'), "columns": "c.full_name, c.email_domain, ...",
      "alias": "c", "join_condition": "f.customer_id = c.customer_id" },
    ...
] %}

SELECT
    {% for config in configs %}{{ config['columns'] }}{% if not loop.last %},{% endif %}{% endfor %}
FROM
    {% for config in configs %}
        {% if loop.first %}{{ config['table'] }} AS {{ config['alias'] }}
        {% else %}LEFT JOIN {{ config['table'] }} AS {{ config['alias'] }} ON {{ config['join_condition'] }}
        {% endif %}
    {% endfor %}
```
</details>

## ✅ Data Quality (dbt tests)

- **Primary key integrity** — `not_null` + `unique` on every dimension and fact key
- **Referential integrity** — `relationships` tests: order_items → orders/products, orders → customers/stores, employees → stores
- **Domain validation** — `accepted_values` on `product_category`, `product_brand`, `order_payment_method`, `order_status`
- **Business rule checks** — `dbt_utils.expression_is_true` enforcing positive prices, quantities, salaries and order totals
- **Soft constraints** — `not_null` on audit timestamp columns runs at `severity: warn`, so a missing timestamp flags a warning instead of blocking the whole pipeline

`silver_test` runs right after Silver builds and **before** `snapshot`/Gold — a fail-fast gate so bad Silver data never gets baked into SCD2 history.

## ⚙️ Orchestration (Airflow)

```
data_ingestion (Databricks CDC job)
       ↓
clean_target (wipe dbt's target/ — clean compile every run)
       ↓
silver  →  silver_test  →  snapshot
                              ↓
                      ┌───────┴───────┐
                   gold_dims       gold_fact
                      └───────┬───────┘
                          gold_obt
```

Runs daily at **11:00 Europe/Warsaw** (`0 11 * * *`, `catchup=False`). The full stack — API server, scheduler, DAG processor, Celery worker, triggerer, Postgres metadata DB, Redis broker — runs as a self-hosted Docker Compose cluster (Airflow 3.2), with the dbt project mounted straight into the containers so `BashOperator` tasks call the dbt CLI directly against the same files used for local development.

## 🧠 Key Design Decisions

- **Incremental only where it earns its keep** — high-volume `orders`/`order_items` use a watermark; small reference tables are cheap enough to fully rebuild, avoiding incremental-merge complexity that would buy nothing
- **SCD2 decoupled from the fact grain** — only the five dimension sources are snapshotted; `order_items` never is, since it's a transactional event, not a slowly-changing attribute
- **Gold dimensions read from snapshots, not Silver** — filtering to `dbt_valid_to IS NULL` guarantees Gold always reflects the latest values while full history stays queryable separately
- **Custom `generate_schema_name` macro** — forces every model into exactly the schema set in its config (`bronze`/`silver`/`gold`/`snapshots`) instead of dbt's default target-prefixed naming, keeping the Databricks catalog layout predictable across environments
- **Config-driven Jinja join loop for the OBT** — the join list is data, looped once, instead of a hand-written repetitive query
- **Tests gate the pipeline, not just document it** — `silver_test` sits before `snapshot`/`gold_*` in the DAG
- **`databricks-sdk` over a dedicated Airflow provider** — the CDC job is triggered with `run_now_and_wait()` directly, a lighter dependency than a full provider install for one job trigger

## ⚠️ Known Limitations / Current Scope

- `DimEmployees` is modeled (with a derived salary band) but not yet joined into the Fact/OBT — the source `orders` table carries no `employee_id`, so today it's a standalone reference dimension rather than a sales-attribution one
- Bronze ingestion (the CDC job itself) runs as a Databricks Job outside this repository; this project owns orchestration plus Silver/Gold/OBT, not the Bronze job's code

<details>
<summary><b>🛠️ Tech stack, project structure & future improvements (click to expand)</b></summary>

**Technology Stack**

| Technology | Purpose |
|---|---|
| Databricks | Lakehouse platform; hosts Bronze/Silver/Gold tables and runs the CDC ingestion job |
| Apache Airflow 3.2 (CeleryExecutor) | Orchestration — daily DAG, Dockerized (Postgres + Redis + API server + scheduler + DAG processor + worker + triggerer) |
| dbt-core / dbt-databricks | Transformation, testing, snapshotting against Databricks |
| databricks-sdk | Triggers and polls the Bronze CDC job from Airflow |
| dbt_utils, dbt_profiler | dbt packages — reusable test macros and data-profiling utilities |
| Docker / Docker Compose | Self-hosted local Airflow deployment |
| Python 3.12, uv | Dependency management |

**Project Structure**

```
walmart-dbt-databricks-warehouse/
├── airflow/
│   ├── dags/
│   │   └── orchestrate.py          -- daily DAG: CDC trigger → dbt run/test/snapshot chain
│   ├── docker-compose.yaml         -- Airflow 3.2 CeleryExecutor cluster
│   ├── Dockerfile
│   └── requirements.txt
├── walmart_project/                -- dbt project (mounted into the Airflow containers)
│   ├── models/
│   │   ├── source/sources.yml      -- Bronze tables in walmart.bronze
│   │   ├── silver/                 -- cleaned models + properties.yml (tests)
│   │   └── gold/
│   │       ├── dims/
│   │       ├── fact/fact_order_items.sql
│   │       └── obt.sql
│   ├── snapshots/                  -- SCD Type 2 for all 5 dimension sources
│   ├── analyses/                   -- ad hoc data-profiling queries
│   ├── macros/generate_schema_name.sql
│   └── dbt_project.yml
├── pyproject.toml
└── uv.lock
```

**Future Improvements**
- Join `DimEmployees` into the Fact/OBT once an employee-to-order relationship exists in the source data
- CI (`dbt build` on PR) instead of relying only on the scheduled run to catch regressions
- Alerting on Airflow task failure
- Direct BI-tool connection to the OBT
- Point-in-time dimension joins for historically-accurate reporting (today's joins are always "current version")

</details>

---

*Demonstrates a complete, orchestrated ELT pipeline: Databricks CDC ingestion → dbt Silver/Gold modeling on Databricks → SCD Type 2 dimension history → data quality testing → daily Airflow orchestration on a self-hosted Celery cluster → analytics-ready delivery.*
