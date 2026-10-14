# Walmart Retail Data Warehouse

An orchestrated ELT pipeline for multi-store retail sales analytics — change data captured with **Lakeflow AUTO CDC** on Databricks, modeled with **dbt** into a star schema with **point-in-time SCD Type 2** dimensions, tested at every layer, and run end-to-end every day by a self-hosted **Apache Airflow** deployment.

`Databricks` · `Lakeflow AUTO CDC` · `Delta Change Data Feed` · `dbt` · `SCD Type 2` · `Star Schema` · `Apache Airflow` · `Asset Bundles` · `Docker`

---

## 🎯 Business Goal

A multi-store retail chain needs one analytics-ready view of sales — who bought what, from which store, and how customer, product and store attributes evolve over time — refreshed automatically every day, tested before it reaches reporting, and resilient enough that a bad load never silently corrupts downstream dashboards.

Every sale is reported with the attributes that were true **when it happened** (a customer who moved from Toronto to Montreal keeps their past orders in Toronto), while today's values stay one column away.

## 🏗️ Architecture

```
walmart.source  (simulated store system: Delta + Change Data Feed)
        │  01_generate_changes — one day of activity per run
        ▼
Lakeflow pipeline: Change Data Feed → AUTO CDC (keys, ordering, deletes)
        ▼
walmart.bronze  (streaming tables, current state)
        │  dbt source freshness — fails if Bronze stopped updating
        ▼
dbt Silver      cleaned + typed; orders / order_items incremental      ── tests gate
        ▼
dbt snapshot    SCD Type 2 history (timestamp strategy)
        ▼
dbt Gold        SCD2 dimensions → fact (point-in-time join) → One Big Table
```

| Layer | Where it lives | Purpose |
|---|---|---|
| **Source** | `walmart.source`, Delta + Change Data Feed | Stands in for the store's OLTP database. A generator notebook writes one day of new orders, status changes and attribute changes per run |
| **Bronze** | `walmart.bronze`, Lakeflow streaming tables | Current state of every source table, maintained by AUTO CDC from the Change Data Feed — inserts, updates and deletes |
| **Silver** | dbt, `walmart.silver` | Trimmed / normalized text, explicit types, boolean flags. `orders` and `order_items` are incremental with a lookback window; small reference tables are rebuilt in full |
| **Snapshots** | dbt, `walmart.snapshots` | SCD Type 2 history of customers, products, stores, employees and orders |
| **Gold** | dbt, `walmart.gold` | `dim_customers`, `dim_products`, `dim_stores` (all versions, SCD2) · `dim_orders`, `dim_employees` (current) · `fact_order_items` (incremental) · `obt` for BI |

## 🔄 Ingestion — Lakeflow AUTO CDC on the Change Data Feed

Everything Bronze needs lives in [`databricks/`](databricks/) and is deployed as a **Databricks Asset Bundle** — one job and one pipeline:

| Job task | What it does |
|---|---|
| `setup_source` | One-time migration step (idempotent): creates `walmart.source.*` with Change Data Feed enabled |
| `generate_changes` | Simulates a day of activity: ~40 pending orders completed or cancelled, ~2 returned, 50 new orders with 1–4 line items, 3 new customers, plus SCD2-relevant changes — 2 customers move city, 3 products change price, 2 employees get a raise |
| `bronze_cdc` | Runs the Lakeflow pipeline below |

The pipeline reads each source table's Change Data Feed as a stream and applies it with AUTO CDC — no hand-written MERGE, no watermark table:

```python
dlt.apply_changes(
    target=f"bronze_{table}",
    source=f"{table}_changes",          # readChangeFeed stream, update pre-images dropped
    keys=[key],
    sequence_by=F.col("_commit_version"),
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=1,
)
```

The pipeline checkpoint remembers what was processed; on a first run or full refresh the stream starts from a snapshot of the whole table, so Bronze can be rebuilt from scratch at any time.

<details>
<summary><b>📜 Why the source is simulated (click to expand)</b></summary>

The project originally ingested from a PostgreSQL database hosted on Ghost through a Lakeflow Connect query-based connector (cursor on `updated_timestamp`). When the hosting service shut down, the source was moved into Databricks as Delta tables seeded from the existing Bronze data, and the connector was replaced with the CDF → AUTO CDC pipeline. Two side effects worth noting: the project is now fully reproducible without external infrastructure, and Bronze now receives **deletes** — the cursor-based connector could only see rows whose timestamp moved forward.

</details>

## 🥈 Silver — Cleaned, Typed, Incremental Where It Matters

- **Cleaning:** `TRIM` on all text, lower-cased emails, digits-only phone numbers, `INITCAP` only on controlled vocabularies (category, payment method, status) — free text like names and brands (`LG`) keeps its casing.
- **Types:** explicit `DECIMAL` amounts, `TIMESTAMP`s, and `is_active` mapped from `Y`/`N` to `BOOLEAN` (anything else becomes `NULL` and fails a test).
- **Incremental models** (`orders`, `order_items`) share one macro, [`incremental_window`](walmart_project/macros/incremental_window.sql):

```sql
{% if var('backfill_from', none) %}
    updated_timestamp >= '{{ var("backfill_from") }}' AND updated_timestamp < '{{ var("backfill_to") }}'
{% else %}
    updated_timestamp > (SELECT MAX(order_updated_at) - INTERVAL 3 DAYS FROM {{ this }})
{% endif %}
```

Simplified above; a normal run re-reads a 3-day lookback window so late-arriving rows are not skipped; re-reading is safe because the models `MERGE` on their unique key. A date-range backfill is one command:

```bash
dbt build --select silver_orders+ --vars "{backfill_from: '2026-10-01', backfill_to: '2026-10-05'}"
```

Bronze is kept as current state by AUTO CDC (one row per key), so deduplication happens at ingestion; uniqueness is enforced by tests in Silver.

## ⭐ Gold — Point-in-Time SCD Type 2

**Dimensions keep every version.** `dim_customers`, `dim_products` and `dim_stores` read all rows of their snapshot, with the snapshot's `dbt_scd_id` as the version key and a half-open validity range from the [`scd2_validity`](walmart_project/macros/scd2_validity.sql) macro. Illustrative example — a customer who moved:

| customer_sk | customer_id | customer_city | valid_from | valid_to | is_current |
|---|---|---|---|---|---|
| `a91f…` | 42 | Toronto | 1900-01-01 | 2026-10-12 11:02 | false |
| `07c3…` | 42 | Montreal | 2026-10-12 11:02 | 9999-12-31 | true |

The first version of each key starts in 1900, so orders placed before a customer was first snapshotted still match a version.

**The fact points to the version valid when the order was placed:**

```sql
LEFT JOIN {{ ref('dim_customers') }} c
    ON  c.customer_id = o.customer_id
    AND o.order_timestamp >= c.valid_from
    AND o.order_timestamp <  c.valid_to      -- half-open: exactly one version matches
```

Validity ranges never overlap (tested), so the join adds a `*_sk` column without multiplying fact rows. Reports can then choose:

- **as it was** — join on `customer_sk`: sales by the customer's city *at the time of purchase*
- **as it is** — join on `customer_id` + `is_current`: today's attributes

The OBT exposes both, e.g. `customer_city` (at order time) next to `customer_city_current`.

| Table | Grain / type | Notes |
|---|---|---|
| `fact_order_items` | 1 row per order line, incremental | Natural keys + version keys (`customer_sk`, `product_sk`, `store_sk`) |
| `dim_customers` | SCD2, one row per version | `full_name`, `email_domain` |
| `dim_products` | SCD2, one row per version | `price_tier` (Budget / Mid / Premium) as of each version |
| `dim_stores` | SCD2, one row per version | |
| `dim_orders` | Current state | Payment method, status — status is an order attribute, not a slowly changing dimension |
| `dim_employees` | Current state | `salary_band`; not linked to sales (orders carry no `employee_id`) |
| `obt` | Same grain as the fact | Config-driven Jinja join loop over all dimensions |

## ✅ Data Quality

Tests run as part of `dbt build`, so any failure stops the pipeline before bad data reaches the next layer.

| Area | Tests |
|---|---|
| Keys | `not_null` + `unique` on every business and version key; one current version per business key |
| Referential integrity | order_items → orders / products, orders → customers / stores, fact `*_sk` → dimension versions |
| SCD2 | `dbt_utils.mutually_exclusive_ranges` — validity ranges of one key never overlap |
| Domains | `accepted_values` on category, brand, payment method, status, price tier |
| Business rules | positive prices / quantities / salaries; `line_amount = quantity × unit_price`; order total = sum of its line items (singular test) |
| Grain | `unique` on `order_item_id` in the fact and the OBT — no join may multiply rows |
| Freshness | `dbt source freshness` on Bronze orders / order_items — warn after 1 day, error after 2 |

## ⚙️ Orchestration (Airflow)

```
data_ingestion   Databricks job: generate_changes → Lakeflow AUTO CDC
      ↓
dbt_setup        dbt clean + dbt deps
      ↓
source_freshness
      ↓
silver           dbt build  (models + tests)
      ↓
snapshot
      ↓
gold_dims  →  gold_fact  →  gold_obt      dbt build, sequential
```

Runs daily at **11:00 Europe/Warsaw** (`catchup=False`, one retry per task). `data_ingestion` triggers the bundle-deployed job with the Databricks SDK (`run_now_and_wait`) and fails the DAG if the job does not succeed. Gold runs sequentially on purpose: the fact's relationship tests need the dimensions already rebuilt.

The full Airflow 3.2 stack — API server, scheduler, DAG processor, Celery worker, triggerer, Postgres, Redis — runs in Docker Compose, with the dbt project mounted into the containers so tasks call the dbt CLI against the same files used for local development.

## 🧠 Key Design Decisions

- **AUTO CDC instead of a hand-written MERGE** — keys, change ordering, deletes and checkpointing are handled by the platform; the ingestion code is a dozen lines per table definition.
- **Change Data Feed instead of a timestamp cursor** — captures deletes and does not depend on every writer updating `updated_timestamp`.
- **Point-in-time SCD2 with half-open ranges** — history in the snapshots is actually used by the fact; `[valid_from, valid_to)` guarantees one match per order.
- **Lookback window + idempotent MERGE** — late-arriving rows are picked up, and any date range can be backfilled without duplicates.
- **Tests gate the pipeline** — `dbt build` at every step, freshness check before Silver, grain tests on the fact and the OBT.
- **Custom `generate_schema_name`** — models land in exactly `silver` / `gold` / `snapshots`, not dbt's default target-prefixed schemas.

## ⚠️ Known Limitations

- **Hard deletes in incremental Silver** — AUTO CDC deletes rows from Bronze, and fully rebuilt Silver models follow, but the incremental `silver_orders` / `silver_order_items` only insert and update. A periodic `--full-refresh` of those two models would remove deleted rows.
- **Synthetic changes** — the source is a simulation seeded from real Bronze history; change volumes are configurable (`new_orders`, `new_customers` job parameters).
- **`dim_employees` is standalone** — orders carry no `employee_id`, so it is not linked to sales.

<details>
<summary><b>🛠️ Tech stack, project structure & future improvements (click to expand)</b></summary>

**Technology Stack**

| Technology | Purpose |
|---|---|
| Databricks (serverless), Unity Catalog | Lakehouse platform for all layers |
| Delta Lake Change Data Feed | Row-level change stream from the source tables |
| Lakeflow Declarative Pipelines (AUTO CDC) | Applies changes to Bronze streaming tables |
| Databricks Asset Bundles | Job + pipeline defined and deployed as code |
| dbt-core / dbt-databricks | Silver / snapshots / Gold, tests, freshness |
| dbt_utils | Generic tests (`mutually_exclusive_ranges`, `expression_is_true`) |
| Apache Airflow 3.2 (CeleryExecutor) | Daily orchestration in Docker Compose |
| databricks-sdk | Triggers and waits for the Databricks job from Airflow |

**Project Structure**

```
walmart-dbt-databricks-warehouse/
├── databricks/                         -- Asset Bundle: Bronze ingestion
│   ├── databricks.yml
│   ├── resources/
│   │   ├── cdc_ingestion.job.yml       -- setup_source → generate_changes → bronze_cdc
│   │   └── bronze_cdc.pipeline.yml
│   ├── notebooks/
│   │   ├── _config.py
│   │   ├── 00_setup_source.py
│   │   └── 01_generate_changes.py
│   └── pipelines/
│       └── bronze_cdc.py               -- CDF → AUTO CDC → bronze streaming tables
├── walmart_project/                    -- dbt project (mounted into Airflow)
│   ├── models/
│   │   ├── source/sources.yml          -- Bronze + freshness
│   │   ├── silver/                     -- cleaned models + tests
│   │   └── gold/                       -- SCD2 dims, fact, obt + tests
│   ├── snapshots/                      -- SCD2 history
│   ├── macros/                         -- incremental_window, scd2_validity, generate_schema_name
│   ├── tests/                          -- singular tests
│   ├── analyses/                       -- data profiling queries
│   └── profiles.yml.example
└── airflow/
    ├── dags/orchestrate.py
    ├── docker-compose.yaml
    └── Dockerfile
```

**Future Improvements**
- `dim_date` (the `order_date` key already exists in Silver)
- CI: `dbt parse` / `dbt build` on pull requests
- Alerting on Airflow task failure and freshness warnings
- A BI layer on top of the OBT

</details>

---

*Demonstrates CDC with Lakeflow AUTO CDC on Delta Change Data Feed → dbt Silver/Gold on Databricks → point-in-time SCD Type 2 → layered data quality testing → daily Airflow orchestration, with ingestion and orchestration fully defined as code.*
