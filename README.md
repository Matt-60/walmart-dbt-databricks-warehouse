# Airbnb Analytics Data Warehouse

An end-to-end ELT warehouse for Airbnb booking analytics — an **Azure SQL** source replicated into **Snowflake** by **Fivetran** (Change Tracking), modeled with **dbt** into a star schema with a **point-in-time SCD Type 2** listing dimension, tested at every layer and run daily by a **dbt Cloud** job.

`Azure SQL Database` · `Fivetran` · `Snowflake` · `dbt Cloud` · `SCD Type 2` · `Star Schema` · `Medallion Architecture` · `Key-pair auth`

---

## 🎯 Business Goal

An Airbnb-style business needs to understand which listings and hosts drive revenue, how pricing changes over time, and how booking behavior evolves — while trusting that a listing's past price or attributes aren't silently overwritten. Every booking is reported against the listing **as it was on the booking date**, so a price change today doesn't rewrite last month's revenue by price tier.

## 🏗️ Architecture

```
Azure SQL Database  (bookings, listings, hosts — Change Tracking enabled)
        │  Fivetran — Azure SQL connector, Change Tracking, daily sync
        ▼
Snowflake  AIRBNB.AZURE_SQL_DB_DBO      raw layer: current state per key + _FIVETRAN_SYNCED / _FIVETRAN_DELETED
        │  dbt source freshness
        ▼
dbt Silver      typed + cleaned, incremental MERGE, soft deletes kept as IS_DELETED     ── tests gate
        ▼
dbt snapshot    listings_snapshot — SCD Type 2 history
        ▼
dbt Gold        dim_listings (SCD2) · dim_hosts · dim_date → fact_bookings (point-in-time join)
```

| Layer | Where it lives | Purpose |
|---|---|---|
| **Source** | Azure SQL Database (serverless, free offer) | Operational tables `bookings`, `listings`, `hosts` with primary keys and Change Tracking |
| **Raw** | `AIRBNB.AZURE_SQL_DB_DBO`, written by Fivetran | Current state per primary key; rows deleted at the source are flagged `_FIVETRAN_DELETED`. This schema *is* the Bronze layer — no separate Bronze models needed |
| **Silver** | dbt, `AIRBNB.SILVER` | Typed and cleaned models, incremental `MERGE` on `_FIVETRAN_SYNCED`; `silver_obt` joins all three (grain = 1 booking) |
| **Snapshots** | dbt, `AIRBNB.SNAPSHOTS` | `listings_snapshot` — SCD Type 2 history of listing attributes and prices |
| **Gold** | dbt, `AIRBNB.GOLD` | Star schema: `fact_bookings`, `dim_listings` (SCD2), `dim_hosts` (SCD1), `dim_date` |

<b>🔍 dbt lineage graph</b>

<img width="1242" height="296" alt="dbt lineage" src="https://github.com/user-attachments/assets/118a752e-c6f5-42eb-9ab5-c7f06b5a0cc1" />

## 🔄 Ingestion — Fivetran with Change Tracking

The source tables live in **Azure SQL Database** with SQL Server **Change Tracking** enabled, so the database itself records which rows were inserted, updated or deleted:

```sql
ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
ALTER TABLE dbo.bookings ENABLE CHANGE_TRACKING;   -- likewise listings, hosts
```

A **Fivetran** Azure SQL connector reads those changes once a day and merges them into Snowflake by primary key. Only changed rows move — no full-file reloads — and deletes arrive as a soft-delete flag (`_FIVETRAN_DELETED`) instead of disappearing silently.

| Setting | Value | Why |
|---|---|---|
| Update method | Change Tracking | Row-level inserts / updates / deletes, no timestamp cursor |
| Sync frequency | 24 h | Keeps the serverless database inside the Azure free offer (it auto-pauses between syncs) |
| Destination | Snowflake native tables, schema `AZURE_SQL_DB_DBO` | Read by dbt as the `fivetran` source |
| Auth (Snowflake) | `FIVETRAN_USER`, key pair, `FIVETRAN_ROLE` | Snowflake is retiring single-factor password auth for service users |

<details>
<summary><b>📜 v1 ingestion: ADLS Gen2 + COPY INTO (click to expand)</b></summary>

The first version loaded CSV files from Azure Data Lake Gen2 through a Snowflake **storage integration** and external stage, as a full snapshot on every run (`TRUNCATE` + `COPY INTO ... FORCE = TRUE`), automated hourly with a Snowflake `TASK`. The full script is kept in [`sql/azure_snowflake_ingestion.sql`](./sql/azure_snowflake_ingestion.sql).

**Why `FORCE = TRUE`:** Snowflake tracks per-table file load history (name + checksum) for ~64 days. Without `FORCE`, a file already recorded as loaded is silently skipped, even after the table is truncated.

**Why it was replaced:** `COPY INTO` works at file granularity — changing one row in a 200-row file reloads all 200 rows, so dbt needed an append-only Bronze layer with deduplication (`QUALIFY ROW_NUMBER()`) before every `MERGE`, and deletes in the source were never visible. Moving the source into a database with Change Tracking and letting Fivetran apply row-level changes removed the Bronze layer, the deduplication and the truncate/reload cycle.

</details>

## 🥈 Silver — Incremental, Soft Deletes Kept

Fivetran already keeps one row per primary key, so Silver doesn't deduplicate — it types, cleans and merges incrementally on the time Fivetran last changed a row:

```sql
{{ config(materialized='incremental', unique_key='BOOKING_ID', incremental_strategy='merge') }}

SELECT
    BOOKING_ID,
    ...
    _FIVETRAN_DELETED AS IS_DELETED,
    _FIVETRAN_SYNCED
FROM {{ source('fivetran', 'bookings') }}
{% if is_incremental() %}
WHERE {{ incremental_window() }}   -- _FIVETRAN_SYNCED > MAX(...) - 24 hours
{% endif %}
```

- **Lookback + idempotent MERGE** — the [`incremental_window`](macros/incremental_window.sql) macro re-reads the last 24 hours so late or retried syncs aren't skipped; re-reading is safe because the model merges on its key. A date range can be backfilled with `--vars "{backfill_from: '2026-10-01', backfill_to: '2026-10-05'}"`.
- **Soft deletes** — an incremental `MERGE` never removes rows, so deletions are carried as `IS_DELETED` and filtered out before Gold (`silver_obt`).
- **Reusable macros** — `init_cap`, `multiply` (booking totals) and `tag` (Low / Medium / High price band).

## ⭐ Gold — Point-in-Time SCD Type 2

`listings_snapshot` keeps every version of a listing (`strategy='timestamp'`, `hard_deletes='invalidate'` — a listing deleted at the source gets its current version closed). `dim_listings` exposes all versions with a half-open validity range:

| LISTING_ID | VALID_FROM | VALID_TO | PRICE_PER_NIGHT | IS_CURRENT |
|---|---|---|---|---|
| 100 | 1900-01-01 | 2025-06-01 | 120 | false |
| 100 | 2025-06-01 | 9999-12-31 | 150 | true |

*(illustrative example)* — the first version of each listing starts in 1900, so bookings made before the listing was first snapshotted still match a version.

The fact joins each booking to the version valid **on the booking date**:

```sql
LEFT JOIN {{ ref('dim_listings') }} l
    ON  b.LISTING_ID = l.LISTING_ID
    AND b.BOOKING_DATE >= l.VALID_FROM
    AND b.BOOKING_DATE <  l.VALID_TO      -- half-open: exactly one version matches
```

Validity ranges never overlap (tested), so the join adds a `LISTING_KEY` without multiplying bookings.

| Table | Type | Notes |
|---|---|---|
| `fact_bookings` | Fact, 1 row = 1 booking | Measures: booking amount, cleaning / service fees, totals, nights. Keys: `LISTING_KEY` (version at booking date), `HOST_KEY`, `DATE_KEY` |
| `dim_listings` | SCD Type 2, 1 row per version | Price, price band, property and room type, location |
| `dim_hosts` | SCD Type 1 | Superhost status, response rate and quality band; deleted hosts kept with `IS_DELETED` so old bookings keep a valid key |
| `dim_date` | Calendar | Generated from the booking date range |

## ✅ Data Quality

Tests run inside `dbt build`, so a failure stops downstream models.

| Area | Tests |
|---|---|
| Source | `unique` + `not_null` on primary keys; freshness on `_FIVETRAN_SYNCED` (warn after 26 h, error after 50 h) |
| Silver | Unique keys; relationships bookings → listings → hosts; positive nights / prices; accepted values for price band and response-rate quality |
| Grain | `unique` on `BOOKING_ID` in `silver_obt` and `fact_bookings` — any join fan-out fails the build |
| Gold keys | `not_null` + `relationships` for `LISTING_KEY`, `HOST_KEY`, `DATE_KEY` |
| SCD2 | `dbt_utils.mutually_exclusive_ranges` on `dim_listings` (no overlapping versions); exactly one current version per listing |

## ⚙️ Orchestration & Security

A **dbt Cloud deploy job** runs daily on `main`, after the Fivetran sync: `dbt source freshness` → `dbt build` (models, snapshot and tests in dependency order).

Each tool connects to Snowflake as its own **service user with key-pair authentication** and a least-privilege role:

| User | Role | Can do |
|---|---|---|
| `FIVETRAN_USER` | `FIVETRAN_ROLE` | Write the raw schema `AZURE_SQL_DB_DBO` |
| `DBT_USER` | `TRANSFORMER` | Read the raw schema; own and build `SILVER`, `SNAPSHOTS`, `GOLD` |

## 🧠 Key Design Decisions

- **Change Tracking + Fivetran instead of file snapshots** — row-level changes and deletes, no reload of unchanged data, no Bronze deduplication.
- **The Fivetran schema is the raw layer** — a `SELECT *` Bronze model on top would add nothing; history is captured by dbt snapshots.
- **Soft deletes in Silver** — keeps incremental models correct when rows disappear at the source.
- **Point-in-time SCD2 with half-open ranges** — historically accurate revenue by listing attributes, guaranteed one match per booking.
- **Tests gate the pipeline** — grain tests on the OBT and the fact catch join fan-out before it reaches reporting.
- **Separate service users and roles** — ingestion and transformation can't touch each other's objects.

## 🐛 Debugging Notes (real issues hit while building this)

- **File reload ≠ row-level diff (v1).** Changing 1 record + adding 1 record in a 200-row source file reloaded the *entire* 201-row file — Snowflake dedupes `COPY INTO` by file checksum, not row content.
- **`100090: Duplicate row detected during DML action` (v1).** A Silver `MERGE` read an append-only Bronze table holding two versions of the same key. Fixed with `QUALIFY ROW_NUMBER()` — and later made unnecessary by Fivetran's merge-by-key.
- **Fact row count inflated (5001 expected, 5011 actual).** `fact_bookings` joined the SCD2 dimension on `LISTING_ID` only, matching every version of a changed listing. First worked around with a current-version filter, then fixed properly with the point-in-time date-range join.
- **`Database 'airbnb' is not currently available`.** The serverless Azure SQL database auto-pauses when idle; the first connection only wakes it. Fivetran retries automatically, and the daily sync keeps compute inside the free monthly allowance.
- **Snowflake rejected password logins** for Fivetran and dbt ("single-factor password authentication" blocked) → both moved to key-pair service users.
- **`Schema 'GOLD' already exists, but current role has no privileges on it`.** The dbt Cloud job was accidentally connecting as the Fivetran user, while the schemas were owned by the role used in development. Fixed with a dedicated `DBT_USER` / `TRANSFORMER` role owning all dbt schemas.

## ⚠️ Known Limitations

- **Changes are applied manually** in Azure SQL to demonstrate CDC and SCD2; there is no automated change generator.
- **Snapshot change detection uses `CREATED_AT`** (the source bumps it on every update). A source that doesn't would need `strategy='check'`.
- **Development and production share schemas** (`generate_schema_name` writes to `SILVER` / `GOLD` regardless of target) — a deliberate simplification for a single-developer project.

<details>
<summary><b>🛠️ Tech stack & project structure (click to expand)</b></summary>

**Technology Stack**

| Technology | Purpose |
|---|---|
| Azure SQL Database (serverless, free offer) | Operational source with Change Tracking |
| Fivetran | Managed replication Azure SQL → Snowflake |
| Snowflake | Cloud data warehouse |
| dbt Cloud (Fusion) | Transformations, snapshots, tests, scheduled job |
| dbt_utils | Surrogate keys and generic tests |
| SQL / Jinja | Models and macros |

**Project Structure**

```
airbnb-dbt-snowflake-warehouse/
├── models/
│   ├── sources.yml              -- Fivetran source + freshness + source tests
│   ├── silver/                  -- silver_bookings, silver_listings, silver_hosts, silver_obt + tests
│   └── gold/                    -- fact_bookings, dim_listings, dim_hosts, dim_date + tests
├── snapshots/
│   └── listings_snapshot.sql    -- SCD Type 2 history of listings
├── macros/                      -- incremental_window, init_cap, multiply, tag, generate_schema_name
├── tests/                       -- singular tests
├── sql/
│   └── azure_snowflake_ingestion.sql   -- v1 ingestion (ADLS + COPY INTO), kept for reference
└── dbt_project.yml              -- vars: fivetran_schema
```

**Future Improvements**
- A stored procedure in Azure SQL that simulates daily bookings and price changes
- CI on pull requests (`dbt build --select state:modified+`)
- Power BI report on the Gold layer

</details>

---

*Demonstrates managed CDC ingestion (Azure SQL Change Tracking → Fivetran → Snowflake), incremental dbt models with soft deletes, point-in-time SCD Type 2, layered data quality tests and a scheduled dbt Cloud job with least-privilege service users.*
