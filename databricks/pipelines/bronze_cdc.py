# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze · AUTO CDC from the source Change Data Feed
# MAGIC Lakeflow Declarative Pipeline that keeps `walmart.bronze.bronze_<table>` in sync with `walmart.source.<table>`.
# MAGIC
# MAGIC - Reads each source table's **Change Data Feed** as a stream. On the first run (or a full refresh) the stream
# MAGIC   starts with a snapshot of the whole table, then continues with inserts, updates and deletes.
# MAGIC - `apply_changes` (AUTO CDC) upserts by primary key, orders changes by commit version and applies deletes —
# MAGIC   the pipeline checkpoint remembers what was already processed, so no manual watermark table is needed.
# MAGIC - Targets are SCD Type 1 streaming tables (current state); history is kept downstream by dbt snapshots.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "walmart")

# source table -> primary key
TABLES = {
    "stores": "store_id",
    "customers": "customer_id",
    "employees": "employee_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}

CDF_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]


def define_bronze_table(table, key):

    @dlt.view(name=f"{table}_changes", comment=f"Change Data Feed of {CATALOG}.source.{table}")
    def changes():
        return (
            spark.readStream
            .option("readChangeFeed", "true")
            .table(f"{CATALOG}.source.{table}")
            .where("_change_type != 'update_preimage'")   # keep only the new image of an update
        )

    dlt.create_streaming_table(
        name=f"bronze_{table}",
        comment=f"Current state of source.{table}, maintained by AUTO CDC",
    )

    dlt.apply_changes(                      # a.k.a. create_auto_cdc_flow in newer releases
        target=f"bronze_{table}",
        source=f"{table}_changes",
        keys=[key],
        sequence_by=F.col("_commit_version"),
        apply_as_deletes=F.expr("_change_type = 'delete'"),
        except_column_list=CDF_COLUMNS,
        stored_as_scd_type=1,
    )


for table, key in TABLES.items():
    define_bronze_table(table, key)
