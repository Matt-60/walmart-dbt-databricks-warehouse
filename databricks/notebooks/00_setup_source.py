# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup source
# MAGIC Creates `walmart.source.*` — Delta tables with **Change Data Feed** that replace the original PostgreSQL source
# MAGIC (hosted on Ghost, decommissioned). Seeded once from the current Bronze data.
# MAGIC
# MAGIC Idempotent: tables that already exist are left untouched, so it is safe as the first task of every run.
# MAGIC (On a brand-new workspace there is no Bronze to seed from — this step only matters for the migration.)

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SOURCE}")

for t, (key, cols) in TABLES.items():
    if spark.catalog.tableExists(src(t)):
        print(f"{src(t)} exists — skipped")
        continue
    spark.sql(f"""
        CREATE TABLE {src(t)}
        USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
        AS SELECT {', '.join(cols)} FROM {brz(t)}
    """)
    print(f"{src(t)} created from {brz(t)}")
