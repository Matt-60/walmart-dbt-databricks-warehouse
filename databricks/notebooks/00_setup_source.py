# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup source
# MAGIC Creates `walmart.source.*` — Delta tables with **Change Data Feed** that replace the original PostgreSQL source
# MAGIC (hosted on Ghost, decommissioned). Seeded once from the current Bronze data.
# MAGIC
# MAGIC Idempotent: tables and checkpoints that already exist are left untouched, so it is safe as the first task of every run.

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

# COMMAND ----------

# Checkpoint = last source version already present in Bronze.
# The seed (CTAS) is already in Bronze, so the first ingest starts after it.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CDC_LOG} (
        table_name    STRING,
        last_version  BIGINT,
        rows_ingested BIGINT,
        ingested_at   TIMESTAMP
    ) USING DELTA
""")

for t in TABLES:
    has_checkpoint = spark.sql(f"SELECT 1 FROM {CDC_LOG} WHERE table_name = '{t}'").count() > 0
    if not has_checkpoint:
        v = latest_version(src(t))
        spark.sql(f"INSERT INTO {CDC_LOG} VALUES ('{t}', {v}, 0, current_timestamp())")
        print(f"checkpoint {t} = version {v}")
