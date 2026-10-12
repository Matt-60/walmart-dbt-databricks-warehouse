# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ingest CDC → Bronze
# MAGIC For every source table: read the **Change Data Feed** since the last ingested version, keep the latest change per key,
# MAGIC and `MERGE` it into `walmart.bronze.bronze_<table>` (inserts, updates and hard deletes).
# MAGIC
# MAGIC - Versions are read as a closed range `(last_version, current_version]`, so writes that land during the run go to the next one.
# MAGIC - The checkpoint is written after the MERGE. If the run fails in between, the same range is re-read and the MERGE converges to
# MAGIC   the same result — the ingest is idempotent.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import Window, functions as F

results = []

for t, (key, cols) in TABLES.items():
    last_v = spark.sql(f"SELECT MAX(last_version) FROM {CDC_LOG} WHERE table_name = '{t}'").first()[0]
    if last_v is None:
        raise Exception(f"No checkpoint for '{t}' — run 00_setup_source first")

    current_v = latest_version(src(t))
    if current_v <= last_v:
        results.append((t, last_v, current_v, 0))
        continue

    changes = (spark.read.format("delta")
               .option("readChangeFeed", "true")
               .option("startingVersion", last_v + 1)
               .option("endingVersion", current_v)
               .table(src(t))
               .where(F.col("_change_type").isin("insert", "update_postimage", "delete")))

    # several changes to one key in the range → keep only the newest
    newest_first = Window.partitionBy(key).orderBy(F.col("_commit_version").desc())
    latest = (changes.withColumn("_rn", F.row_number().over(newest_first))
              .where("_rn = 1").drop("_rn"))
    latest.createOrReplaceTempView("_cdc_changes")
    n = latest.count()

    spark.sql(f"""
        MERGE INTO {brz(t)} b
        USING _cdc_changes s
        ON b.{key} = s.{key}
        WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
        WHEN MATCHED THEN UPDATE SET {', '.join(f'b.{c} = s.{c}' for c in cols if c != key)}
        WHEN NOT MATCHED AND s._change_type != 'delete' THEN
            INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})
    """)

    spark.sql(f"INSERT INTO {CDC_LOG} VALUES ('{t}', {current_v}, {n}, current_timestamp())")
    results.append((t, last_v, current_v, n))

# COMMAND ----------

display(spark.createDataFrame(results, "table STRING, from_version BIGINT, to_version BIGINT, rows_merged BIGINT"))
