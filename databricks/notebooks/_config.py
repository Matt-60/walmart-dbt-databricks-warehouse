# Databricks notebook source
# MAGIC %md
# MAGIC # Shared config
# MAGIC Table definitions and helpers used by `00_setup_source` and `01_generate_changes` (loaded with `%run ./_config`).

# COMMAND ----------

dbutils.widgets.text("catalog", "walmart")
CATALOG = dbutils.widgets.get("catalog")

SOURCE = f"{CATALOG}.source"     # simulated OLTP system (Delta + Change Data Feed)
BRONZE = f"{CATALOG}.bronze"     # raw layer read by dbt

# table -> (primary key, business columns)
TABLES = {
    "stores": ("store_id", [
        "store_id", "store_name", "city", "province", "country",
        "is_active", "created_timestamp", "updated_timestamp"]),
    "customers": ("customer_id", [
        "customer_id", "first_name", "last_name", "email", "phone", "city", "province", "country",
        "is_active", "created_timestamp", "updated_timestamp"]),
    "employees": ("employee_id", [
        "employee_id", "store_id", "first_name", "last_name", "email", "job_title", "salary",
        "is_active", "created_timestamp", "updated_timestamp"]),
    "products": ("product_id", [
        "product_id", "product_name", "category", "brand", "price",
        "is_active", "created_timestamp", "updated_timestamp"]),
    "orders": ("order_id", [
        "order_id", "customer_id", "store_id", "order_timestamp", "payment_method", "order_status",
        "total_amount", "is_active", "created_timestamp", "updated_timestamp"]),
    "order_items": ("order_item_id", [
        "order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_amount",
        "is_active", "created_timestamp", "updated_timestamp"]),
}


def src(t):
    return f"{SOURCE}.{t}"


def brz(t):
    return f"{BRONZE}.bronze_{t}"
