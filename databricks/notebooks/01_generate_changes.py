# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate changes
# MAGIC Simulates one day of activity in the store system, writing to `walmart.source.*`:
# MAGIC
# MAGIC | Change | Table | Exercises downstream |
# MAGIC |---|---|---|
# MAGIC | Pending orders → Completed / Cancelled, some Completed → Returned | orders | incremental Silver (updates), `orders_snapshot` |
# MAGIC | New customers | customers | new dimension members |
# MAGIC | Customers moving to another city | customers | SCD2 in `customers_snapshot` |
# MAGIC | Price changes | products | SCD2 in `products_snapshot` |
# MAGIC | Salary raises | employees | SCD2 in `employees_snapshot` |
# MAGIC | New orders with 1–4 line items (Pending) | orders, order_items | incremental Silver + Fact (inserts) |
# MAGIC
# MAGIC Generated values respect the dbt tests (accepted values, positive amounts, referential integrity,
# MAGIC `total_amount = SUM(line_amount)`). Every changed row gets `updated_timestamp = now`, so the dbt watermarks pick it up.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import Row, functions as F

dbutils.widgets.text("new_orders", "50")
dbutils.widgets.text("new_customers", "3")
dbutils.widgets.text("seed", "")

N_ORDERS = int(dbutils.widgets.get("new_orders"))
N_CUSTOMERS = int(dbutils.widgets.get("new_customers"))
SEED = dbutils.widgets.get("seed")
if SEED:
    random.seed(int(SEED))

NOW = datetime.now()

# must match accepted_values in models/silver/properties.yml
PAYMENT_METHODS = ["Cash", "Debit Card", "Credit Card", "Gift Card", "Online"]

FIRST_NAMES = ["Olivia", "Liam", "Emma", "Noah", "Ava", "Lucas", "Mia", "Ethan", "Chloe", "Jacob", "Sofia", "Owen"]
LAST_NAMES = ["Tremblay", "Gagnon", "Roy", "Smith", "Brown", "Wilson", "Martin", "Lee", "Taylor", "Campbell"]

# COMMAND ----------

# Helpers

def active_value(t):
    """Value used for active rows in this table (bool, int or string — whatever the source used)."""
    return spark.sql(f"""
        SELECT is_active FROM {src(t)}
        WHERE is_active IS NOT NULL
        GROUP BY is_active ORDER BY COUNT(*) DESC LIMIT 1
    """).first()[0]


def next_id(t):
    key = TABLES[t][0]
    return (spark.table(src(t)).agg(F.max(key)).first()[0] or 0) + 1


def upsert(t, rows):
    """MERGE a list of dicts into a source table, casting every column to the table's type.
    Rows only need the key plus the columns that change."""
    if not rows:
        return 0
    key = TABLES[t][0]
    types = {f.name: f.dataType for f in spark.table(src(t)).schema}
    df = spark.createDataFrame([Row(**r) for r in rows])
    cols = list(rows[0].keys())
    df = df.select([F.col(c).cast(types[c]).alias(c) for c in cols])
    df.createOrReplaceTempView("_generated")

    set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols if c != key)
    spark.sql(f"""
        MERGE INTO {src(t)} t
        USING _generated s
        ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})
    """)
    return len(rows)


def sample(t, n, condition="true", columns=None):
    """n random rows from a source table."""
    columns = columns or [TABLES[t][0]]
    return (spark.table(src(t)).where(condition)
            .orderBy(F.rand()).limit(n).select(*columns).collect())


ACTIVE = {t: active_value(t) for t in ["customers", "stores", "products", "orders", "order_items"]}
summary = {}

# COMMAND ----------

# 1. Order lifecycle — existing Pending orders get closed, a few Completed ones are returned

pending = sample("orders", max(1, int(N_ORDERS * 0.8)), "order_status = 'Pending'")
summary["orders: Pending → Completed/Cancelled"] = upsert("orders", [
    {"order_id": r.order_id,
     "order_status": random.choices(["Completed", "Cancelled"], weights=[85, 15])[0],
     "updated_timestamp": NOW}
    for r in pending
])

returned = sample("orders", max(1, int(N_ORDERS * 0.05)), "order_status = 'Completed'")
summary["orders: Completed → Returned"] = upsert("orders", [
    {"order_id": r.order_id, "order_status": "Returned", "updated_timestamp": NOW}
    for r in returned
])

# COMMAND ----------

# 2. New customers, in locations that already exist in the data

locations = [tuple(r) for r in spark.table(src("customers"))
             .select("city", "province", "country").distinct().collect()]

first_id = next_id("customers")
new_customers = []
for i in range(N_CUSTOMERS):
    cid = first_id + i
    fn, ln = random.choice(FIRST_NAMES), random.choice(LAST_NAMES)
    city, province, country = random.choice(locations)
    new_customers.append({
        "customer_id": cid,
        "first_name": fn,
        "last_name": ln,
        "email": f"{fn}.{ln}{cid}@example.com".lower(),
        "phone": f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
        "city": city,
        "province": province,
        "country": country,
        "is_active": ACTIVE["customers"],
        "created_timestamp": NOW,
        "updated_timestamp": NOW,
    })
summary["customers: new"] = upsert("customers", new_customers)

# COMMAND ----------

# 3. Slowly changing attributes → new SCD2 versions in the dbt snapshots

moved = []
for r in sample("customers", 2, columns=["customer_id", "city"]):
    city, province, country = random.choice([l for l in locations if l[0] != r.city] or locations)
    moved.append({"customer_id": r.customer_id, "city": city, "province": province,
                  "country": country, "updated_timestamp": NOW})
summary["customers: moved (SCD2)"] = upsert("customers", moved)

summary["products: price change (SCD2)"] = upsert("products", [
    {"product_id": r.product_id,
     "price": round(float(r.price) * random.uniform(0.9, 1.1), 2),
     "updated_timestamp": NOW}
    for r in sample("products", 3, columns=["product_id", "price"])
])

summary["employees: salary raise (SCD2)"] = upsert("employees", [
    {"employee_id": r.employee_id,
     "salary": round(float(r.salary) * 1.05, 2),
     "updated_timestamp": NOW}
    for r in sample("employees", 2, columns=["employee_id", "salary"])
])

# COMMAND ----------

# 4. New orders with line items (inserted as Pending — closed by step 1 on a later run)

customer_ids = [r[0] for r in spark.table(src("customers"))
                .where(F.col("is_active") == ACTIVE["customers"]).select("customer_id").collect()]
store_ids = [r[0] for r in spark.table(src("stores"))
             .where(F.col("is_active") == ACTIVE["stores"]).select("store_id").collect()]
products = [(r.product_id, float(r.price)) for r in spark.table(src("products"))
            .where((F.col("is_active") == ACTIVE["products"]) & (F.col("price") > 0))
            .select("product_id", "price").collect()]

order_id, item_id = next_id("orders"), next_id("order_items")
orders, items = [], []

for _ in range(N_ORDERS):
    ordered_at = NOW - timedelta(minutes=random.randint(0, 24 * 60))
    total = 0.0
    for product_id, price in random.sample(products, k=min(len(products), random.randint(1, 4))):
        qty = random.randint(1, 5)
        line = round(qty * price, 2)
        total += line
        items.append({
            "order_item_id": item_id, "order_id": order_id, "product_id": product_id,
            "quantity": qty, "unit_price": price, "line_amount": line,
            "is_active": ACTIVE["order_items"],
            "created_timestamp": ordered_at, "updated_timestamp": NOW,
        })
        item_id += 1
    orders.append({
        "order_id": order_id,
        "customer_id": random.choice(customer_ids),
        "store_id": random.choice(store_ids),
        "order_timestamp": ordered_at,
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": "Pending",
        "total_amount": round(total, 2),
        "is_active": ACTIVE["orders"],
        "created_timestamp": ordered_at,
        "updated_timestamp": NOW,   # always "now", so the dbt watermark never skips it
    })
    order_id += 1

summary["orders: new"] = upsert("orders", orders)
summary["order_items: new"] = upsert("order_items", items)

# COMMAND ----------

display(spark.createDataFrame(list(summary.items()), "change STRING, rows INT"))
