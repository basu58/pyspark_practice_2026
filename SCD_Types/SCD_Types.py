# Databricks notebook source
# DBTITLE 1,SCD Types Overview
# MAGIC %md
# MAGIC # Slowly Changing Dimension (SCD) Types 1, 2 & 3
# MAGIC
# MAGIC In data warehousing, **Slowly Changing Dimensions (SCDs)** handle changes to dimensional attribute values over time. A classic example is a **Customer Dimension** where a customer's `city` or `department` changes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Summary Table
# MAGIC
# MAGIC | SCD Type | How It Handles Change | Keeps History? | Typical Use Case |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **Type 1** | Overwrite the old value in place | ❌ No | Data corrections, non-critical attributes |
# MAGIC | **Type 2** | Insert a new row; expire the old row with end-date + active flag | ✅ Full history | Compliance, audit, trend analysis |
# MAGIC | **Type 3** | Add `previous_value` + `effective_date` columns alongside current | ✅ One prior version | Limited history, before/after comparison |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC This notebook demonstrates all three SCD types using **PySpark** on a **Customer Dimension** table.

# COMMAND ----------

# DBTITLE 1,SCD Type 1 - Explanation
# MAGIC %md
# MAGIC ## SCD Type 1 — Overwrite (No History)
# MAGIC
# MAGIC **What it does:** When an attribute changes, the old value is simply **overwritten** with the new value. No history is preserved.
# MAGIC
# MAGIC **Best for:** Corrections, non-critical attributes where history doesn't matter (e.g., fixing a typo in a name, updating a phone number).
# MAGIC
# MAGIC **Implementation:** `UPDATE` the existing row (in Spark, use a `MERGE` or left-anti join + union).
# MAGIC
# MAGIC ### Interview Questions
# MAGIC 1. *When would you choose SCD Type 1 over Type 2?*
# MAGIC    → When the business doesn't need historical tracking — e.g., data corrections, typos, or attributes like `phone_number` where only the current value matters.
# MAGIC 2. *How do you implement SCD Type 1 in Spark without DELETE/UPDATE?*
# MAGIC    → Use a left-anti join to keep unchanged rows, union with the updated rows — Spark DataFrames are immutable, so you rebuild the table.
# MAGIC 3. *What are the downsides of SCD Type 1?*
# MAGIC    → No audit trail; you can't answer "what was the customer's city last year?"

# COMMAND ----------

# DBTITLE 1,SCD Type 1 - Code
from pyspark.sql.functions import col, current_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ── Sample Data: Customer Dimension (Day 1) ──
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name",         StringType(), True),
    StructField("city",         StringType(), True),
    StructField("department",   StringType(), True),
])

customers_data = [
    (1, "Alice",   "New York",   "Sales"),
    (2, "Bob",     "Boston",     "Marketing"),
    (3, "Charlie", "Chicago",   "Finance"),
]

dim_customer = spark.createDataFrame(customers_data, customer_schema)
print("=== BEFORE (Day 1) ===")
dim_customer.show()



# COMMAND ----------

# ── Incoming Updates (Day 2): Alice moves to San Francisco, Bob moves to Sales ──
updates_data = [
    (1, "Alice",   "San Francisco", "Sales"),
    (2, "Bob",     "Boston",         "Sales"),   # department changed
]
dim_updates = spark.createDataFrame(updates_data, customer_schema)
print("=== INCOMING UPDATES (Day 2) ===")
dim_updates.show()

# COMMAND ----------

# ── SCD Type 1: Overwrite using MERGE ──
# Spark supports MERGE via Delta tables. For a pure DataFrame demo,
# we simulate the overwrite with left-anti join + union.

# Step 1: Keep rows that are NOT in the updates (unchanged customers)
unchanged = dim_customer.join(
    dim_updates.select("customer_id"),
    on="customer_id",
    how="left_anti"
)

unchanged.show()
# Step 2: Take the updated rows as-is (they already contain the new values)
updated = dim_updates

# Step 3: Union them → final SCD Type 1 table
dim_customer_scd1 = unchanged.unionByName(updated)

print("=== AFTER SCD Type 1 (Overwrite — no history) ===")
dim_customer_scd1.orderBy("customer_id").show()

# COMMAND ----------

# DBTITLE 1,SCD Type 2 - Explanation
# MAGIC %md
# MAGIC ## SCD Type 2 — Add New Row (Full History)
# MAGIC
# MAGIC **What it does:** When an attribute changes, a **new row is inserted** for the customer with the new values. The old row is **expired** by setting `end_date` and `is_active = false`. This preserves the **complete history** of changes.
# MAGIC
# MAGIC **Key columns added to the dimension table:**
# MAGIC - `effective_start_date` — when this version became current
# MAGIC - `effective_end_date` — when this version expired (`9999-12-31` for the active row)
# MAGIC - `is_active` — boolean flag (`true` for the current version, `false` for old versions)
# MAGIC
# MAGIC **Best for:** Compliance/audit requirements, trend analysis, reporting that needs point-in-time accuracy.
# MAGIC
# MAGIC ### Interview Questions
# MAGIC 1. *How do you identify the current record in an SCD Type 2 table?*
# MAGIC    → `WHERE is_active = true` or `WHERE effective_end_date = '9999-12-31'`.
# MAGIC 2. *Why do we need both `start_date` and `end_date`?*
# MAGIC    → To support point-in-time queries: "what was the customer's department on July 1st?" → `WHERE start_date <= '2024-07-01' AND end_date > '2024-07-01'`.
# MAGIC 3. *How do you implement SCD Type 2 in Spark without Delta MERGE?*
# MAGIC    → Expire changed rows (set end_date + is_active=false), then union with new rows (start_date=today, end_date=9999-12-31, is_active=true).
# MAGIC 4. *What's the downside of SCD Type 2?*
# MAGIC    → Table grows rapidly with every change; more complex queries (must always filter to active rows).
# MAGIC 5. *How does SCD Type 2 affect joins with fact tables?*
# MAGIC    → Fact tables join on the **surrogate key** (a unique row ID per version), not the natural key — otherwise you'd get multiple matches.

# COMMAND ----------

# DBTITLE 1,SCD Type 2 - Code
from pyspark.sql.functions import col, lit, when, to_date, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType

# ── SCD Type 2 Sample Data: Customer Dimension (Day 1) ──
# Each row has effective_start_date, effective_end_date, is_active

scd2_schema = StructType([
    StructField("surrogate_key",    IntegerType(), True),
    StructField("customer_id",       IntegerType(), True),
    StructField("name",               StringType(), True),
    StructField("city",               StringType(), True),
    StructField("department",         StringType(), True),
    StructField("effective_start_date", StringType(), True),  # using string dates for simplicity
    StructField("effective_end_date",   StringType(), True),
    StructField("is_active",           BooleanType(), True),
])

scd2_data = [
    (1, 1, "Alice",   "New York", "Sales",     "2024-01-01", "9999-12-31", True),
    (2, 2, "Bob",     "Boston",   "Marketing", "2024-01-01", "9999-12-31", True),
    (3, 3, "Charlie", "Chicago",  "Finance",   "2024-01-01", "9999-12-31", True),
]

dim_customer_scd2 = spark.createDataFrame(scd2_data, scd2_schema)
print("=== BEFORE (Day 1) — SCD Type 2 ===")
dim_customer_scd2.show()

# COMMAND ----------


# ── Incoming Updates (Day 2: 2024-06-01): Alice moves to San Francisco ──
scd2_updates_data = [
    (1, "Alice", "San Francisco", "Sales"),   # city changed
]
scd2_updates_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name",         StringType(), True),
    StructField("city",         StringType(), True),
    StructField("department",   StringType(), True),
])
dim_updates_scd2 = spark.createDataFrame(scd2_updates_data, scd2_updates_schema)
print("=== INCOMING UPDATES (Day 2) ===")
dim_updates_scd2.show()

# COMMAND ----------

# ── SCD Type 2 Implementation ──

# Step 1: Identify rows that changed (join on customer_id, compare city or department)
changed_ids = dim_customer_scd2.alias("old").join(
    dim_updates_scd2.alias("new"),
    on="customer_id",
    how="inner"
).filter(
    (col("old.city") != col("new.city")) |
    (col("old.department") != col("new.department")) |
    (col("old.name") != col("new.name"))
).select("customer_id")

changed_ids.show()



# COMMAND ----------

# Step 2: Expire old rows for changed customers
expired_rows = dim_customer_scd2.join(
    changed_ids, on="customer_id", how="inner"
).withColumn("effective_end_date", lit("2024-06-01")) \
 .withColumn("is_active", lit(False))

expired_rows.show()

# COMMAND ----------

# Step 3: Keep unchanged rows as-is
unchanged_rows = dim_customer_scd2.join(
    changed_ids, on="customer_id", how="left_anti"
)
unchanged_rows.show()

# COMMAND ----------


# Step 4: Create new rows for changed customers with new values
max_sk = dim_customer_scd2.agg({"surrogate_key": "max"}).collect()[0][0]
new_rows = dim_updates_scd2.join(
    changed_ids, on="customer_id", how="inner"
).withColumn("surrogate_key", lit(max_sk + 1)) \
 .withColumn("effective_start_date", lit("2024-06-01")) \
 .withColumn("effective_end_date", lit("9999-12-31")) \
 .withColumn("is_active", lit(True)) \
 .select("surrogate_key", "customer_id", "name", "city", "department",
         "effective_start_date", "effective_end_date", "is_active")

# Step 5: Union all three parts → final SCD Type 2 table
final_scd2 = unchanged_rows.unionByName(expired_rows).unionByName(new_rows)

print("=== AFTER SCD Type 2 (Full History) ===")
final_scd2.orderBy("customer_id", "effective_start_date").show()

# ── Point-in-Time Query: What was Alice's city on 2024-03-15? ──
print("=== Point-in-Time Query: Alice on 2024-03-15 ===")
final_scd2.filter(
    (col("customer_id") == 1) &
    (col("effective_start_date") <= "2024-03-15") &
    (col("effective_end_date") > "2024-03-15")
).show()

print("=== Current Active Records Only ===")
final_scd2.filter(col("is_active") == True).orderBy("customer_id").show()

# COMMAND ----------

# DBTITLE 1,SCD Type 3 - Explanation
# MAGIC %md
# MAGIC ## SCD Type 3 — Add New Columns (Limited History)
# MAGIC
# MAGIC **What it does:** Instead of adding new rows (Type 2), Type 3 adds **new columns** to the existing row to store the **previous value** and an **effective date**. Typically only one level of history is kept (current + previous).
# MAGIC
# MAGIC **Key columns added to the dimension table:**
# MAGIC - `current_city` — the current value
# MAGIC - `previous_city` — the prior value
# MAGIC - `city_effective_date` — when the current value took effect
# MAGIC
# MAGIC **Best for:** When you only need to compare **before vs. after** — e.g., "what department was this employee in before their transfer?"
# MAGIC
# MAGIC ### Interview Questions
# MAGIC 1. *What's the main difference between SCD Type 2 and Type 3?*
# MAGIC    → Type 2 adds **rows** (unlimited history); Type 3 adds **columns** (only one prior value).
# MAGIC 2. *When would you choose Type 3 over Type 2?*
# MAGIC    → When you only need the previous value for comparison (e.g., before/after a department transfer), not full history. It's simpler and keeps the table small.
# MAGIC 3. *What's the limitation of SCD Type 3?*
# MAGIC    → Only stores **one** previous value. If a customer changes city three times, the first two are lost.
# MAGIC 4. *How does SCD Type 3 impact schema evolution?*
# MAGIC    → Every time you want to track a new attribute's history, you must add more columns (`previous_X`, `X_effective_date`), which can bloat the schema.

# COMMAND ----------

# DBTITLE 1,SCD Type 3 - Code
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ── SCD Type 3 Sample Data: Customer Dimension (Day 1) ──
# We track city history with current_city, previous_city, city_effective_date

scd3_schema = StructType([
    StructField("customer_id",           IntegerType(), True),
    StructField("name",                   StringType(), True),
    StructField("current_city",           StringType(), True),
    StructField("previous_city",          StringType(), True),    # null for first load
    StructField("city_effective_date",   StringType(), True),    # when current_city took effect
    StructField("department",             StringType(), True),
])

scd3_data = [
    (1, "Alice",   "New York",  None,          "2024-01-01", "Sales"),
    (2, "Bob",     "Boston",    None,          "2024-01-01", "Marketing"),
    (3, "Charlie", "Chicago",   None,          "2024-01-01", "Finance"),
]

dim_customer_scd3 = spark.createDataFrame(scd3_data, scd3_schema)
print("=== BEFORE (Day 1) — SCD Type 3 ===")
dim_customer_scd3.show()

# ── Incoming Updates (Day 2: 2024-06-01): Alice moves to San Francisco, Charlie moves to Houston ──
scd3_updates_data = [
    (1, "Alice",   "San Francisco", "Sales"),
    (3, "Charlie", "Houston",       "Finance"),
]
scd3_updates_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name",         StringType(), True),
    StructField("new_city",     StringType(), True),
    StructField("department",   StringType(), True),
])
dim_updates_scd3 = spark.createDataFrame(scd3_updates_data, scd3_updates_schema)
print("=== INCOMING UPDATES (Day 2) ===")
dim_updates_scd3.show()

# ── SCD Type 3 Implementation ──
# For each customer in updates:
#   previous_city = current_city (old value moves to previous)
#   current_city  = new_city
#   city_effective_date = 2024-06-01

# Join existing dimension with updates on customer_id
joined = dim_customer_scd3.alias("old").join(
    dim_updates_scd3.alias("new"),
    on="customer_id",
    how="left"
)

joined.show()

final_scd3 = joined.select(
    col("customer_id"),
    col("old.name"),
    # If there's an update, new city becomes current; old city becomes previous
    when(col("new.new_city").isNotNull(), col("new.new_city"))
        .otherwise(col("old.current_city")).alias("current_city"),
    when(col("new.new_city").isNotNull(), col("old.current_city"))
        .otherwise(col("old.previous_city")).alias("previous_city"),
    when(col("new.new_city").isNotNull(), lit("2024-06-01"))
        .otherwise(col("old.city_effective_date")).alias("city_effective_date"),
    when(col("new.department").isNotNull(), col("new.department"))
        .otherwise(col("old.department")).alias("department"),
)

print("=== AFTER SCD Type 3 (Limited History — previous + current) ===")
final_scd3.orderBy("customer_id").show()

# ── Before/After Comparison Query ──
print("=== Before/After Comparison: Customers who changed city ===")
final_scd3.filter(col("previous_city").isNotNull()) \
    .select("customer_id", "name", "previous_city", "current_city", "city_effective_date") \
    .show()