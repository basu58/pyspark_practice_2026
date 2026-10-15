# Databricks notebook source
# DBTITLE 1,Problem Statement
# MAGIC %md
# MAGIC # 🧩 SQL Interview Question — Hard
# MAGIC
# MAGIC ## **Problem: Customer Retention Cohort Analysis**
# MAGIC
# MAGIC You are given a `purchases` table with the following schema:
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `purchase_id` | int | Unique purchase ID |
# MAGIC | `customer_id` | int | Customer who made the purchase |
# MAGIC | `purchase_date` | date | Date of purchase |
# MAGIC | `amount` | double | Purchase amount in USD |
# MAGIC
# MAGIC **Task:** Build a **cohort retention table** where:
# MAGIC
# MAGIC 1. Each customer is assigned to a **cohort** based on the month of their **first ever purchase** (e.g., `2026-01-01` for any first purchase in Jan 2026).
# MAGIC 2. For each cohort, compute the following for **month indices 0 through 3** (where month 0 = cohort month, month 1 = next month, etc.):
# MAGIC    - `cohort_size` — total number of distinct customers in that cohort
# MAGIC    - `retained_customers` — number of distinct customers from that cohort who made **at least one purchase** in that month index
# MAGIC    - `retention_rate` — `retained_customers / cohort_size * 100`, rounded to 2 decimals
# MAGIC 3. Only include cohorts with **at least 2 customers**.
# MAGIC 4. Only include month indices **0 through 3**.
# MAGIC 5. Order by `cohort_month` ascending, then `month_index` ascending.
# MAGIC
# MAGIC ### Example Input
# MAGIC
# MAGIC | purchase_id | customer_id | purchase_date | amount |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | 1 | 2026-01-05 | 100.0 |
# MAGIC | 2 | 2 | 2026-01-10 | 200.0 |
# MAGIC | 3 | 3 | 2026-01-15 | 150.0 |
# MAGIC | 4 | 1 | 2026-02-05 | 120.0 |
# MAGIC | 5 | 2 | 2026-02-10 | 250.0 |
# MAGIC | 6 | 4 | 2026-02-15 | 300.0 |
# MAGIC | 7 | 5 | 2026-02-20 | 100.0 |
# MAGIC | 8 | 1 | 2026-03-05 | 200.0 |
# MAGIC | 9 | 3 | 2026-03-10 | 180.0 |
# MAGIC | 10 | 4 | 2026-03-15 | 220.0 |
# MAGIC | 11 | 1 | 2026-04-05 | 150.0 |
# MAGIC | 12 | 4 | 2026-04-10 | 300.0 |
# MAGIC | 13 | 5 | 2026-04-15 | 100.0 |
# MAGIC | 14 | 6 | 2026-04-20 | 250.0 |
# MAGIC | 15 | 2 | 2026-04-25 | 180.0 |
# MAGIC
# MAGIC ### Expected Output
# MAGIC
# MAGIC | cohort_month | month_index | cohort_size | retained_customers | retention_rate |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 2026-01-01 | 0 | 3 | 3 | 100.00 |
# MAGIC | 2026-01-01 | 1 | 3 | 2 | 66.67 |
# MAGIC | 2026-01-01 | 2 | 3 | 2 | 66.67 |
# MAGIC | 2026-01-01 | 3 | 3 | 1 | 33.33 |
# MAGIC | 2026-02-01 | 0 | 2 | 2 | 100.00 |
# MAGIC | 2026-02-01 | 1 | 2 | 1 | 50.00 |
# MAGIC | 2026-02-01 | 2 | 2 | 2 | 100.00 |
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC * **Jan cohort** (customers 1, 2, 3 — 3 customers):
# MAGIC   - Month 0 (Jan): all 3 purchased → 100%
# MAGIC   - Month 1 (Feb): customers 1, 2 → 2/3 = 66.67%
# MAGIC   - Month 2 (Mar): customers 1, 3 → 2/3 = 66.67%
# MAGIC   - Month 3 (Apr): customer 1 → 1/3 = 33.33%
# MAGIC * **Feb cohort** (customers 4, 5 — 2 customers):
# MAGIC   - Month 0 (Feb): both purchased → 100%
# MAGIC   - Month 1 (Mar): customer 4 → 1/2 = 50%
# MAGIC   - Month 2 (Apr): customers 4, 5 → 2/2 = 100%
# MAGIC * **Apr cohort** (customer 6 only — 1 customer): excluded (cohort_size < 2)
# MAGIC
# MAGIC ### Key Concepts Tested
# MAGIC
# MAGIC * **Multiple CTEs** — chaining 4+ steps (first purchase → cohort assignment → monthly activity → retention calc)
# MAGIC * **Window functions** — `min() OVER (PARTITION BY customer_id)` for first purchase date
# MAGIC * **Self-join** — joining customer-level cohort data back to monthly purchase activity
# MAGIC * **Date functions** — `date_trunc('month', ...)`, `months_between()` for month index calculation
# MAGIC * **Conditional aggregation** — `COUNT(DISTINCT ...)` grouped by cohort + month index
# MAGIC * **Filtering** — cohort size threshold and month index range
# MAGIC * **Rounding** — `round()` to 2 decimal places
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Write your solution in the code cell below using `spark.sql("...")` 👇

# COMMAND ----------

# ===== SETUP: Sample Data (run this first) =====
purchases_data = [
    (1, 1, "2026-01-05", 100.0),
    (2, 2, "2026-01-10", 200.0),
    (3, 3, "2026-01-15", 150.0),
    (4, 1, "2026-02-05", 120.0),
    (5, 2, "2026-02-10", 250.0),
    (6, 4, "2026-02-15", 300.0),
    (7, 5, "2026-02-20", 100.0),
    (8, 1, "2026-03-05", 200.0),
    (9, 3, "2026-03-10", 180.0),
    (10, 4, "2026-03-15", 220.0),
    (11, 1, "2026-04-05", 150.0),
    (12, 4, "2026-04-10", 300.0),
    (13, 5, "2026-04-15", 100.0),
    (14, 6, "2026-04-20", 250.0),
    (15, 2, "2026-04-25", 180.0),
]

columns = ["purchase_id", "customer_id", "purchase_date", "amount"]
purchases = spark.createDataFrame(purchases_data, columns)
purchases.createOrReplaceTempView("purchases")
purchases.show()

# ===== YOUR SQL SOLUTION BELOW =====
# TODO: Write a SQL query using spark.sql("...") that produces the cohort retention table.
#
# Hints:
#   1. CTE 1: Find each customer's first purchase month using min(date_trunc('month', purchase_date)) OVER (PARTITION BY customer_id)
#   2. CTE 2: For each purchase, compute the purchase month and the month_index = months_between(purchase_month, cohort_month) as integer
#   3. CTE 3: Join cohort info back — group by cohort_month and month_index, count distinct customer_id as retained_customers
#   4. CTE 4: Compute cohort_size = count distinct customers per cohort_month
#   5. Final: Join retained counts with cohort sizes, compute retention_rate = round(retained / cohort_size * 100, 2)
#   6. Filter: month_index BETWEEN 0 AND 3, cohort_size >= 2
#   7. Order by cohort_month, month_index