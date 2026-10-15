# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # 🧩 SQL Interview Question — Medium
# MAGIC
# MAGIC ## **Problem: Monthly Revenue Trend with MoM Growth & Best Customer**
# MAGIC
# MAGIC You are given an `orders` table with the following schema:
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `order_id` | int | Unique order ID |
# MAGIC | `customer_id` | int | Customer who placed the order |
# MAGIC | `order_date` | date | Date of the order |
# MAGIC | `amount` | double | Order amount in USD |
# MAGIC
# MAGIC **Task:** Produce a monthly revenue report with the following columns:
# MAGIC
# MAGIC | Column | Description |
# MAGIC | --- | --- |
# MAGIC | `month` | First day of the month (e.g., `2026-01-01`) |
# MAGIC | `total_revenue` | Sum of all order amounts that month |
# MAGIC | `prev_month_revenue` | Total revenue from the previous month (NULL for the first month) |
# MAGIC | `mom_growth_pct` | Month-over-month growth pct = (total - prev) / prev * 100, rounded to 2 decimals (NULL for first month) |
# MAGIC | `trend` | High Growth if >20%, Moderate Growth if 5-20%, Flat if -5% to 5%, Decline if < -5%, N/A for first month |
# MAGIC | `best_customer_id` | Customer with the highest total spending that month |
# MAGIC | `best_customer_spending` | That customer's total spending that month |
# MAGIC
# MAGIC **Order by** `month` ascending.
# MAGIC
# MAGIC ### Example Input
# MAGIC
# MAGIC | order_id | customer_id | order_date | amount |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | 1 | 2026-01-05 | 100.0 |
# MAGIC | 2 | 2 | 2026-01-10 | 200.0 |
# MAGIC | 3 | 1 | 2026-01-20 | 150.0 |
# MAGIC | 4 | 3 | 2026-01-25 | 300.0 |
# MAGIC | 5 | 1 | 2026-02-05 | 120.0 |
# MAGIC | 6 | 2 | 2026-02-15 | 250.0 |
# MAGIC | 7 | 3 | 2026-02-20 | 180.0 |
# MAGIC | 8 | 1 | 2026-03-01 | 200.0 |
# MAGIC | 9 | 2 | 2026-03-10 | 100.0 |
# MAGIC | 10 | 3 | 2026-03-15 | 400.0 |
# MAGIC | 11 | 1 | 2026-03-20 | 50.0 |
# MAGIC | 12 | 2 | 2026-04-05 | 300.0 |
# MAGIC | 13 | 3 | 2026-04-10 | 150.0 |
# MAGIC
# MAGIC ### Expected Output
# MAGIC
# MAGIC | month | total_revenue | prev_month_revenue | mom_growth_pct | trend | best_customer_id | best_customer_spending |
# MAGIC | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | 2026-01-01 | 750.0 | NULL | NULL | N/A | 3 | 300.0 |
# MAGIC | 2026-02-01 | 550.0 | 750.0 | -26.67 | Decline | 2 | 250.0 |
# MAGIC | 2026-03-01 | 750.0 | 550.0 | 36.36 | High Growth | 3 | 400.0 |
# MAGIC | 2026-04-01 | 450.0 | 750.0 | -40.00 | Decline | 2 | 300.0 |
# MAGIC
# MAGIC ### Key Concepts Tested
# MAGIC * Date truncation with `date_trunc('month', ...)` to bucket by month
# MAGIC * Window functions: `lag()` for previous-month lookup, `rank()` for best customer
# MAGIC * CTEs: chaining multiple steps (monthly totals then growth then best customer then final)
# MAGIC * CASE expressions for classifying trends based on growth percentage
# MAGIC * NULL handling for the first month
# MAGIC * `round()` to 2 decimal places
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Write your solution in the SQL cell below using `spark.sql(...)`

# COMMAND ----------

# ===== SETUP: Sample Data (run this first) =====
orders_data = [
    (1, 1, "2026-01-05", 100.0),
    (2, 2, "2026-01-10", 200.0),
    (3, 1, "2026-01-20", 150.0),
    (4, 3, "2026-01-25", 300.0),
    (5, 1, "2026-02-05", 120.0),
    (6, 2, "2026-02-15", 250.0),
    (7, 3, "2026-02-20", 180.0),
    (8, 1, "2026-03-01", 200.0),
    (9, 2, "2026-03-10", 100.0),
    (10, 3, "2026-03-15", 400.0),
    (11, 1, "2026-03-20", 50.0),
    (12, 2, "2026-04-05", 300.0),
    (13, 3, "2026-04-10", 150.0),
]

columns = ["order_id", "customer_id", "order_date", "amount"]
orders = spark.createDataFrame(orders_data, columns)
orders.createOrReplaceTempView("orders")
orders.show()

# ===== YOUR SQL SOLUTION BELOW =====
# TODO: Write a SQL query using spark.sql("...") that produces the monthly revenue report.
#
# Hints:
#   1. CTE 1: Group by date_trunc('month', order_date) to get monthly total revenue
#   2. CTE 2: Use lag() window function to get prev_month_revenue, then compute mom_growth_pct
#   3. CTE 3: Separately group by month + customer_id to find best customer per month (use rank or row_number)
#   4. Final: Join CTE 2 and CTE 3, add CASE expression for trend classification
#   5. Handle NULL for first month's growth and trend (N/A)
#   6. Round mom_growth_pct to 2 decimals
#   7. Order by month ascending