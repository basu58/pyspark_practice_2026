# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 🧩 PySpark Interview Question — Medium
# MAGIC
# MAGIC ## **Problem: Find the Top 3 Most Frequent Purchased Products per User in Each Category**
# MAGIC
# MAGIC You are given a dataset of e-commerce purchases with the following schema:
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `order_id` | int | Unique order identifier |
# MAGIC | `user_id` | int | User who placed the order |
# MAGIC | `product_id` | int | Product purchased |
# MAGIC | `category` | string | Product category (e.g., Electronics, Clothing, Books) |
# MAGIC | `quantity` | int | Number of units purchased |
# MAGIC | `order_date` | date | Date of purchase |
# MAGIC
# MAGIC **Task:** For each `(user_id, category)` pair, find the **top 3 most frequently purchased `product_id`s** ranked by **total quantity purchased** (descending). If there are ties in total quantity, break ties by `product_id` ascending. Only include products that were purchased at least twice.
# MAGIC
# MAGIC ### Example Input
# MAGIC
# MAGIC | order_id | user_id | product_id | category | quantity | order_date |
# MAGIC | --- | --- | --- | --- | --- | --- |
# MAGIC | 1 | 101 | 501 | Electronics | 2 | 2026-01-05 |
# MAGIC | 2 | 101 | 502 | Electronics | 1 | 2026-01-10 |
# MAGIC | 3 | 101 | 501 | Electronics | 3 | 2026-01-15 |
# MAGIC | 4 | 101 | 503 | Electronics | 4 | 2026-01-20 |
# MAGIC | 5 | 101 | 501 | Electronics | 1 | 2026-02-01 |
# MAGIC | 6 | 101 | 502 | Clothing | 5 | 2026-01-08 |
# MAGIC | 7 | 102 | 601 | Books | 2 | 2026-01-03 |
# MAGIC | 8 | 102 | 601 | Books | 3 | 2026-01-12 |
# MAGIC
# MAGIC ### Expected Output
# MAGIC
# MAGIC | user_id | category | product_id | total_quantity | rank |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 101 | Electronics | 501 | 6 | 1 |
# MAGIC | 101 | Electronics | 503 | 4 | 2 |
# MAGIC | 101 | Clothing | 502 | 5 | 1 |
# MAGIC | 102 | Books | 601 | 5 | 1 |
# MAGIC
# MAGIC ### Key Concepts Tested
# MAGIC * **Window functions** with `partitionBy` + `orderBy` + `rank/dense_rank`
# MAGIC * **GroupBy aggregation** across multiple keys
# MAGIC * **Filtering** based on aggregate conditions (`HAVING` equivalent)
# MAGIC * **Handling ties** in ranking logic
# MAGIC
# MAGIC ### Constraints
# MAGIC * Solve it using **PySpark DataFrame API** (not raw SQL)
# MAGIC * Do NOT use pandas — stay in Spark
# MAGIC * Limit final output to top 3 per group
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Write your solution below 👇

# COMMAND ----------

# ===== SETUP: Sample Data (run this first) =====
from pyspark.sql.functions import col, sum as _sum, count as _count, dense_rank
from pyspark.sql.window import Window

# Build sample DataFrame
orders_data = [
    (1, 101, 501, "Electronics", 2, "2026-01-05"),
    (2, 101, 502, "Electronics", 1, "2026-01-10"),
    (3, 101, 501, "Electronics", 3, "2026-01-15"),
    (4, 101, 503, "Electronics", 4, "2026-01-20"),
    (5, 101, 501, "Electronics", 1, "2026-02-01"),
    (6, 101, 502, "Clothing",    5, "2026-01-08"),
    (7, 102, 601, "Books",       2, "2026-01-03"),
    (8, 102, 601, "Books",       3, "2026-01-12"),
]

columns = ["order_id", "user_id", "product_id", "category", "quantity", "order_date"]
orders = spark.createDataFrame(orders_data, columns)
orders.show()

# ===== YOUR SOLUTION BELOW =====
# TODO: For each (user_id, category), find the top 3 most frequently purchased product_ids
#       ranked by total quantity (desc). Break ties by product_id (asc).
#       Only include products purchased at least twice.
#
# Hints:
#   1. Group by user_id, category, product_id -> aggregate total quantity and purchase count
#   2. Filter products with purchase_count >= 2
#   3. Use a window partitioned by (user_id, category), ordered by (total_quantity DESC, product_id ASC)
#   4. Use rank() or dense_rank(), then filter for rank <= 3

# COMMAND ----------

orders.createOrReplaceTempView('orders')

# COMMAND ----------

w = Window.partitionBy('user_id', 'category').orderBy(col('Total_Quantity').desc(), 'product_id')
orders.groupBy('user_id', 'category', 'product_id').agg(_sum("quantity").alias('Total_Quantity')).filter(col('Total_Quantity')>2)\
    .withColumn('rank', dense_rank().over(w)).where(col('rank') <= 3).show()


# COMMAND ----------

orders.show()

# COMMAND ----------

w = Window.partitionBy(col('user_id'), col('category')).orderBy(col('total_quantity').desc(), col('product_id').asc())
agg_data = orders.groupBy('user_id', 'category', 'product_id').agg(_sum(col('quantity')).alias('total_quantity'))\
.filter(col('total_quantity')>=2)
result = agg_data.withColumn('rank', dense_rank().over(w)).filter(col('rank') <= 3).orderBy('user_id', 'category', 'product_id', col('rank').desc())
result.show()

# COMMAND ----------

spark.sql("select * from (select user_id, category, total_quantity, product_id, dense_rank()over(partition by user_id, category order by total_quantity desc) rank from (select user_id, category, product_id, sum(quantity) total_quantity from orders group by all having sum(quantity) >= 2)) where rank <= 3").show()