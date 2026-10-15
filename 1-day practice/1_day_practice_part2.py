# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
products = spark.read.json(
    "/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/products.json",
    multiLine=True,   # file is one pretty-printed JSON array, not one-object-per-line
)
display(products)

# COMMAND ----------

customers = spark.read.csv("/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/customers.csv", inferSchema = True, header = True)
display(customers)

# COMMAND ----------

sales = spark.read.csv("/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/sales.csv", inferSchema = True, header=True)
display(sales)

# COMMAND ----------

# DBTITLE 1,Read events.json
events = spark.read.json(
    "/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/events.json",
    multiLine=True,   # file is one pretty-printed JSON array, not one-object-per-line
)
display(events)

# COMMAND ----------

orders = spark.read.csv("/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/orders.csv", inferSchema = True, header= True)
display(orders)

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Read customers.csv and compute the count of active customers by country and segment.**

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window, WindowSpec

# COMMAND ----------

active_customers = (
    customers
        .filter(col("is_active") == True)
        .groupBy("country", "segment")
        .count()
        .orderBy("country", "segment")
)
display(active_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Find duplicate customer IDs in the customer master and flag all invalid rows before loading to the warehouse.**

# COMMAND ----------

from pyspark.sql.functions import col, count, when, row_number

# Identify which customer_ids are duplicated
duplicate_ids = (
    customers
        .groupBy("customer_id")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
)

# Flag every row whose customer_id appears more than once as invalid
# Keep the first occurrence (by row_number) as valid, the rest as invalid
duplicate_flagged = (
    customers
        .join(duplicate_ids, on="customer_id", how="left")
        .withColumn(
            "_is_duplicate_id",
            when(col("row_count").isNull(), False).otherwise(True)
        )
        .drop("row_count")
)

# Use a window to pick the first row per customer_id as the valid one
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("customer_id")

duplicate_flagged = (
    duplicate_flagged
        .withColumn("_row_num", row_number().over(w))
        .withColumn(
            "is_valid",
            when(col("_is_duplicate_id") & (col("_row_num") > 1), False)
            .otherwise(True)
        )
        .drop("_row_num", "_is_duplicate_id")
)

display(duplicate_flagged.filter(~col("is_valid")))

# Clean dataset: only valid rows, with duplicates removed
clean_customers_data = duplicate_flagged.filter(col("is_valid")).drop("is_valid")
display(clean_customers_data)