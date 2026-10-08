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