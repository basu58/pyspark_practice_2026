# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.csv('/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/customers.csv', inferSchema=True, header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/customers.csv")
display(df)

# COMMAND ----------

schema = StructType().add("customer_id", StringType()).add("customer_name", StringType()).add("signup_date", DateType()).add("city", StringType()).add("state", StringType()).add("country", StringType()).add("is_active", BooleanType()).add("segment", StringType()).add("source_channel", StringType()).add("loyalty_tier", StringType())
df = spark.read.format("csv").options(header="true", schema = schema).load("/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/customers.csv")
display(df)

# COMMAND ----------

