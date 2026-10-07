# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
df = spark.read.csv('/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/customers.csv', inferSchema=True, header=True)

# COMMAND ----------

display(df)