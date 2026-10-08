# Databricks notebook source
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