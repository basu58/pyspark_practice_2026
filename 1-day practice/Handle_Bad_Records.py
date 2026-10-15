# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
df = spark.read.options(inferSchema = True, header = True).csv('/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/customers.csv')
display(df)

# COMMAND ----------

# DBTITLE 1,Re-read CSV with PERMISSIVE mode + columnNameOfCorruptRecord
df_permissive = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv('/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/real_data_practice/customers.csv')

display(df_permissive)

# COMMAND ----------

# DBTITLE 1,PERMISSIVE mode with explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- Sample CSV data with a malformed row ---
# Schema: name (string), age (int), city (string)
# Row 1: valid
# Row 2: valid
# Row 3: MALFORMED — 'thirty' is not an integer
# Row 4: valid

# --------------------------------------------------------------------
# 1) PERMISSIVE (default) — corrupt rows go into a special column
# --------------------------------------------------------------------
# KEY: columnNameOfCorruptRecord requires an EXPLICIT schema that includes
# the corrupt-record column. With inferSchema=True alone, Spark does NOT
# add _corrupt_record to the schema, so the column can't be referenced.
print("\n=== 1. PERMISSIVE mode ===")
print("Malformed rows are kept; the entire bad row is stuffed into _corrupt_record.\n")

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

df_permissive = spark.read \
    .option("header", True) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .csv("/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/1-day practice/Data/people_data.csv")

df_permissive.show(truncate=False)

# Filter to see only the corrupt row
df_permissive.filter(df_permissive._corrupt_record.isNotNull()).show(truncate=False)


# COMMAND ----------


# --------------------------------------------------------------------
# 2) DROPMALFORMED — corrupt rows are silently dropped
# --------------------------------------------------------------------
print("\n=== 2. DROPMALFORMED mode ===")
print("Malformed rows are dropped entirely; only clean rows remain.\n")

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

df_drop = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("mode", "DROPMALFORMED") \
    .schema(schema) \
    .csv("/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/1-day practice/Data/people_data.csv")

df_drop.show(truncate=False)



# COMMAND ----------

# --------------------------------------------------------------------
# 3) FAILFAST — corrupt rows cause the job to throw an exception
# --------------------------------------------------------------------
print("\n=== 3. FAILFAST mode ===")
print("Malformed rows cause an immediate error / exception.\n")

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

df_fail = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("mode", "FAILFAST") \
    .schema(schema)\
    .csv("/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/1-day practice/Data/people_data.csv")

try:
    df_fail.show(truncate=False)
except Exception as e:
    print(f"*** FAILFAST raised an exception as expected! ***")
    print(f"Error: {e}")
