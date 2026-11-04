# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import udf

# COMMAND ----------

# DBTITLE 1,Basic UDF: sum_num
from pyspark.sql.types import LongType

# Basic UDF: wrap a plain Python function with udf()
def sum_num(a, b):
    return a + b

# WITHOUT a returnType, Spark guesses by inspecting the function and can pick a
# surprising type (running this bare inferred STRING, so totals came back as "3").
# Best practice: ALWAYS declare the return type explicitly.
sum_udf = udf(sum_num, returnType=LongType())

df = spark.createDataFrame([(1, 2), (10, 20), (100, 200)], "a int, b int")
df.withColumn("total", sum_udf("a", "b")).display()

# COMMAND ----------

# DBTITLE 1,What is a UDF?
# MAGIC %md
# MAGIC ## What is a PySpark UDF?
# MAGIC
# MAGIC A **User Defined Function (UDF)** lets you apply custom Python logic to every row of a DataFrame when no built-in Spark function can do the job.
# MAGIC
# MAGIC **How it works internally:** Spark runs in the JVM and cannot execute Python functions directly. It serializes your function, ships it to the executors, and runs it **row-by-row in a Python worker**, converting each value to a Python object and back. That serialization round-trip is why UDFs are much slower than native Spark expressions.
# MAGIC
# MAGIC **Common uses:**
# MAGIC
# MAGIC * Custom business rules with no built-in equivalent (masking, specialized parsing, grading logic)
# MAGIC * Reusing a third-party Python library (fuzzy matching, phonetics, encryption)
# MAGIC * Complex text transformations beyond built-in regex functions
# MAGIC
# MAGIC **Two ways to create one:** `udf(func)` and the `@udf` decorator. When a UDF is unavoidable, prefer the vectorized `pandas_udf` (see below).

# COMMAND ----------

# DBTITLE 1,UDF with @udf decorator
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Decorator syntax with an EXPLICIT return type (recommended over inference)
@udf(returnType=StringType())
def grade(score):
    if score is None:
        return None
    if score >= 90:
        return "A"
    elif score >= 75:
        return "B"
    elif score >= 60:
        return "C"
    return "F"

students = spark.createDataFrame(
    [("Ashok", 85), ("Basudev", 92), ("Charlie", 58), ("Diana", 71)],
    "name string, score int",
)
display(students.withColumn("grade", grade(col("score"))))

# COMMAND ----------

# DBTITLE 1,Native alternative (preferred)
# The SAME grading logic with native Spark expressions — no UDF, no Python worker, far faster.
# Rule of thumb: if a built-in expression can do it, never write a UDF.
from pyspark.sql.functions import when, col

students.withColumn(
    "grade",
    when(col("score") >= 90, "A")
    .when(col("score") >= 75, "B")
    .when(col("score") >= 60, "C")
    .otherwise("F"),
).display()

# COMMAND ----------

# DBTITLE 1,Vectorized pandas UDF
# pandas UDF (vectorized UDF): Spark hands your function an entire pandas Series at once,
# processed via Arrow batches — typically several times faster than a row-by-row UDF.
import pandas as pd
from pyspark.sql.functions import pandas_udf, col

@pandas_udf("double")
def to_fahrenheit(temps: pd.Series) -> pd.Series:
    return temps * 9 / 5 + 32

temps = spark.createDataFrame(
    [("Bangalore", 30.0), ("Delhi", 41.5), ("Shimla", 12.3)],
    "city string, temp_c double",
)
display(temps.withColumn("temp_f", to_fahrenheit(col("temp_c"))))

# COMMAND ----------

# DBTITLE 1,When to use a UDF
# MAGIC %md
# MAGIC ## When to use a UDF — and when not to
# MAGIC
# MAGIC **Use a UDF when:**
# MAGIC
# MAGIC * No native Spark expression can produce the result
# MAGIC * You must call an external Python library or a custom algorithm
# MAGIC
# MAGIC **Avoid a UDF when:**
# MAGIC
# MAGIC * A built-in exists: `when/otherwise` for branching, `regexp_replace`/`regexp_extract` for text, `split`/`concat_ws`, date functions, and higher-order functions (`transform`, `filter`, `exists`) for arrays and maps
# MAGIC * The UDF would run over millions of rows in a hot path
# MAGIC
# MAGIC **Performance ladder (fastest → slowest):**
# MAGIC
# MAGIC 1. Native Spark/SQL expressions (stays in the JVM)
# MAGIC 2. `pandas_udf` (vectorized, Arrow-batched)
# MAGIC 3. Regular Python `udf` (row-by-row serialization)
# MAGIC
# MAGIC **Gotchas:**
# MAGIC
# MAGIC * Always declare `returnType=` explicitly — inference can surprise you (e.g. `sum_num` returns `long`, not `int`)
# MAGIC * Handle `None` inside the UDF yourself — a Python error inside a UDF fails the whole query
# MAGIC * On serverless compute, Hive JAR UDFs (`CREATE FUNCTION ... USING JAR`) are blocked — use Python UDFs or Unity Catalog Python functions (`CREATE FUNCTION ... LANGUAGE PYTHON`) instead