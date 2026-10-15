# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

data = [
    (101, "Alice",   "Tech",    90000),
    (102, "Bob",     "Tech",    80000),
    (103, "Charlie", "Tech",    80000),
    (104, "David",   "Tech",    70000),
    (105, "Eva",     "HR",      60000),
    (106, "Frank",   "HR",      60000),
    (107, "Grace",   "HR",      50000),
    (108, "Hank",    "Finance", 95000)
]

columns = ["emp_id", "emp_name", "department", "salary"]
df = spark.createDataFrame(data, columns)
df.createOrReplaceTempView("people")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # **1. row_number()**
# MAGIC Assigns a unique sequential integer (1, 2, 3...) to each row within a partition, strictly determined by the ordering. It does not handle ties—tied values receive arbitrary unique numbers.
# MAGIC
# MAGIC Interview Scenario: "Find the highest earner in each department, or eliminate duplicate records."

# COMMAND ----------

row_num_df = Window.partitionBy("department").orderBy(col("salary").desc())
display(row_num_df)
df.withColumn('row_num', row_number().over(row_num_df)).filter(col('row_num') == 1).show()
spark.sql("select * from (select *,row_number()over(partition by department order by salary desc) as row_num from people) where row_num = 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **2. rank()**
# MAGIC Assigns a sequential rank starting at 1. If two rows have identical values in the orderBy clause, they receive the same rank, and the subsequent rank(s) are skipped.
# MAGIC
# MAGIC Interview Scenario: "Rank employees by salary per department. If two people tie for 2nd place, the next person should be 4th."

# COMMAND ----------

w = Window.partitionBy("department").orderBy(col("salary").desc())

df.withColumn("rank", rank().over(w)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **3. dense_rank()**
# MAGIC Assigns sequential ranks like rank(), but does not skip numbers after ties.
# MAGIC
# MAGIC Interview Scenario: "Find the top 2 distinct salary tiers per department." (If salaries are 90k, 80k, 80k, 70k, dense_rank yields 1, 2, 2, 3 so filtering <= 2 includes the 80k tie without dropping the 2nd tier).

# COMMAND ----------

w = Window.partitionBy('department').orderBy(col('salary').desc())
df.withColumn('dense_rank', dense_rank().over(w)).filter(col('dense_rank') <= 2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **4. lag()**
# MAGIC Fetches the value of a column from a previous row within the partition at a given offset.
# MAGIC
# MAGIC Interview Scenario: "Compare each employee's salary with the person earning directly above them in their department."

# COMMAND ----------

w = Window.partitionBy('department').orderBy(col('salary').desc())
df.withColumns({'previous_salary': lag('salary').over(w)\
    ,'salary_compare': when(col('previous_salary').isNull(), 0)\
        .otherwise(col('salary') - col('previous_salary'))
    }).show()

# COMMAND ----------

w = Window.partitionBy('department').orderBy(col('salary').desc())
df.withColumns({'previous_salary': lag('salary', 1, 0).over(w)\
    ,'salary_compare': when(col('previous_salary') == 0, 0)\
        .otherwise(col('salary') - col('previous_salary'))
    }).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **5. lead()**
# MAGIC Fetches the value of a column from a subsequent row within the partition at a given offset.
# MAGIC
# MAGIC Find next highest salaried employee name and his salary

# COMMAND ----------

w = Window.partitionBy('department').orderBy(col('salary'))
df.withColumns({
    'next_emp_name': lead('emp_name', 1).over(w),
    'next_salary': lead('salary', 1).over(w)
}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **6. sum() over a window**
# MAGIC Calculates the sum of values over a specified window frame.
# MAGIC
# MAGIC Interview Scenario: "Calculate a running total of department salary spend versus total department budget."
# MAGIC
# MAGIC Crucial Interview Detail: If orderBy is provided inside the Window spec, PySpark calculates an unbounded-preceding to current-row running sum. Without orderBy, it calculates the total sum for the entire partition.

# COMMAND ----------

w = Window.partitionBy('department').orderBy(col('salary').desc())
df.withColumn('running_total', sum(col("salary")).over(w)).show()
w = Window.partitionBy('department')
df.withColumn('running_total', sum(col("salary")).over(w)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **7. avg() over a window**
# MAGIC Computes the average over the partition or running frame.
# MAGIC
# MAGIC Interview Scenario: "Find employees who earn more than their department's average salary."

# COMMAND ----------

w = Window.partitionBy('department')
df.withColumn('dept_avg_sal', avg('salary').over(w)).filter(col('salary') > col('dept_avg_sal')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **8. min() / max() over a window**
# MAGIC Retrieves the minimum or maximum value in the partition.
# MAGIC
# MAGIC Interview Scenario: "Show each employee's salary gap relative to the highest-paid employee in their department."

# COMMAND ----------

w = Window.partitionBy('department')
df.withColumns({'Min_sal': min(col('salary')).over(w)\
    ,'emp_sal_minus_min_dept_sal': col('salary') - col('Min_sal')\
    ,'max_sal': max(col('salary')).over(w)\
    ,'emp_sal_minus_max_dept_sal': col('salary') - col('max_sal')
    }).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **9. count() over a window**
# MAGIC Counts rows within the window frame.
# MAGIC
# MAGIC Interview Scenario: "Filter out departments that have fewer than 3 employees."

# COMMAND ----------

w = Window.partitionBy('department')
df.withColumn('count_emp', count(col('emp_id')).over(w)).filter(col('count_emp') >= 3).drop(col('count_emp')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **10. first_value()**
# MAGIC Returns the first value in the window frame according to ordering.
# MAGIC
# MAGIC Interview Scenario: "Attach the name of the top earner in each department to every employee row."

# COMMAND ----------

w = Window.partitionBy('department')
df.withColumn('Top_Earner', first_value('salary').over(w)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **11. last_value()**
# MAGIC Returns the last value in the window frame according to ordering.
# MAGIC
# MAGIC Tricky Interview Pitfall: By default, adding an orderBy creates a frame bounds of RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. Therefore, last_value() will just return the current row's value unless you explicitly widen the window frame using rowsBetween.
# MAGIC
# MAGIC Interview Scenario: "Attach the lowest earner in each department to every row."

# COMMAND ----------

w = Window.partitionBy('department')
df.withColumn('Last_Earner', last_value(col('salary')).over(w)).show()

# COMMAND ----------

full_window = Window.partitionBy("department").orderBy("salary") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df.withColumn("lowest_earner", last_value("emp_name").over(full_window)).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder.appName("PySparkJoinsTutorial").getOrCreate()

# Employee DataFrame
emp_data = [
    (101, "Alice", 10),
    (102, "Bob", 20),
    (103, "Charlie", 20),
    (104, "David", 30),
    (105, "Eva", None),    # Employee without a department
    (106, "Frank", 50)    # Department 50 does not exist in Dept DF
]
emp_cols = ["emp_id", "emp_name", "dept_id"]
emp_df = spark.createDataFrame(emp_data, emp_cols)

# Department DataFrame
dept_data = [
    (10, "Engineering"),
    (20, "Marketing"),
    (30, "Sales"),
    (40, "HR")            # Department with no employees
]
dept_cols = ["dept_id", "dept_name"]
dept_df = spark.createDataFrame(dept_data, dept_cols)