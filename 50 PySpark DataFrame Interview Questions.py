# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 50 PySpark DataFrame Interview Questions — Rapid Review
# MAGIC
# MAGIC **Categories:**
# MAGIC 1. Q1–8:   DataFrame Basics
# MAGIC 2. Q9–16:  Selecting & Filtering
# MAGIC 3. Q17–24: Sorting, Limiting & Collecting
# MAGIC 4. Q25–32: Aggregations & GroupBy
# MAGIC 5. Q33–40: Joins & Set Operations
# MAGIC 6. Q41–45: String & Date Functions
# MAGIC 7. Q46–50: Window Functions
# MAGIC
# MAGIC Each question has sample data + solution inline. Run all cells to verify outputs.

# COMMAND ----------

# ===== SHARED SAMPLE DATA (run this first) =====
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

employees = spark.createDataFrame([
    (1, "Alice",   "Engineering", 120000, "2023-01-15"),
    (2, "Bob",     "Sales",       95000,  "2023-03-20"),
    (3, "Charlie", "Engineering", 110000, "2023-06-01"),
    (4, "Diana",   "Marketing",    70000,  None),
    (5, "Eve",     "Sales",       95000,  "2023-02-10"),
    (6, "Frank",   "Engineering", 130000, "2023-09-15"),
    (7, "Grace",   "Marketing",    85000, "2023-04-05"),
    (8, "Henry",   None,           90000, "2023-07-22"),
], ["emp_id", "emp_name", "department", "salary", "hire_date"])

departments = spark.createDataFrame([
    ("Engineering", "Building A", 500000),
    ("Sales",       "Building B", 300000),
    ("Marketing",   "Building A", 200000),
    ("Finance",     "Building C", 400000),
], ["department", "location", "budget"])

orders = spark.createDataFrame([
    (101, 1, "2024-01-05", 250.00, "Electronics"),
    (102, 2, "2024-01-10", 150.00, "Clothing"),
    (103, 1, "2024-02-15", 300.00, "Electronics"),
    (104, 3, "2024-02-20", 100.00, "Books"),
    (105, 2, "2024-03-01", 450.00, "Electronics"),
    (106, 1, "2024-03-10", 200.00, "Clothing"),
    (107, 3, "2024-03-15", 350.00, "Books"),
    (108, 4, "2024-01-25", 500.00, "Electronics"),
], ["order_id", "customer_id", "order_date", "amount", "category"])

print('employees')
employees.show()
print('departments')
departments.show()
print('orders')
orders.show()

# COMMAND ----------

# DBTITLE 1,Q1: Create DataFrame with Schema
# Q1: Create a DataFrame from a list of tuples with explicit schema
# Data: [(1, "Alice", True), (2, "Bob", False)]
# Columns: id (int, not null), name (string), active (boolean)
# Hint: use StructType, StructField, IntegerType, StringType, BooleanType

# YOUR CODE BELOW:
from pyspark.sql.types import *
data = [(1, "Alice", True), (2, "Bob", False)]
schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('Name', StringType()),
    StructField('Active', BooleanType())
])
df = spark.createDataFrame(data=data, schema=schema)
display(df)

# COMMAND ----------

# DBTITLE 1,Q2: Print Schema
# Q2: Print the schema of the employees DataFrame
# Hint: use printSchema()
df.printSchema()
# YOUR CODE BELOW:

# COMMAND ----------

# DBTITLE 1,Q3: Columns and dtypes
# Q3: Get column names and data types of employees as a Python list
# Hint: use .columns and .dtypes

# YOUR CODE BELOW:
li = employees.columns
li1 = employees.dtypes
print(li, '\n', li1)

# COMMAND ----------

# DBTITLE 1,Q4: Select Columns
# Q4: Select emp_name and salary columns — try 3 different ways
# Hint: string names, col(), DataFrame.column syntax
employees.select('emp_name', 'salary').show()
employees.select(col("emp_name"), col('salary')).show()
employees.alias('a').select('a.emp_name', 'a.salary').show()
# YOUR CODE BELOW:

# COMMAND ----------

# DBTITLE 1,Q5: withColumn
# Q5: Add a new column "bonus" = 10% of salary using withColumn
# Show: emp_name, salary, bonus
employees.withColumn('bonus', col('salary')*.10).select('emp_name', 'salary', 'bonus').show()
# YOUR CODE BELOW:

# COMMAND ----------

# DBTITLE 1,Q6: withColumnRenamed
# Q6: Rename "emp_name" to "full_name" using withColumnRenamed
# Show: full_name, salary
employees.withColumnRenamed('emp_name', 'full_name').select('full_name', 'salary').show()
# YOUR CODE BELOW:

# COMMAND ----------

# DBTITLE 1,Q7: Drop Columns
# Q7: Drop columns "hire_date" and "emp_id" from employees
# Hint: use drop()
employees.drop('hire_date', "emp_id").show()
# YOUR CODE BELOW:

# COMMAND ----------

# DBTITLE 1,Q8: Cast Data Types
# Q8: Cast salary to double and hire_date to date, then printSchema
# Hint: use .cast("double") and .cast("date")

# YOUR CODE BELOW:
employees.printSchema()
employees.withColumns({'salary': col('salary').cast("double"),
    'hire_date': col('hire_date').cast('date')
    }).printSchema()

# COMMAND ----------

# DBTITLE 1,Q9: Filter >
# Q9: Filter employees with salary > 100000
# Hint: use filter() or where() with col()
employees.filter(col('salary')>100000).show()
employees.where(col('salary')>100000).show()
# YOUR CODE BELOW:

# COMMAND ----------

# DBTITLE 1,Q10: Filter AND
# Q10: Filter employees with salary > 90000 AND department == "Engineering"
# Hint: use & for AND, wrap each condition in ()
employees.filter((col('salary')>90000) & (col('department')=='Engineering')).show()
# YOUR CODE BELOW:

# COMMAND ----------

# DBTITLE 1,Q11: Filter OR
# Q11: Filter employees where department == "Sales" OR department == "Marketing"
# Hint: use | for OR, wrap each condition in ()

# YOUR CODE BELOW:
employees.filter((col('department')=="Sales") | (col('department')=='Marketing')).show()

# COMMAND ----------

# DBTITLE 1,Q12: Filter isin
# Q12: Filter employees where department is in ("Engineering", "Sales")
# Hint: use .isin()

# YOUR CODE BELOW:
employees.filter(col('department').isin("Engineering", "Sales")).show()

# COMMAND ----------

# DBTITLE 1,Q13: like and rlike
# Q13: Filter employees whose name starts with "A" using like()
# Then filter names starting with A or C using rlike()
# Hint: like("A%"), rlike("^[AC]")

# YOUR CODE BELOW:
employees.filter(col('emp_name').like('A%')).show()
employees.filter(col('emp_name').rlike('^[AC]')).show()

# COMMAND ----------

# DBTITLE 1,Q14: Filter Null
# Q14: Filter employees where department is null, and where it is not null
# Hint: use isNull() and isNotNull()

# YOUR CODE BELOW:
employees.filter(col('department').isNull()).show()
employees.filter(col('department').isNotNull()).show()

# COMMAND ----------

# DBTITLE 1,Q15: Filter between
# Q15: Filter employees where salary is between 90000 and 120000
# Hint: use .between(low, high)

# YOUR CODE BELOW:
employees.filter(col('salary').between(90000, 120000)).show()

# COMMAND ----------

# DBTITLE 1,Q16: when/otherwise
# Q16: Add a "tier" column using when/otherwise
# "High" if salary >= 120000, "Medium" if >= 90000, else "Low"
# Show: emp_name, salary, tier

# YOUR CODE BELOW:
employees.withColumn("tier",\
    when(col('salary') >= 120000, 'High').\
    when(col('salary') >= 90000, 'Medium').\
    otherwise('Low')
    ).show()

# COMMAND ----------

# DBTITLE 1,Q17: OrderBy Asc
# Q17: Order employees by salary ascending
# Show: emp_name, salary
# Hint: use orderBy(col("salary").asc())

# YOUR CODE BELOW:
employees.select('emp_id', 'salary').orderBy(col("salary").desc()).show()

# COMMAND ----------

# DBTITLE 1,Q18: OrderBy Multi
# Q18: Order employees by department ascending, salary descending
# Show: emp_name, department, salary

# YOUR CODE BELOW:
employees.orderBy('department', col("salary").desc()).select('emp_name', 'department', 'salary').show(20)

# COMMAND ----------

# DBTITLE 1,Q19: Limit Top 3
# Q19: Get top 3 highest paid employees using limit
# Hint: orderBy desc + limit(3)

# YOUR CODE BELOW:
employees.orderBy(col('salary').desc()).limit(3).show()

# COMMAND ----------

# DBTITLE 1,Q20: head/take/first
# Q20: Get first 3 rows using head(), first 2 using take(), first 1 using first()
# Hint: these return Python Row objects, not DataFrames

# YOUR CODE BELOW:
print(employees.head(3), '\n')
print(employees.take(2), '\n')
print(employees.first())

# COMMAND ----------

# DBTITLE 1,Q21: collect
# Q21: Collect all rows to driver as a list using collect()
# Print the number of rows collected
# WARNING: do not use on large DataFrames in production

# YOUR CODE BELOW:
res = df.collect()
df.count()

# COMMAND ----------

# DBTITLE 1,Q22: Distinct
# Q22: Get distinct department values from employees
# Hint: use select().distinct()

# YOUR CODE BELOW:
employees.select('department').distinct().show()

# COMMAND ----------

# DBTITLE 1,Q23: dropDuplicates
# Q23: Drop duplicate rows — on full row and on ["department", "salary"] subset
# Hint: use dropDuplicates() and dropDuplicates(["col1", "col2"])

# YOUR CODE BELOW:
employees.dropDuplicates().show()
employees.dropDuplicates(['department', 'salary']).show()

# COMMAND ----------

# DBTITLE 1,Q24: Count
# Q24: Count total employees and count per department
# Hint: use .count() and groupBy().count()

# YOUR CODE BELOW:
print(employees.count())
employees.groupBy('department').agg(count('emp_id').alias('count_emp')).show()

# COMMAND ----------

# DBTITLE 1,Q25: GroupBy sum
# Q25: GroupBy department and sum salaries
# Hint: use groupBy().sum("salary")

# YOUR CODE BELOW:
employees.groupBy('department').agg(sum('salary')).show()

# COMMAND ----------

# DBTITLE 1,Q26: GroupBy avg/count
# Q26: GroupBy department and get avg salary and count
# Hint: use groupBy().avg("salary") and groupBy().count()

# YOUR CODE BELOW:
employees.groupBy('department').agg(avg('salary'), count('emp_id')).show()

# COMMAND ----------

# DBTITLE 1,Q27: GroupBy max/min
# Q27: GroupBy department and get max and min salary
# Hint: use groupBy().max("salary") and groupBy().min("salary")

# YOUR CODE BELOW:
employees.groupBy('department').max('salary').show()
employees.groupBy('department').min('salary').show()

# COMMAND ----------

# DBTITLE 1,Q28: Multiple agg()
# Q28: Multiple aggregations in one call using agg()
# Get: total_salary, avg_salary, max_salary, min_salary, emp_count per department
# Hint: use agg(sum().alias(), avg().alias(), ...)

# YOUR CODE BELOW:
employees.groupBy('department').agg(sum('salary').alias('total_salary'), avg('salary').alias('avg_salary'), \
    max('salary').alias('max_salary'), min('salary').alias('min_salary'), countDistinct('emp_id').alias('emp_count')).show()

# COMMAND ----------

# DBTITLE 1,Q29: countDistinct
# Q29: Count distinct salaries per department using countDistinct
# Hint: use agg(countDistinct("salary").alias("distinct_salaries"))

# YOUR CODE BELOW:
employees.groupBy('department').agg(countDistinct('salary').alias('distinct_salary')).show()

# COMMAND ----------

# DBTITLE 1,Q30: Having (filter after agg)
# Q30: Filter after aggregation (HAVING equivalent)
# Only show departments with avg salary > 90000
# Hint: use .filter() after groupBy().agg()

# YOUR CODE BELOW:
employees.groupBy('department').agg(avg('salary').alias('avg_sal')).filter(col('avg_sal')>90000).show()

# COMMAND ----------

orders.show()

# COMMAND ----------

# DBTITLE 1,Q31: Pivot
# Q31: Pivot table — avg amount by customer_id, pivoted on category (use orders)
# Hint: use groupBy().pivot("category").avg("amount")

# YOUR CODE BELOW:
orders.groupBy('customer_id').pivot('category').agg(avg('amount').alias('avg_amount')).show()

# COMMAND ----------

# DBTITLE 1,Q32: approx_count_distinct + collect_list/set
# Q32: Approximate distinct count of customer_id using approx_count_distinct (use orders)
# Bonus: collect_list vs collect_set of emp_name per department (use employees)
# Hint: agg(approx_count_distinct("customer_id")), collect_list(), collect_set()

# YOUR CODE BELOW:

# --- approx_count_distinct ---
orders.agg(approx_count_distinct('customer_id').alias('approx_customers')).show()

# --- collect_list vs collect_set ---
employees.groupBy('department').agg(
    collect_list('emp_name').alias('all_names'),
    collect_set('emp_name').alias('unique_names')
).show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Q33: Inner Join
# Q33: Inner join employees with departments on "department"
# Hint: employees.join(departments, "department", "inner")

# YOUR CODE BELOW:
employees.join(departments, on='department', how='inner').show()

# COMMAND ----------

# DBTITLE 1,Q34: Left Join
# Q34: Left outer join — all employees, even without matching department
# Hint: use "left" as join type

# YOUR CODE BELOW:
employees.join(departments, 'department', 'left').show()

# COMMAND ----------

# DBTITLE 1,Q35: Right Join
# Q35: Right outer join — all departments, even without matching employees
# Hint: use "right" as join type

# YOUR CODE BELOW:
employees.join(departments, 'department', 'right').show()

# COMMAND ----------

# DBTITLE 1,Q36: Full Outer Join
# Q36: Full outer join — all rows from both sides
# Hint: use "outer" as join type

# YOUR CODE BELOW:
employees.join(departments, "department", 'outer').show()

# COMMAND ----------

# DBTITLE 1,Q37: Left Anti Join
# Q37: Left anti join — employees whose department is NOT in departments table
# Hint: use "left_anti" as join type

# YOUR CODE BELOW:
employees.join(departments, 'department', 'anti').show()

# COMMAND ----------

# DBTITLE 1,Q38: Left Semi Join
# Q38: Left semi join — employees whose department IS in departments table (no dept columns)
# Hint: use "left_semi" as join type

# YOUR CODE BELOW:
employees.join(departments, 'department', 'left_semi').show()

# COMMAND ----------

# DBTITLE 1,Q39: Union
# Q39: Union two DataFrames — Engineering employees + salary > 100000
# Show both with duplicates and without (using distinct)
# Hint: use .union() and .union().distinct()

# YOUR CODE BELOW:
df_eng = employees.filter(col('department') == 'Engineering')
df_high = employees.filter(col('salary') > 100000)

# union (keeps duplicates)
df_eng.union(df_high).show()

# union with distinct (removes duplicates)
df_eng.union(df_high).distinct().show()

# COMMAND ----------

# DBTITLE 1,Q40: UnionByName + CrossJoin
# Q40: UnionByName with different column order
# df1 = employees.select("emp_name", "salary")
# df2 = employees.select("salary", "emp_name")  # swapped
# Hint: use .unionByName()
# Bonus: crossJoin employees with departments

# YOUR CODE BELOW:
df1 = employees.select('emp_name', 'salary')
df2 = employees.select('salary', 'emp_name')  # swapped order
df1.unionByName(df2).show()

# Bonus: crossJoin
employees.crossJoin(departments.select(col('department').alias('dept_name'), 'location')) \
        .select('emp_name', 'dept_name', 'location').show(5)


# COMMAND ----------

# DBTITLE 1,Q41: concat/concat_ws
# Q41: Concat emp_name and department with " - " separator
# Try both: concat() with lit() and concat_ws()
# Hint: concat(col, lit(" - "), col) and concat_ws(" | ", col, col)

# YOUR CODE BELOW:
employees.withColumns({
                        'emp_dept1': concat(col('emp_name'), lit(" - "), col('department')),
                       'emp_dept_2': concat_ws(" | ", col('emp_name'), col('department'))
                       }).show()

# COMMAND ----------

# DBTITLE 1,Q42: String Functions
# Q42: String functions on emp_name — substring (first 3 chars), upper, lower, length
# Hint: substring(col, 1, 3), upper(), lower(), length()

# YOUR CODE BELOW:
employees.withColumns({'sub_string_emp_name': substring(col('emp_name'), 1, 3),
                       'upper_sub_string_emp_name': upper('sub_string_emp_name'), 
                       'lower_sub_string_emp_name': lower('sub_string_emp_name'), 
                       'length_sub_string_emp_name': length('sub_string_emp_name')
                       }).show()

# COMMAND ----------

# DBTITLE 1,Q43: regexp_replace/extract
# Q43: regexp_replace — replace vowels with "*"
# regexp_extract — extract first 3 chars using regex group
# Hint: regexp_replace(col, "[aeiouAEIOU]", "*"), regexp_extract(col, "^([A-Za-z]{3})", 1)

# YOUR CODE BELOW:
employees.withColumns({"regexp_replace": regexp_replace('emp_name',  "[aeiouAEIOU]", "*"),
                       "regexp_extract": regexp_extract('emp_name',  "^([A-Za-z]{3})", 1)
                       }).show()

# COMMAND ----------

# DBTITLE 1,Q44: Date Functions
# Q44: Date functions on orders — cast order_date to date first
# Get: date_add +7, date_sub -7, add_months +1
# Hint: col("order_date").cast("date"), date_add(), date_sub(), add_months()

# YOUR CODE BELOW
orders.withColumns({'order_date': col('order_date').cast("date"),
                    "date_add": date_add(col('order_date'), 7),
                    'date_sub': date_sub('order_date', 7),
                    'add_months': add_months('order_date', 1)
                    }).show()

# COMMAND ----------

# DBTITLE 1,Q45: Date Trunc/Format
# Q45: More date functions on orders (cast order_date to date first)
# Get: month_start (date_trunc), month_end (last_day), months_ago (months_between), year_month (date_format)
# Hint: date_trunc("month", col), last_day(col), months_between(current_date(), col), date_format(col, "yyyy-MM")

# YOUR CODE BELOW:
orders.withColumns({
    'order_date': col('order_date').cast('date'),
    'date_trunc': date_trunc("month", col('order_date')),
    'last_day': last_day(col("order_date")),
    'months_between': months_between(current_date(), col('order_date')),
    'date_format': date_format(col('order_date'), 'yyyy-MM')
}).show()

# COMMAND ----------

# DBTITLE 1,Q46: row_number()
# Q46: row_number() — rank employees by salary desc within each department
# Show: department, emp_name, salary, rn
# Hint: Window.partitionBy("department").orderBy(col("salary").desc()), row_number().over(w)

# YOUR CODE BELOW:
w = Window.partitionBy("department").orderBy(col("salary").desc())
employees.withColumn('rn', row_number().over(w)).select('department', 'emp_name', 'salary', 'rn').show()

# COMMAND ----------

# DBTITLE 1,Q47: rank vs dense_rank
# Q47: rank() vs dense_rank() vs row_number() — show the difference with ties
# Create: [("Alice", 100), ("Bob", 90), ("Charlie", 90), ("Diana", 80)]
# Hint: Window.orderBy(col("score").desc()), rank(), dense_rank(), row_number()
# rank skips (1,2,2,4), dense_rank doesn't (1,2,2,3), row_number is unique (1,2,3,4)

# YOUR CODE BELOW:
df = spark.createDataFrame([("Alice", 100), ("Bob", 90), ("Charlie", 90), ("Diana", 80)], schema=['name', 'score'])
w = Window.orderBy(col("score").desc())
df.withColumns({
    'rank': rank().over(w),
    'dense_rank':dense_rank().over(w),
    'row_number': row_number().over(w)
}).show()

# COMMAND ----------

# DBTITLE 1,Q48: lag/lead
# Q48: lag() and lead() — compare each employee salary with prev/next in department
# Also compute salary_diff = salary - lag(salary) within department ordered by salary asc
# Hint: Window.partitionBy("department").orderBy(col("salary")), lag(), lead()

# YOUR CODE BELOW:
w = Window.partitionBy('department').orderBy(col('salary'))
employees.withColumns({
    'prev_sal': lag('salary', 1).over(w),
    'next_sal': lead('salary', 1).over(w),
    'salary_diff': col('salary') - col('prev_sal')
}).show()

# COMMAND ----------

# DBTITLE 1,Q49: Running Total
# Q49: Running total with sum() OVER — cumulative salary per department ordered by salary asc
# Show: department, emp_name, salary, running_total
# Hint: Window.partitionBy().orderBy().rowsBetween(Window.unboundedPreceding, Window.currentRow)

# YOUR CODE BELOW:
w = Window.partitionBy('department').orderBy(col('salary').asc()) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

employees.withColumn('running_total', sum('salary').over(w)) \
    .select('department', 'emp_name', 'salary', 'running_total').show()


# COMMAND ----------

# DBTITLE 1,Q50: Window Multi-Agg
# Q50: Top 2 highest paid per department + salary_pct of dept total + dept avg
# Show: department, emp_name, salary, dept_avg, salary_pct, rn
# Hint: Two windows — one partitionBy for dept_avg/dept_total, one partitionBy+orderBy for rank
# salary_pct = round(salary / sum(salary).over(w) * 100, 2), filter rn <= 2

# YOUR CODE BELOW:
w_part = Window.partitionBy('department')
w_rank = Window.partitionBy('department').orderBy(col('salary').desc())

employees.withColumn('dept_avg', avg('salary').over(w_part)) \
        .withColumn('salary_pct', round(col('salary') / sum('salary').over(w_part) * 100, 2)) \
        .withColumn('rn', row_number().over(w_rank)) \
        .filter(col('rn') <= 2) \
        .select('department', 'emp_name', 'salary', 'dept_avg', 'salary_pct', 'rn') \
        .orderBy('department', 'rn').show()
