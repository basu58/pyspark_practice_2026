# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 🧩 PySpark Interview Question — Medium
# MAGIC
# MAGIC ## **Problem: Find the Department with the Highest Salary Gap Between Any Two Employees**
# MAGIC
# MAGIC You are given an `employees` table with the following schema:
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `emp_id` | int | Unique employee ID |
# MAGIC | `emp_name` | string | Employee name |
# MAGIC | `department` | string | Department name |
# MAGIC | `salary` | int | Annual salary in USD |
# MAGIC
# MAGIC **Task:** For each `department`, find:
# MAGIC
# MAGIC 1. The **maximum salary gap** between any two employees (i.e., `max(salary) - min(salary)`)
# MAGIC 2. The **employee with the highest salary** (name and salary)
# MAGIC 3. The **employee with the lowest salary** (name and salary)
# MAGIC 4. The **average salary** (rounded to 2 decimal places)
# MAGIC 5. The **number of employees**
# MAGIC
# MAGIC Then, return only the **top 3 departments** ranked by salary gap (descending). If there's a tie in salary gap, break ties by average salary descending, then by department name ascending.
# MAGIC
# MAGIC Only include departments with **at least 3 employees**.
# MAGIC
# MAGIC ### Expected Output Columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC | --- | --- |
# MAGIC | `department` | Department name |
# MAGIC | `salary_gap` | max(salary) - min(salary) |
# MAGIC | `highest_paid_name` | Name of the highest-paid employee |
# MAGIC | `highest_salary` | The highest salary |
# MAGIC | `lowest_paid_name` | Name of the lowest-paid employee |
# MAGIC | `lowest_salary` | The lowest salary |
# MAGIC | `avg_salary` | Average salary, rounded to 2 decimals |
# MAGIC | `employee_count` | Number of employees in the department |
# MAGIC
# MAGIC **Order by** `salary_gap` descending, then `avg_salary` descending, then `department` ascending.
# MAGIC
# MAGIC ### Example Input
# MAGIC
# MAGIC | emp_id | emp_name | department | salary |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | Alice | Engineering | 120000 |
# MAGIC | 2 | Bob | Engineering | 95000 |
# MAGIC | 3 | Charlie | Engineering | 110000 |
# MAGIC | 4 | Diana | Engineering | 70000 |
# MAGIC | 5 | Eve | Sales | 80000 |
# MAGIC | 6 | Frank | Sales | 60000 |
# MAGIC | 7 | Grace | Sales | 90000 |
# MAGIC | 8 | Henry | Marketing | 100000 |
# MAGIC | 9 | Ivy | Marketing | 85000 |
# MAGIC | 10 | Jack | Marketing | 92000 |
# MAGIC | 11 | Karen | Marketing | 78000 |
# MAGIC
# MAGIC ### Expected Output
# MAGIC
# MAGIC | department | salary_gap | highest_paid_name | highest_salary | lowest_paid_name | lowest_salary | avg_salary | employee_count |
# MAGIC | --- | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | Engineering | 50000 | Alice | 120000 | Diana | 70000 | 98750.00 | 4 |
# MAGIC | Marketing | 22000 | Henry | 100000 | Karen | 78000 | 88750.00 | 4 |
# MAGIC | Sales | 30000 | Grace | 90000 | Frank | 60000 | 76666.67 | 3 |
# MAGIC
# MAGIC ### Key Concepts Tested
# MAGIC
# MAGIC * **Window functions** — `rank()` / `row_number()` to find highest/lowest paid employees per department
# MAGIC * **Conditional filtering** with `when()` / `filter()` on rank columns
# MAGIC * **GroupBy aggregation** — `max()`, `min()`, `avg()`, `count()` per department
# MAGIC * **Joins** — joining aggregated stats with ranked employee details
# MAGIC * **Tie-breaking** in final `orderBy()` with multiple sort keys
# MAGIC * **Rounding** with `round()`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Write your solution in the code cell below 👇

# COMMAND ----------

# ===== SETUP: Sample Data (run this first) =====
from pyspark.sql.functions import col, max as _max, min as _min, avg as _avg, count as _count, round, when, rank
from pyspark.sql.window import Window

employees_data = [
    (1, "Alice",   "Engineering", 120000),
    (2, "Bob",     "Engineering", 95000),
    (3, "Charlie", "Engineering", 120000),  # tie with Alice for highest
    (4, "Diana",   "Engineering", 70000),
    (5, "Eve",     "Sales",       60000),  # tie with Frank for lowest
    (6, "Frank",   "Sales",       60000),
    (7, "Grace",   "Sales",       90000),
    (8, "Henry",   "Marketing",   90000),  # all same salary
    (9, "Ivy",     "Marketing",   90000),
    (10, "Jack",   "Marketing",   90000),
    (11, "Karen",  "HR",          80000),  # only 2 employees — excluded
    (12, "Leo",    "HR",          70000),
    (13, "Mike",   "Finance",    130000),
    (14, "Nancy",  "Finance",    100000),
    (15, "Oscar",  "Finance",     85000),
]

columns = ["emp_id", "emp_name", "department", "salary"]
employees = spark.createDataFrame(employees_data, columns)
display(employees)

# ===== YOUR SOLUTION BELOW =====
# TODO: For each department, find the salary gap (max - min), highest/lowest paid employees,
#       average salary (rounded to 2 decimals), and employee count.
#       Only include departments with >= 3 employees.
#       Return top 3 departments by salary_gap DESC, tie-break by avg_salary DESC, then department ASC.
#
# Hints:
#   1. Use rank() with a window partitioned by department, ordered by salary DESC — rank 1 = highest paid
#   2. Use rank() with a window partitioned by department, ordered by salary ASC — rank 1 = lowest paid
#   3. GroupBy department for aggregates: max(salary), min(salary), avg(salary), count(*)
#   4. Filter the ranked DataFrames for rank == 1 to get highest/lowest paid employee names
#   5. Join aggregates with highest-paid and lowest-paid employee details
#   6. Compute salary_gap = max_salary - min_salary
#   7. Filter departments with employee_count >= 3
#   8. Order by salary_gap DESC, avg_salary DESC, department ASC, then limit(3)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expected Output
# MAGIC
# MAGIC | department | salary_gap | highest_paid_name | highest_salary | lowest_paid_name | lowest_salary | avg_salary | employee_count |
# MAGIC | --- | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | Engineering | 50000 | Alice | 120000 | Diana | 70000 | 98750.00 | 4 |
# MAGIC | Marketing | 22000 | Henry | 100000 | Karen | 78000 | 88750.00 | 4 |
# MAGIC | Sales | 30000 | Grace | 90000 | Frank | 60000 | 76666.67 | 3 |

# COMMAND ----------

w = Window.partitionBy('department').orderBy(col('salary_gap').desc(), col('avg_salary').desc(), col('department'))

grouped_data = employees.groupBy('department').\
    agg(max('salary').alias('highest_salary'), min('salary').alias('lowest_salary'),\
        avg('salary').alias('avg_salary'),\
        count_distinct(column('emp_id')).alias('employee_count')).withColumn('avg_salary', round(col('avg_salary'), 2)).\
    filter(col('employee_count') >= 3)
    

result = employees.join(grouped_data, on='department', how='left')\
    .withColumns({
        'highest_paid_name': when(col('highest_salary') == col('salary'), col('emp_name')),
        'lowest_paid_name': when(col('lowest_salary') == col('salary'), col('emp_name')),
        'salary_gap': col('highest_salary') - col('lowest_salary')
    }).filter(col('highest_paid_name').isNotNull() | col('lowest_paid_name').isNotNull())
result = result.alias('a').join(result.alias('b'), on='department', how='inner').\
    select('a.department', 'a.salary_gap', 'a.highest_paid_name', 'a.highest_salary', 'b.lowest_paid_name', 'b.lowest_salary', 'a.avg_salary', 'a.employee_count')\
        .dropna(subset = ['highest_paid_name', 'lowest_paid_name'])
w1 = Window.orderBy(col('salary_gap').desc())
result = result.withColumn('rank', dense_rank().over(w)).filter(col('rank')==1)\
    .withColumn('rank1', dense_rank().over(w1)).filter(col('rank1')<=3)\
    .drop('rank').drop('rank1')\
    .orderBy(col('salary_gap').desc())
display(result)

# COMMAND ----------


result.withColumn('rank1', dense_rank().over(w)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # ===== YOUR SOLUTION BELOW =====
# MAGIC # TODO: For each department, find the salary gap (max - min), highest/lowest paid employees,
# MAGIC #       average salary (rounded to 2 decimals), and employee count.
# MAGIC #       Only include departments with >= 3 employees.
# MAGIC #       Return top 3 departments by salary_gap DESC, tie-break by avg_salary DESC, then department ASC.

# COMMAND ----------

# ===== EDGE CASE TEST: Running YOUR CURRENT Cell 5 logic against edge cases =====
# Edge cases:
#   1. Tie in highest salary (Alice & Charlie both 120000 in Engineering)
#   2. Tie in lowest salary (Eve & Frank both 60000 in Sales)
#   3. All same salary (Marketing: all 90000) — gap=0
#   4. Department with only 2 employees (HR) — should be excluded
#   5. 4 qualifying departments — should return only top 3

edge_case_data = [
    (1, "Alice",   "Engineering", 120000),
    (2, "Bob",     "Engineering", 95000),
    (3, "Charlie", "Engineering", 120000),  # tie with Alice for highest
    (4, "Diana",   "Engineering", 70000),
    (5, "Eve",     "Sales",       60000),  # tie with Frank for lowest
    (6, "Frank",   "Sales",       60000),
    (7, "Grace",   "Sales",       90000),
    (8, "Henry",   "Marketing",   90000),  # all same salary
    (9, "Ivy",     "Marketing",   90000),
    (10, "Jack",   "Marketing",   90000),
    (11, "Karen",  "HR",          80000),  # only 2 employees — excluded
    (12, "Leo",    "HR",          70000),
    (13, "Mike",   "Finance",    130000),
    (14, "Nancy",  "Finance",    100000),
    (15, "Oscar",  "Finance",     85000),
]

employees_edge = spark.createDataFrame(edge_case_data, ["emp_id", "emp_name", "department", "salary"])
print('=== Edge case data ===')
employees_edge.show()

# --- YOUR CURRENT Cell 5 logic applied to edge case data ---
w = Window.partitionBy('department').orderBy(col('salary_gap').desc(), col('avg_salary').desc(), col('department'))

grouped_data = employees_edge.groupBy('department').\
    agg(max('salary').alias('highest_salary'), min('salary').alias('lowest_salary'),\
        avg('salary').alias('avg_salary'),\
        count_distinct(column('emp_id')).alias('employee_count')).withColumn('avg_salary', round(col('avg_salary'), 2)).\
    filter(col('employee_count') >= 3)
    
result = employees_edge.join(grouped_data, on='department', how='left')\
    .withColumns({
        'highest_paid_name': when(col('highest_salary') == col('salary'), col('emp_name')),
        'lowest_paid_name': when(col('lowest_salary') == col('salary'), col('emp_name')),
        'salary_gap': col('highest_salary') - col('lowest_salary')
    }).filter(col('highest_paid_name').isNotNull() | col('lowest_paid_name').isNotNull())

result = result.alias('a').join(result.alias('b'), on='department', how='inner').\
    select('a.department', 'a.salary_gap', 'a.highest_paid_name', 'a.highest_salary', 'b.lowest_paid_name', 'b.lowest_salary', 'a.avg_salary', 'a.employee_count')\
        .dropna(subset=['highest_paid_name', 'lowest_paid_name'])

result = result.withColumn('rank', dense_rank().over(w)).filter(col('rank')==1).drop('rank').orderBy(col('salary_gap').desc()).limit(3)

print('=== YOUR solution on edge cases ===')
result.show(50, truncate=False)
print(f'Total rows returned: {result.count()}')
print('')
print('Expected: Engineering (gap=50000), Finance (gap=45000), Sales (gap=30000)')
print('Sales (gap=30000) should be #3 but may be cut off by limit(3) due to tie duplicates')

# COMMAND ----------

# ===== REFERENCE SOLUTION: Handles all edge cases =====
# Uses row_number() instead of self-join to avoid duplicates from ties.
# Adds orderBy + limit(3) per the problem spec.

from pyspark.sql.functions import col, max as _max, min as _min, avg as _avg, count as _count, round, when, row_number
from pyspark.sql.window import Window

def solve(employees_df):
    # Step 1: Department-level aggregates
    dept_stats = (employees_df.groupBy('department')
        .agg(
            _max('salary').alias('max_sal'),
            _min('salary').alias('min_sal'),
            _avg('salary').alias('avg_salary'),
            _count('emp_id').alias('employee_count')
        )
        .withColumn('avg_salary', round(col('avg_salary'), 2))
        .withColumn('salary_gap', col('max_sal') - col('min_sal'))
        .filter(col('employee_count') >= 3)  # exclude small departments
        .drop('max_sal', 'min_sal')  # avoid ambiguous column names on join
    )

    # Step 2: Rank employees within each department by salary (both directions)
    w_desc = Window.partitionBy('department').orderBy(col('salary').desc(), col('emp_name').asc())
    w_asc = Window.partitionBy('department').orderBy(col('salary').asc(), col('emp_name').asc())

    ranked = (employees_df
        .withColumn('rn_desc', row_number().over(w_desc))  # rn_desc=1 → highest paid
        .withColumn('rn_asc',  row_number().over(w_asc))     # rn_asc=1  → lowest paid
    )

    # Step 3: Extract one highest-paid and one lowest-paid employee per department
    highest = (ranked.filter(col('rn_desc') == 1)
        .select('department', col('emp_name').alias('highest_paid_name'), col('salary').alias('highest_salary')))

    lowest = (ranked.filter(col('rn_asc') == 1)
        .select('department', col('emp_name').alias('lowest_paid_name'), col('salary').alias('lowest_salary')))

    # Step 4: Join everything together
    result = (dept_stats
        .join(highest, 'department', 'left')
        .join(lowest, 'department', 'left')
        .select(
            'department', 'salary_gap',
            'highest_paid_name', 'highest_salary',
            'lowest_paid_name', 'lowest_salary',
            'avg_salary', 'employee_count'
        )
        .orderBy(col('salary_gap').desc(), col('avg_salary').desc(), col('department').asc())
        .limit(3)
    )
    return result

# --- Run on original data ---
print('=== Reference solution on ORIGINAL data ===')
solve(employees).show(truncate=False)

# --- Run on edge case data ---
print('=== Reference solution on EDGE CASE data ===')
solve(employees_edge).show(truncate=False)

print('Expected: Engineering (gap=50000), Finance (gap=45000), Sales (gap=30000)')
print('  - Engineering: Alice (tie-break by name asc) / Diana')
print('  - Marketing excluded from top 3 (gap=0, not in top 3)')
print('  - Sales: Grace / Eve (tie-break by name asc)')
print('  - HR excluded (only 2 employees)')