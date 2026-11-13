# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,50 PySpark & SQL Interview Questions
# MAGIC %md
# MAGIC # 50 PySpark & SQL Interview Questions
# MAGIC
# MAGIC 50 interview questions solvable in **both SQL and PySpark**. Each section has one sample-data cell (DataFrames + temp views), followed by 5 questions with expected output. Solve each question in whichever API you prefer.

# COMMAND ----------

# DBTITLE 1,Section 1 Sample Data: employees
# ===== Section 1: Filtering, Selection & Nulls (Q1-Q10) =====
employees = spark.createDataFrame(
    [(1, "Amit", "Sales", 5000, 500),
     (2, "Bhavna", "Sales", 6000, None),
     (3, "Chetan", "IT", 7000, 300),
     (4, "Deepa", "IT", 8000, None),
     (5, "Esha", "HR", 4500, None),
     (6, "Farhan", "HR", 5500, 200),
     (7, "Gita", "Sales", 6500, 400),
     (8, "Harish", "IT", 7500, None)],
    ["emp_id", "emp_name", "department", "salary", "commission"]
)
employees.createOrReplaceTempView("employees")
display(employees)

# COMMAND ----------

# DBTITLE 1,Q1: Employees Earning Above 5000
# MAGIC %md
# MAGIC ## Q1: Employees Earning Above 5000
# MAGIC
# MAGIC **Table:** `employees` — Return `emp_id`, `emp_name`, `salary` for employees whose **salary is strictly greater than 5000**, ordered by salary ascending.
# MAGIC
# MAGIC **Expected Output (6 rows):**
# MAGIC
# MAGIC | emp_id | emp_name | salary |
# MAGIC |---|---|---|
# MAGIC | 6 | Farhan | 5500 |
# MAGIC | 2 | Bhavna | 6000 |
# MAGIC | 7 | Gita | 6500 |
# MAGIC | 3 | Chetan | 7000 |
# MAGIC | 8 | Harish | 7500 |
# MAGIC | 4 | Deepa | 8000 |

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_id, emp_name, salary from employees where salary > 5000 order by salary;

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

employees.filter(col('salary') > 5000).select('emp_id', 'emp_name', 'salary').orderBy('salary').show()

# COMMAND ----------

# DBTITLE 1,Q2: Sales Employees With Commission
# MAGIC %md
# MAGIC ## Q2: Sales Employees With Commission
# MAGIC
# MAGIC **Table:** `employees` — Return `emp_id`, `emp_name`, `salary`, `commission` for employees in the **Sales department who have a non-null commission**.
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | emp_id | emp_name | salary | commission |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Amit | 5000 | 500 |
# MAGIC | 7 | Gita | 6500 | 400 |

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_id, emp_name, salary, commission from employees where department = 'Sales' and commission is not null;

# COMMAND ----------

employees.filter((col('department') == 'Sales') & (col('commission').isNotNull())).drop('department').show()

# COMMAND ----------

# DBTITLE 1,Q3: Names Ending With 'a'
# MAGIC %md
# MAGIC ## Q3: Names Ending With 'a'
# MAGIC
# MAGIC **Table:** `employees` — Return `emp_id`, `emp_name`, `department` for employees whose **name ends with 'a'** (use `endswith`/`rlike`).
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | emp_id | emp_name | department |
# MAGIC |---|---|---|
# MAGIC | 2 | Bhavna | Sales |
# MAGIC | 4 | Deepa | IT |
# MAGIC | 5 | Esha | HR |
# MAGIC | 7 | Gita | Sales |

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_id, emp_name, department from employees where emp_name rlike 'a$';

# COMMAND ----------

employees.filter(col('emp_name').rlike('a$')).select('emp_id', 'emp_name', 'department').show()

# COMMAND ----------

employees.filter(col('emp_name').like('%a')).select('emp_id', 'emp_name', 'department').show()

# COMMAND ----------

employees.filter(col('emp_name').endswith('a')).select('emp_id', 'emp_name', 'department').show()

# COMMAND ----------

employees.filter(col('emp_name').startswith('B')).select('emp_id', 'emp_name', 'department').show()

# COMMAND ----------

# DBTITLE 1,Q4: NULL Commission Detection
# MAGIC %md
# MAGIC ## Q4: NULL Commission Detection
# MAGIC
# MAGIC **Table:** `employees` — Return `emp_id`, `emp_name`, `department` for employees whose **commission is NULL** (careful: `= NULL` does not work in SQL; use `IS NULL`).
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | emp_id | emp_name | department |
# MAGIC |---|---|---|
# MAGIC | 2 | Bhavna | Sales |
# MAGIC | 4 | Deepa | IT |
# MAGIC | 5 | Esha | HR |
# MAGIC | 8 | Harish | IT |

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_id, emp_name, department from employees where commission is null;

# COMMAND ----------

employees.filter(col('commission').isNull()).select('emp_id', 'emp_name', 'department').show()

# COMMAND ----------

# DBTITLE 1,Q5: Total Payout With NULL Replacement
# MAGIC %md
# MAGIC ## Q5: Total Payout With NULL Replacement
# MAGIC
# MAGIC **Table:** `employees` — Return `emp_name` and `total_payout = salary + commission`, treating NULL commission as **0** (use `coalesce`/`fillna`).
# MAGIC
# MAGIC **Expected Output (8 rows):**
# MAGIC
# MAGIC | emp_name | total_payout |
# MAGIC |---|---|
# MAGIC | Amit | 5500 |
# MAGIC | Bhavna | 6000 |
# MAGIC | Chetan | 7300 |
# MAGIC | Deepa | 8000 |
# MAGIC | Esha | 4500 |
# MAGIC | Farhan | 5700 |
# MAGIC | Gita | 6900 |
# MAGIC | Harish | 7500 |

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_name, salary + coalesce(commission, 0) total_payout from employees;

# COMMAND ----------

employees.fillna(0, 'commission').withColumn('total_payout', col('salary') + col('commission')).select('emp_name', 'total_payout').show()

# COMMAND ----------

# DBTITLE 1,Q6: Headcount and Avg Salary Per Department
# MAGIC %md
# MAGIC ## Q6: Headcount and Average Salary Per Department
# MAGIC
# MAGIC **Table:** `employees` — Return `department`, `headcount`, and `avg_salary` (rounded to 2 decimals) per department.
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | department | headcount | avg_salary |
# MAGIC |---|---|---|
# MAGIC | Sales | 3 | 5833.33 |
# MAGIC | IT | 3 | 7500.0 |
# MAGIC | HR | 2 | 5000.0 |

# COMMAND ----------

# MAGIC %sql
# MAGIC select department, count(*) headcount, round(avg(salary), 2) avg_salary from employees group by 1;

# COMMAND ----------

employees.groupBy('department').agg(count('*').alias('headcount'), round(avg('salary'), 2).alias('avg_salary')).show()

# COMMAND ----------

# DBTITLE 1,Q7: Department With Highest Avg Salary
# MAGIC %md
# MAGIC ## Q7: Department With the Highest Average Salary
# MAGIC
# MAGIC **Table:** `employees` — Return only the department with the **highest average salary**, with that average.
# MAGIC
# MAGIC **Expected Output (1 row):**
# MAGIC
# MAGIC | department | avg_salary |
# MAGIC |---|---|
# MAGIC | IT | 7500.0 |

# COMMAND ----------

# MAGIC %sql
# MAGIC select department, avg(salary) avg_salary from employees group by 1 order by avg_salary desc limit 1;

# COMMAND ----------

employees.groupBy('department').agg(avg('salary').alias('avg_salary')).orderBy(col('avg_salary').desc()).limit(1).show()

# COMMAND ----------

# DBTITLE 1,Q8: Highest Paid Per Department
# MAGIC %md
# MAGIC ## Q8: Highest Paid Employee Per Department
# MAGIC
# MAGIC **Table:** `employees` — Return `department`, `emp_name`, `salary` for the **highest paid employee in each department**.
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | department | emp_name | salary |
# MAGIC |---|---|---|
# MAGIC | Sales | Gita | 6500 |
# MAGIC | IT | Deepa | 8000 |
# MAGIC | HR | Farhan | 5500 |

# COMMAND ----------

# MAGIC %sql
# MAGIC select department, emp_name, salary from employees where salary in (select max(salary) max_salary from employees group by department);

# COMMAND ----------

max_sal_df = employees.groupBy('department').agg(max('salary').alias('max_sal'))
employees.alias('a').join(max_sal_df.alias('b'), 'department', 'left')\
    .filter(col('salary') == col('max_sal'))\
        .select('department', 'emp_name', 'salary').show()

# COMMAND ----------

# DBTITLE 1,Q9: High Earners Per Department
# MAGIC %md
# MAGIC ## Q9: Count of High Earners Per Department
# MAGIC
# MAGIC **Table:** `employees` — Return `department` and `high_earners` = count of employees with **salary > 5000** per department.
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | department | high_earners |
# MAGIC |---|---|
# MAGIC | Sales | 2 |
# MAGIC | IT | 3 |
# MAGIC | HR | 1 |

# COMMAND ----------

# MAGIC %sql
# MAGIC select department, count(*) high_earners from employees where salary > 5000 group by 1;

# COMMAND ----------

employees.filter(col('salary')>5000).groupBy('department').agg(count('*').alias('high_earners')).show()

# COMMAND ----------

# DBTITLE 1,Q10: Total Salary Per Department Desc
# MAGIC %md
# MAGIC ## Q10: Total Salary Per Department, Descending
# MAGIC
# MAGIC **Table:** `employees` — Return `department` and `total_salary`, ordered by total salary **descending**.
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | department | total_salary |
# MAGIC |---|---|
# MAGIC | IT | 22500 |
# MAGIC | Sales | 17500 |
# MAGIC | HR | 10000 |

# COMMAND ----------

# MAGIC %sql
# MAGIC select department, sum(salary) total_salary from employees group by 1 order by total_salary desc;

# COMMAND ----------

employees.groupBy('department').agg(sum('salary').alias('total_salary')).orderBy(col('total_salary').desc()).show()

# COMMAND ----------

# DBTITLE 1,Section 3 Sample Data: emp, dept
# ===== Section 3: Joins (Q11-Q15) =====
emp = spark.createDataFrame(
    [(1, "Amit", 10, 5000),
     (2, "Bhavna", 10, 6000),
     (3, "Chetan", 20, 7000),
     (4, "Deepa", 20, 8000),
     (5, "Esha", 30, 4500),
     (6, "Farhan", 30, 5500),
     (7, "Gita", 50, 6500)],
    ["emp_id", "emp_name", "dept_id", "salary"]
)
dept = spark.createDataFrame(
    [(10, "Sales", "Mumbai"),
     (20, "IT", "Bangalore"),
     (30, "HR", "Delhi"),
     (40, "Finance", "Pune")],
    ["dept_id", "dept_name", "location"]
)
emp.createOrReplaceTempView("emp")
dept.createOrReplaceTempView("dept")
display(emp)
display(dept)

# COMMAND ----------

# DBTITLE 1,Q11: Inner Join
# MAGIC %md
# MAGIC ## Q11: Inner Join — Employees With Department Details
# MAGIC
# MAGIC **Tables:** `emp`, `dept` — Return `emp_name`, `dept_name`, `location` for every employee who **has a matching department**.
# MAGIC
# MAGIC **Expected Output (6 rows):**
# MAGIC
# MAGIC | emp_name | dept_name | location |
# MAGIC |---|---|---|
# MAGIC | Amit | Sales | Mumbai |
# MAGIC | Bhavna | Sales | Mumbai |
# MAGIC | Chetan | IT | Bangalore |
# MAGIC | Deepa | IT | Bangalore |
# MAGIC | Esha | HR | Delhi |
# MAGIC | Farhan | HR | Delhi |

# COMMAND ----------

# DBTITLE 1,Q12: Left Join Headcount
# MAGIC %md
# MAGIC ## Q12: Left Join — Department Headcount (Keep Zero Counts)
# MAGIC
# MAGIC **Tables:** `emp`, `dept` — Return `dept_name` and `employee_count` for **every department**, including departments with 0 employees.
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | dept_name | employee_count |
# MAGIC |---|---|
# MAGIC | Sales | 2 |
# MAGIC | IT | 2 |
# MAGIC | HR | 2 |
# MAGIC | Finance | 0 |

# COMMAND ----------

# DBTITLE 1,Q13: Departments With No Employees
# MAGIC %md
# MAGIC ## Q13: Departments With No Employees
# MAGIC
# MAGIC **Tables:** `emp`, `dept` — Return `dept_name`, `location` for departments that have **no employees at all**.
# MAGIC
# MAGIC **Expected Output (1 row):**
# MAGIC
# MAGIC | dept_name | location |
# MAGIC |---|---|
# MAGIC | Finance | Pune |

# COMMAND ----------

# DBTITLE 1,Q14: Employees With No Department
# MAGIC %md
# MAGIC ## Q14: Employees With No Valid Department
# MAGIC
# MAGIC **Tables:** `emp`, `dept` — Return `emp_name`, `dept_id` for employees whose `dept_id` has **no match in `dept`**.
# MAGIC
# MAGIC **Expected Output (1 row):**
# MAGIC
# MAGIC | emp_name | dept_id |
# MAGIC |---|---|
# MAGIC | Gita | 50 |

# COMMAND ----------

# DBTITLE 1,Q15: Above Department Average
# MAGIC %md
# MAGIC ## Q15: Employees Earning Above Their Department Average
# MAGIC
# MAGIC **Tables:** `emp` — Classic interview question: return `emp_name`, `salary` for employees whose salary is **greater than the average salary of their own department**.
# MAGIC
# MAGIC *Hint: dept 10 avg = 5500, dept 20 avg = 7500, dept 30 avg = 5000.*
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | emp_name | salary |
# MAGIC |---|---|
# MAGIC | Bhavna | 6000 |
# MAGIC | Deepa | 8000 |
# MAGIC | Farhan | 5500 |

# COMMAND ----------

# DBTITLE 1,Section 4 Sample Data: monthly_sales, transactions
# ===== Section 4: Window Functions (Q16-Q20) =====
# Q16 and Q17 reuse `employees` from Section 1
monthly_sales = spark.createDataFrame(
    [(1, "2024-01", 1000),
     (2, "2024-02", 1500),
     (3, "2024-03", 1200),
     (4, "2024-04", 2000)],
    ["sale_id", "month", "amount"]
)
transactions = spark.createDataFrame(
    [(1, "A", 100),
     (2, "B", 200),
     (1, "A", 100),
     (3, "C", 300),
     (2, "B", 200)],
    ["txn_id", "customer", "amount"]
)
monthly_sales.createOrReplaceTempView("monthly_sales")
transactions.createOrReplaceTempView("transactions")
display(monthly_sales)
display(transactions)

# COMMAND ----------

# DBTITLE 1,Q16: Second Highest Salary
# MAGIC %md
# MAGIC ## Q16: Second Highest Salary
# MAGIC
# MAGIC **Table:** `employees` (Section 1) — Return `emp_name`, `salary` of the employee with the **second highest salary**. Solve with `dense_rank()` or `offset` — not by calling `max()` twice.
# MAGIC
# MAGIC **Expected Output (1 row):**
# MAGIC
# MAGIC | emp_name | salary |
# MAGIC |---|---|
# MAGIC | Harish | 7500 |

# COMMAND ----------

# DBTITLE 1,Q17: Top 2 Earners Per Department
# MAGIC %md
# MAGIC ## Q17: Top 2 Earners Per Department
# MAGIC
# MAGIC **Table:** `employees` (Section 1) — Return `department`, `emp_name`, `salary` for the **top 2 earners in each department** (use `dense_rank` or `row_number`).
# MAGIC
# MAGIC **Expected Output (6 rows):**
# MAGIC
# MAGIC | department | emp_name | salary |
# MAGIC |---|---|---|
# MAGIC | IT | Deepa | 8000 |
# MAGIC | IT | Harish | 7500 |
# MAGIC | Sales | Gita | 6500 |
# MAGIC | Sales | Bhavna | 6000 |
# MAGIC | HR | Farhan | 5500 |
# MAGIC | HR | Esha | 4500 |

# COMMAND ----------

# DBTITLE 1,Q18: Dedup With row_number
# MAGIC %md
# MAGIC ## Q18: Remove Duplicates Using row_number
# MAGIC
# MAGIC **Table:** `transactions` — Remove duplicate rows using a **window function** (`row_number` over all columns), not `dropDuplicates`. Return the deduplicated `txn_id`, `customer`, `amount`.
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | txn_id | customer | amount |
# MAGIC |---|---|---|
# MAGIC | 1 | A | 100 |
# MAGIC | 2 | B | 200 |
# MAGIC | 3 | C | 300 |

# COMMAND ----------

# DBTITLE 1,Q19: Month-Over-Month Growth
# MAGIC %md
# MAGIC ## Q19: Month-Over-Month Growth
# MAGIC
# MAGIC **Table:** `monthly_sales` — Return `month`, `amount`, `growth_pct` = percentage change vs the previous month, **rounded to 2 decimals** (use `lag`). First month shows NULL.
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | month | amount | growth_pct |
# MAGIC |---|---|---|
# MAGIC | 2024-01 | 1000 | null |
# MAGIC | 2024-02 | 1500 | 50.0 |
# MAGIC | 2024-03 | 1200 | -20.0 |
# MAGIC | 2024-04 | 2000 | 66.67 |

# COMMAND ----------

# DBTITLE 1,Q20: Running Total
# MAGIC %md
# MAGIC ## Q20: Running Total
# MAGIC
# MAGIC **Table:** `monthly_sales` — Return `month`, `amount`, `running_total` = cumulative sum of `amount` by month order (use a window with `rowsBetween(unboundedPreceding, currentRow)`).
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | month | amount | running_total |
# MAGIC |---|---|---|
# MAGIC | 2024-01 | 1000 | 1000 |
# MAGIC | 2024-02 | 1500 | 2500 |
# MAGIC | 2024-03 | 1200 | 3700 |
# MAGIC | 2024-04 | 2000 | 5700 |

# COMMAND ----------

# DBTITLE 1,Section 5 Sample Data: products, emails, shipments, inventory
# ===== Section 5: Strings, Dates & NULL Handling (Q21-Q25) =====
products = spark.createDataFrame(
    [(1, " Wireless Mouse ", 299.99),
     (2, "Keyboard ", 499.0),
     (3, "  Monitor", 399.5),
     (4, " USB Hub  ", 199.0)],
    ["product_id", "product_name", "price"]
)
emails = spark.createDataFrame(
    [(1, "alice@gmail.com"),
     (2, "bob@yahoo.com"),
     (3, "carol@gmail.com"),
     (4, "dave@outlook.com")],
    ["user_id", "email"]
)
shipments = spark.createDataFrame(
    [(1, "2024-01-05", "2024-01-08"),
     (2, "2024-01-20", "2024-01-21"),
     (3, "2024-02-14", "2024-02-19"),
     (4, "2024-03-30", "2024-04-02")],
    ["order_id", "order_date", "ship_date"]
)
inventory = spark.createDataFrame(
    [(1, "Cable", 10),
     (2, "Adapter", None),
     (3, "Mouse", 30),
     (4, "Webcam", None)],
    ["item_id", "item_name", "price"]
)
for name, df in [("products", products), ("emails", emails), ("shipments", shipments), ("inventory", inventory)]:
    df.createOrReplaceTempView(name)
display(products)
display(emails)
display(shipments)
display(inventory)

# COMMAND ----------

# DBTITLE 1,Q21: Trim Product Names
# MAGIC %md
# MAGIC ## Q21: Trim Whitespace From Product Names
# MAGIC
# MAGIC **Table:** `products` — Return `product_id` and `clean_name` = `product_name` with **leading/trailing spaces removed**.
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | product_id | clean_name |
# MAGIC |---|---|
# MAGIC | 1 | Wireless Mouse |
# MAGIC | 2 | Keyboard |
# MAGIC | 3 | Monitor |
# MAGIC | 4 | USB Hub |

# COMMAND ----------

# DBTITLE 1,Q22: Extract Email Domain
# MAGIC %md
# MAGIC ## Q22: Extract Email Domain
# MAGIC
# MAGIC **Table:** `emails` — Return `user_id`, `email`, `domain` (the part after `@`, e.g. `gmail.com`). Use `split`/`substring_index`, not a UDF.
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | user_id | email | domain |
# MAGIC |---|---|---|
# MAGIC | 1 | alice@gmail.com | gmail.com |
# MAGIC | 2 | bob@yahoo.com | yahoo.com |
# MAGIC | 3 | carol@gmail.com | gmail.com |
# MAGIC | 4 | dave@outlook.com | outlook.com |

# COMMAND ----------

# DBTITLE 1,Q23: Orders in January 2024
# MAGIC %md
# MAGIC ## Q23: Orders Placed in January 2024
# MAGIC
# MAGIC **Table:** `shipments` — Return `order_id`, `order_date` for orders whose `order_date` falls in **January 2024** (cast the string to date first).
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | order_id | order_date |
# MAGIC |---|---|
# MAGIC | 1 | 2024-01-05 |
# MAGIC | 2 | 2024-01-20 |

# COMMAND ----------

# DBTITLE 1,Q24: Shipping Delay in Days
# MAGIC %md
# MAGIC ## Q24: Shipping Delay in Days
# MAGIC
# MAGIC **Table:** `shipments` — Return `order_id`, `shipping_days` = days between `order_date` and `ship_date` (use `datediff`).
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | order_id | shipping_days |
# MAGIC |---|---|
# MAGIC | 1 | 3 |
# MAGIC | 2 | 1 |
# MAGIC | 3 | 5 |
# MAGIC | 4 | 3 |

# COMMAND ----------

# DBTITLE 1,Q25: Fill NULL Prices With Zero
# MAGIC %md
# MAGIC ## Q25: Fill NULL Prices With Zero
# MAGIC
# MAGIC **Table:** `inventory` — Return `item_id`, `item_name`, `price_filled` where NULL prices become **0** (use `coalesce`).
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | item_id | item_name | price_filled |
# MAGIC |---|---|---|
# MAGIC | 1 | Cable | 10 |
# MAGIC | 2 | Adapter | 0 |
# MAGIC | 3 | Mouse | 30 |
# MAGIC | 4 | Webcam | 0 |

# COMMAND ----------

# DBTITLE 1,Section 6 Sample Data: hobbies, events, items
# ===== Section 6: Arrays, JSON & Explode (Q26-Q30) =====
hobbies = spark.createDataFrame(
    [(1, "Amit", ["cricket", "music"]),
     (2, "Bhavna", ["music", "dance"]),
     (3, "Chetan", ["chess"])],
    ["emp_id", "emp_name", "hobby_list"]
)
events = spark.createDataFrame(
    [(1, '{"name": "Amit", "age": 30, "city": "Mumbai"}'),
     (2, '{"name": "Bhavna", "age": 25, "city": "Pune"}')],
    ["event_id", "payload"]
)
items = spark.createDataFrame(
    [(1, "apple,banana,cherry"),
     (2, "dog,cat")],
    ["item_id", "tags"]
)
for name, df in [("hobbies", hobbies), ("events", events), ("items", items)]:
    df.createOrReplaceTempView(name)
display(hobbies)
display(events)
display(items)

# COMMAND ----------

# DBTITLE 1,Q26: Explode Hobbies and Count
# MAGIC %md
# MAGIC ## Q26: Explode Hobbies and Count Each
# MAGIC
# MAGIC **Table:** `hobbies` — Explode `hobby_list` into one row per hobby, then return `hobby`, `hobby_count` ordered by count descending, then hobby ascending.
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | hobby | hobby_count |
# MAGIC |---|---|
# MAGIC | music | 2 |
# MAGIC | chess | 1 |
# MAGIC | cricket | 1 |
# MAGIC | dance | 1 |

# COMMAND ----------

# DBTITLE 1,Q27: People Per Hobby
# MAGIC %md
# MAGIC ## Q27: Reverse Grouping — People Per Hobby
# MAGIC
# MAGIC **Table:** `hobbies` — Explode the array, then group by hobby and return `hobby` and `people` = list of employee names who have that hobby (use `collect_list`).
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | hobby | people |
# MAGIC |---|---|
# MAGIC | cricket | [Amit] |
# MAGIC | music | [Amit, Bhavna] |
# MAGIC | dance | [Bhavna] |
# MAGIC | chess | [Chetan] |

# COMMAND ----------

# DBTITLE 1,Q28: Parse JSON Column
# MAGIC %md
# MAGIC ## Q28: Parse a JSON Column
# MAGIC
# MAGIC **Table:** `events` — `payload` is a JSON string. Return `event_id`, `name`, `age`, `city` extracted from the JSON (use `from_json` with a schema, or `get_json_object`).
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | event_id | name | age | city |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Amit | 30 | Mumbai |
# MAGIC | 2 | Bhavna | 25 | Pune |

# COMMAND ----------

# DBTITLE 1,Q29: People With 2+ Hobbies
# MAGIC %md
# MAGIC ## Q29: People With 2 or More Hobbies
# MAGIC
# MAGIC **Table:** `hobbies` — Return `emp_name` and `hobby_count` = size of `hobby_list`, for people with **2 or more hobbies** (use `size`/`array_size`).
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | emp_name | hobby_count |
# MAGIC |---|---|
# MAGIC | Amit | 2 |
# MAGIC | Bhavna | 2 |

# COMMAND ----------

# DBTITLE 1,Q30: Split and Explode Tags
# MAGIC %md
# MAGIC ## Q30: Split a Comma-Separated Column Into Rows
# MAGIC
# MAGIC **Table:** `items` — `tags` is a comma-separated string. Split it into an array and explode so each `item_id` has one row per `tag`.
# MAGIC
# MAGIC **Expected Output (5 rows):**
# MAGIC
# MAGIC | item_id | tag |
# MAGIC |---|---|
# MAGIC | 1 | apple |
# MAGIC | 1 | banana |
# MAGIC | 1 | cherry |
# MAGIC | 2 | dog |
# MAGIC | 2 | cat |

# COMMAND ----------

# DBTITLE 1,Section 7 Sample Data: sales_region
# ===== Section 7: Pivot & Unpivot (Q31-Q35) =====
sales_region = spark.createDataFrame(
    [(2024, "Q1", 100),
     (2024, "Q2", 150),
     (2024, "Q3", 200),
     (2024, "Q4", 250),
     (2025, "Q1", 120),
     (2025, "Q2", 180)],
    ["year", "quarter", "sales"]
)
sales_region.createOrReplaceTempView("sales_region")
display(sales_region)

# COMMAND ----------

# DBTITLE 1,Q31: Pivot Quarters to Columns
# MAGIC %md
# MAGIC ## Q31: Pivot Quarters Into Columns
# MAGIC
# MAGIC **Table:** `sales_region` — Pivot so each row is a `year` and there is one column per quarter (`Q1`, `Q2`, `Q3`, `Q4`) with the sales value (use `pivot` in PySpark or conditional aggregation in SQL).
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | year | Q1 | Q2 | Q3 | Q4 |
# MAGIC |---|---|---|---|---|
# MAGIC | 2024 | 100 | 150 | 200 | 250 |
# MAGIC | 2025 | 120 | 180 | null | null |

# COMMAND ----------

# DBTITLE 1,Q32: Unpivot to Long Format
# MAGIC %md
# MAGIC ## Q32: Unpivot Back to Long Format
# MAGIC
# MAGIC **Table:** `sales_region` — The inverse of Q31: given one row per year/quarter/sales, your task is to reshape a pivoted table back to long format (use `stack` in SQL or `unpivot`/explode with a map in PySpark). Start from `sales_region` grouped as the pivot from Q31, then un-pivot and confirm you get the original 6 rows back.
# MAGIC
# MAGIC **Expected Output (6 rows):**
# MAGIC
# MAGIC | year | quarter | sales |
# MAGIC |---|---|---|
# MAGIC | 2024 | Q1 | 100 |
# MAGIC | 2024 | Q2 | 150 |
# MAGIC | 2024 | Q3 | 200 |
# MAGIC | 2024 | Q4 | 250 |
# MAGIC | 2025 | Q1 | 120 |
# MAGIC | 2025 | Q2 | 180 |

# COMMAND ----------

# DBTITLE 1,Q33: Max Sales Per Year
# MAGIC %md
# MAGIC ## Q33: Maximum Sales Per Year
# MAGIC
# MAGIC **Table:** `sales_region` — Return `year` and `max_sales` for each year.
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | year | max_sales |
# MAGIC |---|---|
# MAGIC | 2024 | 250 |
# MAGIC | 2025 | 180 |

# COMMAND ----------

# DBTITLE 1,Q34: Q1 Sales By Year
# MAGIC %md
# MAGIC ## Q34: Q1 Sales for Each Year
# MAGIC
# MAGIC **Table:** `sales_region` — Return `year` and `sales` where `quarter = 'Q1'`, ordered by year.
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | year | sales |
# MAGIC |---|---|
# MAGIC | 2024 | 100 |
# MAGIC | 2025 | 120 |

# COMMAND ----------

# DBTITLE 1,Q35: Total Sales Per Quarter
# MAGIC %md
# MAGIC ## Q35: Total Sales Per Quarter Across Years
# MAGIC
# MAGIC **Table:** `sales_region` — Return `quarter` and `total_sales` = sum of sales for that quarter across **all years**.
# MAGIC
# MAGIC **Expected Output (4 rows):**
# MAGIC
# MAGIC | quarter | total_sales |
# MAGIC |---|---|
# MAGIC | Q1 | 220 |
# MAGIC | Q2 | 330 |
# MAGIC | Q3 | 200 |
# MAGIC | Q4 | 250 |

# COMMAND ----------

# DBTITLE 1,Section 8 Sample Data: batches, scores
# ===== Section 8: Set Operations, Duplicates & Anti-Joins (Q36-Q40) =====
batch_2023 = spark.createDataFrame(
    [(1, "Amit"), (2, "Bhavna"), (3, "Chetan")],
    ["emp_id", "emp_name"]
)
batch_2024 = spark.createDataFrame(
    [(2, "Bhavna"), (4, "Deepa"), (5, "Esha")],
    ["emp_id", "emp_name"]
)
batch_2024_swapped = batch_2024.select("emp_name", "emp_id")  # column order reversed for Q37
scores = spark.createDataFrame(
    [(1, "Math", 80),
     (2, "Science", 90),
     (1, "Math", 80),
     (3, "Math", 70),
     (2, "Science", 90)],
    ["student_id", "subject", "score"]
)
for name, df in [("batch_2023", batch_2023), ("batch_2024", batch_2024), ("scores", scores)]:
    df.createOrReplaceTempView(name)
display(batch_2023)
display(batch_2024)
display(scores)

# COMMAND ----------

# DBTITLE 1,Q36: Union vs Union Distinct
# MAGIC %md
# MAGIC ## Q36: Union vs Union Distinct
# MAGIC
# MAGIC **Tables:** `batch_2023`, `batch_2024` — Combine both batches. `union` (all) keeps duplicates → 6 rows; `union` with distinct (or SQL `UNION`) removes duplicates → 5 rows. Return the **distinct** result.
# MAGIC
# MAGIC **Expected Output (5 rows):**
# MAGIC
# MAGIC | emp_id | emp_name |
# MAGIC |---|---|
# MAGIC | 1 | Amit |
# MAGIC | 2 | Bhavna |
# MAGIC | 3 | Chetan |
# MAGIC | 4 | Deepa |
# MAGIC | 5 | Esha |

# COMMAND ----------

# DBTITLE 1,Q37: unionByName
# MAGIC %md
# MAGIC ## Q37: unionByName With Different Column Order
# MAGIC
# MAGIC **Tables:** `batch_2023`, `batch_2024_swapped` — `batch_2024_swapped` has columns in reverse order `(emp_name, emp_id)`. Combine it with `batch_2023` so columns are matched **by name, not position** (use `unionByName`).
# MAGIC
# MAGIC **Expected Output (6 rows):**
# MAGIC
# MAGIC | emp_id | emp_name |
# MAGIC |---|---|
# MAGIC | 1 | Amit |
# MAGIC | 2 | Bhavna |
# MAGIC | 3 | Chetan |
# MAGIC | 2 | Bhavna |
# MAGIC | 4 | Deepa |
# MAGIC | 5 | Esha |

# COMMAND ----------

# DBTITLE 1,Q38: Anti-Join
# MAGIC %md
# MAGIC ## Q38: Anti-Join — Present in 2023 But Not 2024
# MAGIC
# MAGIC **Tables:** `batch_2023`, `batch_2024` — Return `emp_id`, `emp_name` for employees in `batch_2023` who are **not present** in `batch_2024` (use `LEFT JOIN ... IS NULL` or `NOT EXISTS` or `left_anti`).
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | emp_id | emp_name |
# MAGIC |---|---|
# MAGIC | 1 | Amit |
# MAGIC | 3 | Chetan |

# COMMAND ----------

# DBTITLE 1,Q39: Intersection
# MAGIC %md
# MAGIC ## Q39: Intersection — Common Employees
# MAGIC
# MAGIC **Tables:** `batch_2023`, `batch_2024` — Return `emp_id`, `emp_name` for employees present in **both** tables (use `INTERSECT` or an inner join).
# MAGIC
# MAGIC **Expected Output (1 row):**
# MAGIC
# MAGIC | emp_id | emp_name |
# MAGIC |---|---|
# MAGIC | 2 | Bhavna |

# COMMAND ----------

# DBTITLE 1,Q40: dropDuplicates
# MAGIC %md
# MAGIC ## Q40: Deduplicate Rows (dropDuplicates)
# MAGIC
# MAGIC **Table:** `scores` — Remove exact duplicate rows using `dropDuplicates` (all columns), returning `student_id`, `subject`, `score`.
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | student_id | subject | score |
# MAGIC |---|---|---|
# MAGIC | 1 | Math | 80 |
# MAGIC | 2 | Science | 90 |
# MAGIC | 3 | Math | 70 |

# COMMAND ----------

# DBTITLE 1,Section 9 Sample Data: order_items, test_scores
# ===== Section 9: Advanced Aggregation (Q41-Q45) =====
# Q43 and Q45 reuse `employees` from Section 1
order_items = spark.createDataFrame(
    [(1, "A", 120),
     (2, "A", 150),
     (3, "B", 20),
     (4, "B", 200),
     (5, "C", 300),
     (6, "C", 50)],
    ["order_id", "customer", "amount"]
)
test_scores = spark.createDataFrame(
    [(10,), (20,), (30,), (20,), (20,)],
    ["score"]
)
order_items.createOrReplaceTempView("order_items")
test_scores.createOrReplaceTempView("test_scores")
display(order_items)
display(test_scores)

# COMMAND ----------

# DBTITLE 1,Q41: Conditional Aggregation
# MAGIC %md
# MAGIC ## Q41: Conditional Aggregation — Total and Big Orders Per Customer
# MAGIC
# MAGIC **Table:** `order_items` — Return `customer`, `total_amount`, and `big_orders` = count of orders where `amount > 100`, in a **single aggregation** (use `sum(case when ...)` / `count(case when ...)` or `F.when` inside agg).
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | customer | total_amount | big_orders |
# MAGIC |---|---|---|
# MAGIC | A | 270 | 2 |
# MAGIC | B | 220 | 1 |
# MAGIC | C | 350 | 1 |

# COMMAND ----------

# DBTITLE 1,Q42: Percentage of Total
# MAGIC %md
# MAGIC ## Q42: Percentage of Total Per Customer
# MAGIC
# MAGIC **Table:** `order_items` — Return `customer`, `total_amount`, and `pct_of_total` = customer total / grand total × 100, **rounded to 2 decimals** (grand total = 840).
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | customer | total_amount | pct_of_total |
# MAGIC |---|---|---|
# MAGIC | A | 270 | 32.14 |
# MAGIC | B | 220 | 26.19 |
# MAGIC | C | 350 | 41.67 |

# COMMAND ----------

# DBTITLE 1,Q43: Median Salary Per Department
# MAGIC %md
# MAGIC ## Q43: Median Salary Per Department
# MAGIC
# MAGIC **Table:** `employees` (Section 1) — Return `department` and `median_salary` (use `percentile`/`percentile_approx` or the `median` function). With an even number of rows (HR has 2), the median is the average of the middle two values.
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | department | median_salary |
# MAGIC |---|---|
# MAGIC | Sales | 6000 |
# MAGIC | IT | 7500 |
# MAGIC | HR | 5000 |

# COMMAND ----------

# DBTITLE 1,Q44: Mode of Test Scores
# MAGIC %md
# MAGIC ## Q44: Mode of Test Scores
# MAGIC
# MAGIC **Table:** `test_scores` — Return the `score` value that occurs **most frequently** and its count.
# MAGIC
# MAGIC **Expected Output (1 row):**
# MAGIC
# MAGIC | score | count |
# MAGIC |---|---|
# MAGIC | 20 | 3 |

# COMMAND ----------

# DBTITLE 1,Q45: Salary Bands
# MAGIC %md
# MAGIC ## Q45: Salary Bands (Binning)
# MAGIC
# MAGIC **Table:** `employees` (Section 1) — Classify each salary into a band — `Low (<5000)`, `Mid (5000-7000 inclusive)`, `High (>7000)` — and return `band` and `headcount` (use `when`/`CASE`).
# MAGIC
# MAGIC **Expected Output (3 rows):**
# MAGIC
# MAGIC | band | headcount |
# MAGIC |---|---|
# MAGIC | Low (<5000) | 1 |
# MAGIC | Mid (5000-7000) | 5 |
# MAGIC | High (>7000) | 2 |

# COMMAND ----------

# DBTITLE 1,Section 10 Sample Data: logins, signups, first_orders
# ===== Section 10: Streaks, Gaps & Real-World Scenarios (Q46-Q50) =====
logins = spark.createDataFrame(
    [(1, "A", "2024-01-01"),
     (2, "A", "2024-01-02"),
     (3, "A", "2024-01-03"),
     (4, "A", "2024-01-07"),
     (5, "A", "2024-01-08"),
     (6, "B", "2024-01-01"),
     (7, "B", "2024-01-04")],
    ["login_id", "user", "login_date"]
)
signups = spark.createDataFrame(
    [(1, "A", "2024-01-01"), (2, "B", "2024-01-05")],
    ["user_id", "user", "signup_date"]
)
first_orders = spark.createDataFrame(
    [(1, "A", "2024-01-03"), (2, "B", "2024-01-10")],
    ["user_id", "user", "first_order_date"]
)
for name, df in [("logins", logins), ("signups", signups), ("first_orders", first_orders)]:
    df.createOrReplaceTempView(name)
display(logins)
display(signups)
display(first_orders)

# COMMAND ----------

# DBTITLE 1,Q46: Longest Login Streak
# MAGIC %md
# MAGIC ## Q46: Longest Consecutive-Day Login Streak
# MAGIC
# MAGIC **Table:** `logins` — For each user, find the **longest streak of consecutive login days** (use `lag` + date arithmetic to group consecutive days — the classic "gaps and islands" pattern).
# MAGIC
# MAGIC *User A: Jan 1-3 (streak 3), Jan 7-8 (streak 2) → max 3. User B: single days → 1.*
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | user | max_streak_days |
# MAGIC |---|---|
# MAGIC | A | 3 |
# MAGIC | B | 1 |

# COMMAND ----------

# DBTITLE 1,Q47: Biggest Login Gap
# MAGIC %md
# MAGIC ## Q47: Biggest Gap Between Consecutive Logins
# MAGIC
# MAGIC **Table:** `logins` — For each user, find the **maximum gap in days** between two consecutive logins (use `lag` + `datediff`).
# MAGIC
# MAGIC *User A: gaps are 1,1,4,1 → max 4. User B: gap 3 → max 3.*
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | user | max_gap_days |
# MAGIC |---|---|
# MAGIC | A | 4 |
# MAGIC | B | 3 |

# COMMAND ----------

# DBTITLE 1,Q48: First and Last Login
# MAGIC %md
# MAGIC ## Q48: First and Last Login Per User
# MAGIC
# MAGIC **Table:** `logins` — Return `user`, `first_login`, `last_login` for each user (use `min`/`max` aggregates or `first_value`/`last_value` window functions).
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | user | first_login | last_login |
# MAGIC |---|---|---|
# MAGIC | A | 2024-01-01 | 2024-01-08 |
# MAGIC | B | 2024-01-01 | 2024-01-04 |

# COMMAND ----------

# DBTITLE 1,Q49: Users With Consecutive Logins
# MAGIC %md
# MAGIC ## Q49: Users With At Least 2 Consecutive Login Days
# MAGIC
# MAGIC **Table:** `logins` — Return the `user`(s) who logged in on **at least 2 consecutive days** at some point.
# MAGIC
# MAGIC **Expected Output (1 row):**
# MAGIC
# MAGIC | user |
# MAGIC |---|
# MAGIC | A |

# COMMAND ----------

# DBTITLE 1,Q50: Days to First Order
# MAGIC %md
# MAGIC ## Q50: Days From Signup to First Order
# MAGIC
# MAGIC **Tables:** `signups`, `first_orders` — Join on user and return `user`, `signup_date`, `first_order_date`, `days_to_first_order` (use `datediff`).
# MAGIC
# MAGIC **Expected Output (2 rows):**
# MAGIC
# MAGIC | user | signup_date | first_order_date | days_to_first_order |
# MAGIC |---|---|---|---|
# MAGIC | A | 2024-01-01 | 2024-01-03 | 2 |
# MAGIC | B | 2024-01-05 | 2024-01-10 | 5 |