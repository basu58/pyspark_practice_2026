# Databricks notebook source
# DBTITLE 1,Window Functions Practice
# MAGIC %md
# MAGIC # Window Functions Practice
# MAGIC
# MAGIC 10 LeetCode-style medium questions covering window functions: ranking (RANK / DENSE_RANK / ROW_NUMBER), running totals, LAG, moving comparisons, NTILE, and gaps-and-islands. Solve in SQL and/or PySpark — the expected output is shown in each question, and each question has its own sample-data cell.

# COMMAND ----------

# DBTITLE 1,Q1: Second Highest Salary Per Department
# MAGIC %md
# MAGIC ## Q1: Second Highest Salary Per Department
# MAGIC
# MAGIC Given the `employees` table, find the **second highest distinct salary** in each department. A department with fewer than two distinct salaries must not appear in the result. Order by department.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | department | second_highest_salary |
# MAGIC |---|---|
# MAGIC | HR | 4000 |
# MAGIC | IT | 8000 |

# COMMAND ----------

# DBTITLE 1,Q1: Sample Data
# Sample data for Q1
employees = spark.createDataFrame(
    [(1, "Ankit", "IT", 9000),
     (2, "Gopal", "IT", 9000),
     (3, "Bhavna", "IT", 7000),
     (4, "Chetan", "IT", 7000),
     (5, "Farhan", "IT", 8000),
     (6, "Divya", "HR", 5000),
     (7, "Esha", "HR", 4000),
     (8, "Tara", "Admin", 5500)],
    ["emp_id", "emp_name", "department", "salary"]
)
employees.createOrReplaceTempView("employees")
display(employees)

# COMMAND ----------

# DBTITLE 1,Q2: Running Total of Revenue
# MAGIC %md
# MAGIC ## Q2: Running Total of Revenue
# MAGIC
# MAGIC Given the `daily_sales` table, show each day's revenue together with the **cumulative revenue** up to and including that day (`running_total`), ordered by date.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | sale_date | revenue | running_total |
# MAGIC |---|---|---|
# MAGIC | 2024-01-01 | 100 | 100 |
# MAGIC | 2024-01-02 | 200 | 300 |
# MAGIC | 2024-01-03 | 150 | 450 |
# MAGIC | 2024-01-04 | 300 | 750 |
# MAGIC | 2024-01-05 | 50 | 800 |

# COMMAND ----------

# DBTITLE 1,Q2: Sample Data
# Sample data for Q2
daily_sales = spark.createDataFrame(
    [("2024-01-01", 100),
     ("2024-01-02", 200),
     ("2024-01-03", 150),
     ("2024-01-04", 300),
     ("2024-01-05", 50)],
    ["sale_date", "revenue"]
)
daily_sales.createOrReplaceTempView("daily_sales")
display(daily_sales)

# COMMAND ----------

# DBTITLE 1,Q3: Month-over-Month Revenue Growth
# MAGIC %md
# MAGIC ## Q3: Month-over-Month Revenue Growth
# MAGIC
# MAGIC Given the `monthly_sales` table, for each month show the revenue, the **previous month's revenue** (`prev_revenue`), and the **percentage growth** (`growth_pct` = (revenue − prev_revenue) / prev_revenue × 100), rounded to 2 decimal places. The first month shows NULL for both.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | sale_month | revenue | prev_revenue | growth_pct |
# MAGIC |---|---|---|---|
# MAGIC | 2024-01 | 1000 | NULL | NULL |
# MAGIC | 2024-02 | 1200 | 1000 | 20.0 |
# MAGIC | 2024-03 | 900 | 1200 | -25.0 |
# MAGIC | 2024-04 | 1500 | 900 | 66.67 |

# COMMAND ----------

# DBTITLE 1,Q3: Sample Data
# Sample data for Q3
monthly_sales = spark.createDataFrame(
    [("2024-01", 1000),
     ("2024-02", 1200),
     ("2024-03", 900),
     ("2024-04", 1500)],
    ["sale_month", "revenue"]
)
monthly_sales.createOrReplaceTempView("monthly_sales")
display(monthly_sales)

# COMMAND ----------

# DBTITLE 1,Q4: Above Department Average
# MAGIC %md
# MAGIC ## Q4: Above Department Average
# MAGIC
# MAGIC Given the `dept_employees` table, list only the employees who earn **strictly more than their department's average salary**, along with that average (`dept_avg`, rounded to 2 decimals). Order by department, then salary descending.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | emp_name | department | salary | dept_avg |
# MAGIC |---|---|---|---|
# MAGIC | Eshan | Marketing | 8000 | 6666.67 |
# MAGIC | Farida | Marketing | 8000 | 6666.67 |
# MAGIC | Bianca | Sales | 7000 | 6000.0 |

# COMMAND ----------

# DBTITLE 1,Q4: Sample Data
# Sample data for Q4
dept_employees = spark.createDataFrame(
    [(1, "Aarav", "Sales", 5000),
     (2, "Bianca", "Sales", 7000),
     (3, "Chirag", "Sales", 6000),
     (4, "Diya", "Marketing", 4000),
     (5, "Eshan", "Marketing", 8000),
     (6, "Farida", "Marketing", 8000)],
    ["emp_id", "emp_name", "department", "salary"]
)
dept_employees.createOrReplaceTempView("dept_employees")
display(dept_employees)

# COMMAND ----------

# DBTITLE 1,Q5: Top 3 Purchases Per Customer
# MAGIC %md
# MAGIC ## Q5: Top 3 Purchases Per Customer
# MAGIC
# MAGIC Given the `purchases` table, list each customer's **3 most expensive purchases** (or fewer, if they made fewer than 3). Add a `purchase_rank` column (1 = most expensive). Order by customer, then rank.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | customer | product | amount | purchase_rank |
# MAGIC |---|---|---|---|
# MAGIC | Aman | TV | 800 | 1 |
# MAGIC | Aman | Phone | 500 | 2 |
# MAGIC | Aman | Watch | 200 | 3 |
# MAGIC | Bela | Laptop | 1200 | 1 |
# MAGIC | Bela | Mouse | 20 | 2 |
# MAGIC | Chetan | Desk | 300 | 1 |
# MAGIC | Chetan | Chair | 150 | 2 |
# MAGIC | Chetan | Lamp | 90 | 3 |

# COMMAND ----------

# DBTITLE 1,Q5: Sample Data
# Sample data for Q5
purchases = spark.createDataFrame(
    [(1, "Aman", "TV", 800),
     (2, "Aman", "Phone", 500),
     (3, "Aman", "Watch", 200),
     (4, "Aman", "Cable", 30),
     (5, "Bela", "Laptop", 1200),
     (6, "Bela", "Mouse", 20),
     (7, "Chetan", "Desk", 300),
     (8, "Chetan", "Chair", 150),
     (9, "Chetan", "Lamp", 90),
     (10, "Chetan", "Rug", 60)],
    ["purchase_id", "customer", "product", "amount"]
)
purchases.createOrReplaceTempView("purchases")
display(purchases)

# COMMAND ----------

# DBTITLE 1,Q6: Days Between Consecutive Orders
# MAGIC %md
# MAGIC ## Q6: Days Between Consecutive Orders
# MAGIC
# MAGIC Given the `orders` table, for each order show the number of **days since the customer's previous order** (`days_since_prev_order`; NULL for a customer's first order). `order_date` is in `yyyy-MM-dd` format. Order by customer, then order_date.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | order_id | customer | order_date | days_since_prev_order |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Ravi | 2024-01-01 | NULL |
# MAGIC | 2 | Ravi | 2024-01-04 | 3 |
# MAGIC | 3 | Ravi | 2024-01-10 | 6 |
# MAGIC | 4 | Sneha | 2024-01-02 | NULL |
# MAGIC | 5 | Sneha | 2024-01-03 | 1 |
# MAGIC | 6 | Amit | 2024-01-05 | NULL |

# COMMAND ----------

# DBTITLE 1,Q6: Sample Data
# Sample data for Q6
orders = spark.createDataFrame(
    [(1, "Ravi", "2024-01-01"),
     (2, "Ravi", "2024-01-04"),
     (3, "Ravi", "2024-01-10"),
     (4, "Sneha", "2024-01-02"),
     (5, "Sneha", "2024-01-03"),
     (6, "Amit", "2024-01-05")],
    ["order_id", "customer", "order_date"]
)
orders.createOrReplaceTempView("orders")
display(orders)

# COMMAND ----------

# DBTITLE 1,Q7: Consecutive Login Streaks
# MAGIC %md
# MAGIC ## Q7: Consecutive Login Streaks
# MAGIC
# MAGIC Given the `logins` table (one row per user per login day), find every **streak of 3 or more consecutive login days**. For each qualifying streak return the user, its first date (`streak_start`), last date (`streak_end`), and length in days (`streak_days`).
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | user_name | streak_start | streak_end | streak_days |
# MAGIC |---|---|---|---|
# MAGIC | Neha | 2024-01-01 | 2024-01-03 | 3 |
# MAGIC | Om | 2024-01-03 | 2024-01-05 | 3 |

# COMMAND ----------

# DBTITLE 1,Q7: Sample Data
# Sample data for Q7
logins = spark.createDataFrame(
    [("Neha", "2024-01-01"),
     ("Neha", "2024-01-02"),
     ("Neha", "2024-01-03"),
     ("Neha", "2024-01-05"),
     ("Om", "2024-01-01"),
     ("Om", "2024-01-03"),
     ("Om", "2024-01-04"),
     ("Om", "2024-01-05"),
     ("Priya", "2024-01-02"),
     ("Priya", "2024-01-03")],
    ["user_name", "login_date"]
)
logins.createOrReplaceTempView("logins")
display(logins)

# COMMAND ----------

# DBTITLE 1,Q8: Customers by Spend Quartile
# MAGIC %md
# MAGIC ## Q8: Customers by Spend Quartile
# MAGIC
# MAGIC Given the `customer_spend` table, split customers into **4 equally-sized groups** (quartiles) by `total_spend`, where quartile 1 holds the lowest spenders. When rows don't divide evenly, earlier groups get the extra rows. Show each customer with their `quartile`, ordered by total_spend ascending.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | customer_name | total_spend | quartile |
# MAGIC |---|---|---|
# MAGIC | A | 100 | 1 |
# MAGIC | C | 200 | 1 |
# MAGIC | E | 300 | 2 |
# MAGIC | H | 400 | 2 |
# MAGIC | B | 450 | 3 |
# MAGIC | F | 550 | 3 |
# MAGIC | G | 650 | 4 |
# MAGIC | D | 800 | 4 |

# COMMAND ----------

# DBTITLE 1,Q8: Sample Data
# Sample data for Q8
customer_spend = spark.createDataFrame(
    [(1, "A", 100),
     (2, "B", 450),
     (3, "C", 200),
     (4, "D", 800),
     (5, "E", 300),
     (6, "F", 550),
     (7, "G", 650),
     (8, "H", 400)],
    ["customer_id", "customer_name", "total_spend"]
)
customer_spend.createOrReplaceTempView("customer_spend")
display(customer_spend)

# COMMAND ----------

# DBTITLE 1,Q9: Gap to Department's Top Salary
# MAGIC %md
# MAGIC ## Q9: Gap to Department's Top Salary
# MAGIC
# MAGIC Given the `salaries` table, for each employee show the **highest salary in their department** (`dept_max`) and **how much less** they earn than that top (`gap_to_max`; 0 for the top earner). Order by department, then salary descending.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | emp_name | department | salary | dept_max | gap_to_max |
# MAGIC |---|---|---|---|---|
# MAGIC | Gita | Finance | 9000 | 9000 | 0 |
# MAGIC | Ira | Finance | 7500 | 9000 | 1500 |
# MAGIC | Harish | Finance | 6000 | 9000 | 3000 |
# MAGIC | Kavya | Tech | 12000 | 12000 | 0 |
# MAGIC | Jai | Tech | 5000 | 12000 | 7000 |

# COMMAND ----------

# DBTITLE 1,Q9: Sample Data
# Sample data for Q9
salaries = spark.createDataFrame(
    [(1, "Gita", "Finance", 9000),
     (2, "Harish", "Finance", 6000),
     (3, "Ira", "Finance", 7500),
     (4, "Jai", "Tech", 5000),
     (5, "Kavya", "Tech", 12000)],
    ["emp_id", "emp_name", "department", "salary"]
)
salaries.createOrReplaceTempView("salaries")
display(salaries)

# COMMAND ----------

# DBTITLE 1,Q10: Salary Rank and Department Headcount
# MAGIC %md
# MAGIC ## Q10: Salary Rank and Department Headcount
# MAGIC
# MAGIC Given the `employee_salaries` table, for each employee show their **salary rank within their department** (`salary_rank`; highest salary = 1; employees with equal salaries share a rank and the next rank is skipped, e.g. 1, 1, 3) and the **number of employees in their department** (`dept_headcount`). Order by department, then salary descending.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | emp_name | department | salary | salary_rank | dept_headcount |
# MAGIC |---|---|---|---|---|
# MAGIC | Sameer | HR | 9000 | 1 | 4 |
# MAGIC | Omar | HR | 5000 | 2 | 4 |
# MAGIC | Reena | HR | 5000 | 2 | 4 |
# MAGIC | Qadir | HR | 4000 | 4 | 4 |
# MAGIC | Lena | IT | 8000 | 1 | 4 |
# MAGIC | Mohan | IT | 8000 | 1 | 4 |
# MAGIC | Pooja | IT | 7000 | 3 | 4 |
# MAGIC | Naina | IT | 6000 | 4 | 4 |

# COMMAND ----------

# DBTITLE 1,Q10: Sample Data
# Sample data for Q10
employee_salaries = spark.createDataFrame(
    [(1, "Lena", "IT", 8000),
     (2, "Mohan", "IT", 8000),
     (3, "Naina", "IT", 6000),
     (4, "Omar", "HR", 5000),
     (5, "Pooja", "IT", 7000),
     (6, "Qadir", "HR", 4000),
     (7, "Reena", "HR", 5000),
     (8, "Sameer", "HR", 9000)],
    ["emp_id", "emp_name", "department", "salary"]
)
employee_salaries.createOrReplaceTempView("employee_salaries")
display(employee_salaries)