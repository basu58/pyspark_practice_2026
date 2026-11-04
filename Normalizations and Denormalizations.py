# Databricks notebook source
# DBTITLE 1,Normalization, Denormalization & Relationships
# MAGIC %md
# MAGIC # Normalization, Denormalization & Relationships
# MAGIC
# MAGIC How to turn one messy, redundant table into clean, related tables — and when to join them back for fast reads. Each concept has a runnable PySpark demo, and the notebook ends with one LeetCode-style medium practice question.

# COMMAND ----------

# DBTITLE 1,Normalization: What and Why
# MAGIC %md
# MAGIC ## 1. Normalization — what and why
# MAGIC
# MAGIC **Normalization** organizes data into tables so that **every fact is stored exactly once**.
# MAGIC
# MAGIC Why does redundancy hurt? The `orders_flat` table below repeats the customer's city and the product's details on every order row. That causes three classic **anomalies**:
# MAGIC
# MAGIC * **Update anomaly** — Aarav moves from Pune to Nagpur: you must update *every* one of his order rows. Miss one and the data contradicts itself.
# MAGIC * **Insert anomaly** — you cannot add a new product (or a new customer) until someone places an order for it.
# MAGIC * **Delete anomaly** — delete Chirag's only order and Chirag himself vanishes from the database.
# MAGIC
# MAGIC The normal forms, in rule-of-thumb form:
# MAGIC
# MAGIC | Form | Rule | Example of a violation |
# MAGIC |---|---|---|
# MAGIC | **1NF** | Every cell holds one atomic value — no lists | `products = "Laptop, Mouse"` in a single cell |
# MAGIC | **2NF** | No non-key column depends on *part* of a composite key | Only applies when the key is made of 2+ columns |
# MAGIC | **3NF** | No non-key column depends on another non-key column (a *transitive* dependency) | `order_id → customer_name → city`: the city depends on the customer, not the order |
# MAGIC
# MAGIC A 3NF table satisfies the famous mnemonic — every non-key column must depend on **"the key, the whole key, and nothing but the key."**
# MAGIC
# MAGIC The demo table below satisfies 1NF (all values atomic) but violates **3NF**.

# COMMAND ----------

# DBTITLE 1,Demo: The Denormalized orders_flat Table
# A denormalized, redundant table: every order repeats customer and product details.
# city depends on the customer, price/category depend on the product — 3NF violations.
orders_flat = spark.createDataFrame(
    [(1, "Aarav", "Pune", "Laptop", "Electronics", 55000, 1),
     (2, "Aarav", "Pune", "Mouse", "Electronics", 800, 2),
     (3, "Bela", "Mumbai", "Desk", "Furniture", 9000, 1),
     (4, "Aarav", "Pune", "Desk", "Furniture", 9000, 1),
     (5, "Chirag", "Delhi", "Laptop", "Electronics", 55000, 1)],
    ["order_id", "customer_name", "customer_city", "product_name",
     "product_category", "product_price", "quantity"]
)
orders_flat.createOrReplaceTempView("orders_flat")
display(orders_flat)

# COMMAND ----------

# DBTITLE 1,Normalizing orders_flat Step by Step
# MAGIC %md
# MAGIC ## 2. Normalizing `orders_flat` — step by step
# MAGIC
# MAGIC **Step 1 — find the real entities** hiding inside the flat table: *customers* (name, city), *products* (name, category, price), and *orders* (which customer bought which product, in what quantity).
# MAGIC
# MAGIC **Step 2 — find the dependencies.** In `orders_flat` everything hangs off `order_id`:
# MAGIC
# MAGIC * `order_id → quantity` — a genuine fact about the order.
# MAGIC * `order_id → customer_name → city` — **transitive**: the city really belongs to the *customer*.
# MAGIC * `order_id → product_name → category, price` — **transitive**: these belong to the *product*.
# MAGIC
# MAGIC (2NF doesn't bite here because the key is a single column — partial dependencies only exist with composite keys.)
# MAGIC
# MAGIC **Step 3 — give each entity its own table and key**, and replace repeated values with **foreign keys** (IDs pointing at the parent table):
# MAGIC
# MAGIC * `customers(customer_id, customer_name, city)`
# MAGIC * `products(product_id, product_name, category, price)`
# MAGIC * `orders(order_id, customer_id, product_id, quantity)`
# MAGIC
# MAGIC Each fact now lives exactly once: a customer's city is updated in one row, a product can exist with zero orders, and deleting an order never deletes the customer. The three tables are created below.

# COMMAND ----------

# DBTITLE 1,Demo: customers Table (3NF)
# Normalized: each customer stored exactly once
# (fixing Aarav's city later = updating ONE row)
customers = spark.createDataFrame(
    [(1, "Aarav", "Pune"),
     (2, "Bela", "Mumbai"),
     (3, "Chirag", "Delhi")],
    ["customer_id", "customer_name", "city"]
)
customers.createOrReplaceTempView("customers")
display(customers)

# COMMAND ----------

# DBTITLE 1,Demo: products Table (3NF)
# Normalized: each product stored exactly once
# (a product can now exist with zero orders — insert anomaly gone)
products = spark.createDataFrame(
    [(1, "Laptop", "Electronics", 55000),
     (2, "Mouse", "Electronics", 800),
     (3, "Desk", "Furniture", 9000)],
    ["product_id", "product_name", "category", "price"]
)
products.createOrReplaceTempView("products")
display(products)

# COMMAND ----------

# DBTITLE 1,Demo: orders Table (3NF, with FKs)
# Normalized: orders keep only order facts + foreign keys (customer_id, product_id)
orders = spark.createDataFrame(
    [(1, 1, 1, 1),   # Aarav  bought Laptop, qty 1
     (2, 1, 2, 2),   # Aarav  bought Mouse,  qty 2
     (3, 2, 3, 1),   # Bela   bought Desk,   qty 1
     (4, 1, 3, 1),   # Aarav  bought Desk,   qty 1
     (5, 3, 1, 1)],  # Chirag bought Laptop, qty 1
    ["order_id", "customer_id", "product_id", "quantity"]
)
orders.createOrReplaceTempView("orders")
display(orders)

# COMMAND ----------

# DBTITLE 1,Relationships: 1:1, 1:N, M:N
# MAGIC %md
# MAGIC ## 3. Relationships — how the tables connect
# MAGIC
# MAGIC | Type | Meaning | Example | How it's implemented |
# MAGIC |---|---|---|---|
# MAGIC | **One-to-many (1:N)** | One row in A matches many rows in B | one `customer` → many `orders`; one `product` → many `orders` | a foreign key (`customer_id`) on the "many" side |
# MAGIC | **Many-to-many (M:N)** | Rows of A match many of B and vice versa | `students` ↔ `courses` | a **bridge / junction table** (`enrollments`) holding both FKs |
# MAGIC | **One-to-one (1:1)** | One row of A matches exactly one of B | `employee` ↔ `passport_details` | FK with a unique constraint; usually done to isolate sensitive or optional data |
# MAGIC
# MAGIC 1:N is the workhorse of relational design. M:N **cannot** be modeled with foreign keys alone — it always needs the bridge table. Notice that in our schema `orders` sits between `customers` and `products`: it is effectively the bridge that resolves the customers ↔ products many-to-many ("which customers bought which products, how many times").

# COMMAND ----------

# DBTITLE 1,Demo: M:N via Bridge Table
# Many-to-many: students <-> courses, resolved by the enrollments bridge table.
# enrollments holds one row per (student, course) pair — the two FKs together are unique.
students = spark.createDataFrame(
    [(1, "Asha"), (2, "Bunty"), (3, "Chhaya")],
    ["student_id", "student_name"]
)
courses = spark.createDataFrame(
    [(1, "Math"), (2, "Science"), (3, "History")],
    ["course_id", "course_name"]
)
enrollments = spark.createDataFrame(
    [(1, 1, 1),   # Asha   -> Math
     (2, 1, 2),   # Asha   -> Science
     (3, 2, 1),   # Bunty  -> Math
     (4, 3, 3)],  # Chhaya -> History
    ["enrollment_id", "student_id", "course_id"]
)
students.createOrReplaceTempView("students")
courses.createOrReplaceTempView("courses")
enrollments.createOrReplaceTempView("enrollments")

# the join across the bridge materializes the M:N relationship
display(
    enrollments
    .join(students, "student_id")
    .join(courses, "course_id")
    .select("student_name", "course_name")
    .orderBy("student_name", "course_name")
)

# COMMAND ----------

# DBTITLE 1,Denormalization: What and Why
# MAGIC %md
# MAGIC ## 4. Denormalization — joining it back on purpose
# MAGIC
# MAGIC **Denormalization** deliberately merges or pre-joins normalized tables into wider, redundant tables. You give up "each fact once" to make **reads** cheap.
# MAGIC
# MAGIC * **Why:** joins cost time and shuffle. Reporting queries, BI dashboards, and analytics scans want all the columns in one wide row. Data-warehouse **star schemas** (one big fact table + small dimension tables) are deliberately denormalized designs.
# MAGIC * **Trade-offs:** redundant storage, and the update anomaly returns — denormalized copies must be refreshed when the source changes.
# MAGIC * **Rule of thumb:** *normalize for writes* (OLTP — transactions, one row per fact), *denormalize for reads* (OLAP — analytics, reporting, ML features).
# MAGIC
# MAGIC The cell below denormalizes our 3NF tables back into the original wide table with a single join — identical output to `orders_flat`, but behind it the data still has a single source of truth.

# COMMAND ----------

# DBTITLE 1,Demo: Denormalize Back to the Wide Table
# Denormalization for reads: one join rebuilds the wide table from the 3NF tables.
# Same rows as orders_flat — but the source data is stored exactly once.
display(
    orders
    .join(customers, "customer_id")
    .join(products, "product_id")
    .select("order_id", "customer_name", "city", "product_name",
            "category", "price", "quantity")
    .orderBy("order_id")
)

# COMMAND ----------

# DBTITLE 1,Practice Question: Inconsistent Dimension Data
# MAGIC %md
# MAGIC ## 5. Practice Question (Medium): Inconsistent Dimension Data
# MAGIC
# MAGIC A company kept its orders in one denormalized table. Because each customer's city is stored on every order row (the update anomaly that normalization prevents), the same customer sometimes appears with **different cities** across their orders.
# MAGIC
# MAGIC Given the `orders_flat2` table, find every customer that appears with **more than one distinct city**. For each, return:
# MAGIC
# MAGIC * `customer_name`
# MAGIC * `distinct_cities` — number of distinct cities recorded for that customer
# MAGIC * `total_orders` — their total number of orders
# MAGIC * `conflicting_cities` — all their cities in one comma-separated string, sorted alphabetically
# MAGIC
# MAGIC Order by `distinct_cities` descending, then `customer_name` ascending.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | customer_name | distinct_cities | total_orders | conflicting_cities |
# MAGIC |---|---|---|---|
# MAGIC | Aarav | 2 | 2 | Mumbai,Pune |
# MAGIC | Chirag | 2 | 3 | Delhi,Noida |

# COMMAND ----------

# DBTITLE 1,Practice Question: Sample Data
# Sample data for the practice question
orders_flat2 = spark.createDataFrame(
    [(1, "Aarav", "Pune", "Laptop", 1),
     (2, "Aarav", "Mumbai", "Mouse", 2),
     (3, "Bela", "Mumbai", "Desk", 1),
     (4, "Bela", "Mumbai", "Chair", 1),
     (5, "Chirag", "Delhi", "Lamp", 3),
     (6, "Chirag", "Delhi", "Desk", 1),
     (7, "Chirag", "Noida", "Lamp", 1)],
    ["order_id", "customer_name", "customer_city", "product_name", "quantity"]
)
orders_flat2.createOrReplaceTempView("orders_flat2")
display(orders_flat2)