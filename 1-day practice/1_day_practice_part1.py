# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
employees_data = [
    (101, "Rahul", 10, 80000),
    (102, "Priya", 20, 90000),
    (103, "Amit", 10, 75000),
    (104, "Neha", 30, 85000),
    (105, "Ravi", None, 70000)
]

# COMMAND ----------

df = spark.createDataFrame(data=employees_data, schema=["id", "name", "dept_id", "salary"])
display(df)
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = StructType(
    [StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("dept_id", IntegerType()),
    StructField("salary", IntegerType())]
)

# COMMAND ----------

df = spark.createDataFrame(data=employees_data, schema=schema)
display(df)
df.printSchema()

# COMMAND ----------

df.select("name", "Salary", "Dept_Id").show()

# COMMAND ----------

from pyspark.sql.functions import col, expr

# COMMAND ----------

df.select(col("id"), col("name")).show()

# COMMAND ----------

df.select("id", "salary", expr("salary*10 as bonus")).show()

# COMMAND ----------

df.selectExpr("id", "salary", "salary*10 as bonus").show()

# COMMAND ----------

df.select(col("id"), "salary", (col("salary")*10).alias("bonus")).show()

# COMMAND ----------

df.filter(col("salary")>80000).show()

# COMMAND ----------

df.where(col("salary")>80000).show()

# COMMAND ----------

df.filter((col("salary")>80000) & (col("dept_id") == 20)).show()

# COMMAND ----------

df.filter((col("salary")>80000) | (col("dept_id") == 20)).show()

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

df1 = df.withColumn("increased_salary", when(col("salary")<80000, col("salary")*1.5)
                    .when(col("salary")>80000, col("salary")*1.2)
                    .otherwise(col("salary")))
display(df1)

# COMMAND ----------

# DBTITLE 1,withColumns example
df2 = df1.withColumns(
   { "increased_salary":col("salary") + ( col("salary") * .10),
    "salary_band": (when(col("salary")>80000, "High")
                    .when(col("salary")<80000, "low"))
                    .otherwise("Average")
    }
)
df2.show()

# COMMAND ----------

df2.withColumnRenamed("increased_salary", "incr_sal").show()
display(df2.withColumnsRenamed({"increased_salary": "incr_sal", "salary_band": "sal_band"}))

# COMMAND ----------

df2.drop("salary_band").show()

# COMMAND ----------

df2.select("dept_id").distinct().show()

# COMMAND ----------

df2.dropDuplicates(["dept_id"]).show()
df2.drop_duplicates(["dept_id"]).show()

# COMMAND ----------

df2.filter(col("dept_id").isNull()).show()
df2.filter(col("dept_id").isNotNull()).show()
df2.fillna({"dept_id": 0000}).show()
df2.na.fill({"dept_id": 0000}).show()
df2.dropna(subset=["dept_id"]).show()
df2.dropna(how="all").show()
df2.dropna(how="any").show()
df2.dropna(how="any", subset=["dept_id"]).show()
df2.dropna(how="all", subset=["dept_id"]).show()
df2.dropna(how="all", subset=["dept_id", "salary"]).show()
df2.dropna(how="any", subset=["dept_id", "salary"]).show()
df2.dropna(how="any", thresh=2).show()
df2.dropna(how="any", thresh=3).show()
df2.dropna(how="any", thresh=4).show()

# COMMAND ----------

from pyspark.sql.functions import (
    sum, avg, min, max, count
)

# COMMAND ----------

df.agg(sum(col("salary")).alias("sum_salary")).show()
df.select(sum("salary").alias("sum_salary")).show()

# COMMAND ----------

df.agg(avg('salary').alias("avg_sal")).show()
df.select(avg("salary").alias("avg_sal")).show()
df.agg(min('salary').alias("min_sal")).show()
df.select(min("salary").alias("min_sal")).show()
df.agg(max('salary').alias("max_sal")).show()
df.select(max("salary").alias("max_sal")).show()
df.agg(count('salary').alias("count_sal")).show()
df.select(count("salary").alias("count_sal")).show()

# COMMAND ----------

df.groupBy('dept_id').\
    agg(\
        sum(col('salary')).alias('sum_salary'),\
        avg(col('salary')).alias('avg_salary')\
    ).orderBy(col("sum_salary")).show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.agg(sum("salary").alias("total"), avg("salary").alias("mean")).show()
df.groupBy("dept_id").agg(countDistinct("id").alias("n_employees")).show()
df.agg(percentile_approx("salary", 0.5).alias("median_sal")).show()

# COMMAND ----------

df.withColumn(
    "name",
    upper(col("name"))
).show()


# COMMAND ----------

df.withColumn("name", lower(col("name"))).show()

# COMMAND ----------

df.withColumn("name", initcap(col("name"))).show()

# COMMAND ----------

df.withColumn("name", trim(col("name"))).show()

# COMMAND ----------

df.withColumn("name", concat(col("name"), lit("_suffix"))).show()

# COMMAND ----------

df.withColumn("name_len", length(col("name"))).show()

# COMMAND ----------

df.withColumn("name", substring(col("name"), 1, 3)).show()

# COMMAND ----------

df.withColumn("name", regexp_replace(col("name"), "[^a-zA-Z]", "")).show()

# COMMAND ----------

df.withColumn("name", split(col("name"), " ")).show()

# COMMAND ----------

df.withColumn("name", lpad(col("name"), 10, "*")).show()

# COMMAND ----------

df.withColumn("name", rpad(col("name"), 10, "*")).show()

# COMMAND ----------

df.withColumn("name", regexp_extract(col("name"), r"(\w+)", 1)).show()

# COMMAND ----------

s = 'Hello'
print(s.find("l"))

# COMMAND ----------

df.withColumn("name", instr(col("name"), "a")).show()

# COMMAND ----------

df.withColumn("name", translate(col("name"), "ae", "13")).show()

# COMMAND ----------

df.withColumn("name", reverse(col("name"))).show()

# COMMAND ----------

df.withColumn("name", format_string("Name: %s", col("name"))).show()

# COMMAND ----------

# DBTITLE 1,Read sales.csv from workspace file
sales = spark.read.csv("/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/1-day practice/Data/sales.csv", header=True, inferSchema=True)
sales.show()

# COMMAND ----------

# DBTITLE 1,Date function essentials on sales
# 1) Extract date parts
sales.select(
    "order_id", "order_date",
    year("order_date").alias("yr"),
    month("order_date").alias("mon"),
    dayofmonth("order_date").alias("day"),
    dayofweek("order_date").alias("dow"),   # 1 = Sunday
    weekofyear("order_date").alias("week"),
    quarter("order_date").alias("qtr")
).show()

# COMMAND ----------

# 2) Format a date as a string
sales.select(
    date_format("order_date", "yyyy-MM-dd").alias("iso"),
    date_format("order_date", "dd-MMMM-yyyy").alias("pretty")
).show()


# COMMAND ----------


# 3) Date arithmetic
sales.select(
    date_add("order_date", 7).alias("plus_7d"),
    date_sub("order_date", 7).alias("minus_7d"),
    add_months("order_date", 1).alias("plus_1m"),
    next_day("order_date", "Monday").alias("next_monday")
).show()


# COMMAND ----------


# 4) Differences and month boundaries
sales.select(
    datediff(current_date(), "order_date").alias("days_since_order"),
    months_between(current_date(), "order_date").alias("months_since_order"),
    trunc("order_date", "mm").alias("month_start"),
    last_day("order_date").alias("month_end")
).show()


# COMMAND ----------


# 5) Parsing strings to dates and Unix epoch seconds
sales.select(
    to_date(lit("2026-10-05"), "yyyy-MM-dd").alias("parsed_date"),
    unix_timestamp("order_date").alias("epoch_secs")
).show()

# COMMAND ----------

# DBTITLE 1,Window functions intro
# MAGIC %md
# MAGIC # Window Functions
# MAGIC
# MAGIC A **window function** computes a result over a set of rows *related to the current row* — unlike `groupBy().agg()`, it does **not** collapse rows: every input row survives and just gains new columns.
# MAGIC
# MAGIC **Anatomy of a window spec:**
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC w = Window.partitionBy("dept_name").orderBy("salary")
# MAGIC ```
# MAGIC
# MAGIC * `partitionBy` — split rows into independent groups (like `GROUP BY`)
# MAGIC * `orderBy` — order rows inside each partition (required for ranking and lead/lag)
# MAGIC * `rowsBetween` / `rangeBetween` — optional *frame*: which rows around the current row an aggregate covers (running totals, moving averages)
# MAGIC
# MAGIC Each function family below has its own cell, using the employees, departments, and sales data.

# COMMAND ----------

# DBTITLE 1,Load employees + departments, join
# Load employees & departments CSVs, then join (sales is already loaded)
emp = spark.read.csv(
    "/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/1-day practice/Data/employees.csv",
    header=True, inferSchema=True
).withColumn("dept_id", col("dept_id").cast("int"))   # CSV has 10.0 -> double, cast to int
dept = spark.read.csv(
    "/Workspace/Repos/basudevchhotray@gmail.com/pyspark_practice_2026/1-day practice/Data/departments.csv",
    header=True, inferSchema=True
)

from pyspark.sql.window import Window

emp_dept = emp.join(dept, "dept_id", "left")   # left join keeps Ravi (null dept)
emp_dept.show()

# COMMAND ----------

# DBTITLE 1,About row_number
# MAGIC %md
# MAGIC ### 1) `row_number()` — position within a group
# MAGIC
# MAGIC **What it does:** once the window orders the rows, `row_number()` stamps `1, 2, 3, ...` on them. Every number is **unique** — if two rows tie on the `orderBy` columns, the tie is broken arbitrarily.
# MAGIC
# MAGIC **In our example:** each department is its own "competition" (`partitionBy("dept_name")`), ordered by salary descending:
# MAGIC
# MAGIC | dept_name | name | salary | salary_rank |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Engineering | Rahul | 80000 | 1 |
# MAGIC | Engineering | Amit | 75000 | 2 |
# MAGIC | Finance | Priya | 90000 | 1 |
# MAGIC | HR | Neha | 85000 | 1 |
# MAGIC
# MAGIC Ravi has a NULL department, so he forms his own one-person partition — rank 1.
# MAGIC
# MAGIC **The top-N pattern:** `filter(col("rn") == 1)` picks the top earner per department. Note we must compute `rn` in one `withColumn` step and filter in the next — Spark does not let you reference a window column inside the same call that creates it.
# MAGIC
# MAGIC **When to reach for it:** top-N per group, deduplication (keep only the newest row per key), pagination.

# COMMAND ----------

# DBTITLE 1,row_number — rank within group
# 1) row_number — 1,2,3... within each partition; ALWAYS unique, ties broken arbitrarily
w = Window.partitionBy("dept_name").orderBy(col("salary").desc())

emp_dept.withColumn("salary_rank", row_number().over(w)).show()

# Classic use: top earner per department (rank = 1)
emp_dept.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).show()

# COMMAND ----------

# DBTITLE 1,About rank vs dense_rank
# MAGIC %md
# MAGIC ### 2) `rank()` vs `dense_rank()` vs `row_number()` — ties make the difference
# MAGIC
# MAGIC All three number rows, but they disagree when rows **tie** on the order column:
# MAGIC
# MAGIC | Function | Ties get | Gap after ties? |
# MAGIC | --- | --- | --- |
# MAGIC | `row_number()` | arbitrary unique numbers | n/a |
# MAGIC | `rank()` | the same number | yes — the next rank is skipped |
# MAGIC | `dense_rank()` | the same number | no |
# MAGIC
# MAGIC Our example orders by `dept_id`, so Rahul & Amit (both dept 10) tie. Watch Priya, the row *after* the tie:
# MAGIC
# MAGIC * `row_number`: 1, 2, 3, 4, 5 — arbitrary within the tie
# MAGIC * `rank`: 1, 2, 2, **4** — the number 3 is skipped
# MAGIC * `dense_rank`: 1, 2, 2, **3** — no gap
# MAGIC
# MAGIC Also notice: Ravi's NULL `dept_id` sorted **first** — Spark puts NULLs first in ascending order by default.
# MAGIC
# MAGIC **Rule of thumb:** unique row ID → `row_number`; "2nd highest with ties counted once" → `dense_rank`; competition-style ranking (gaps) → `rank`.

# COMMAND ----------

# DBTITLE 1,rank vs dense_rank
# 2) rank vs dense_rank vs row_number — the difference shows with TIES
# Order by dept_id: Rahul & Amit tie (both 10); nulls sort FIRST by default in ascending order
w = Window.orderBy("dept_id")

emp_dept.withColumns({
    "rn": row_number().over(w),      # unique, ties get arbitrary order
    "rk": rank().over(w),           # ties share a rank, next rank SKIPS (gap)
    "drk": dense_rank().over(w)    # ties share a rank, NO gap
}).show()

# COMMAND ----------

# DBTITLE 1,About lag / lead
# MAGIC %md
# MAGIC ### 3) `lag()` / `lead()` — reach into a neighboring row
# MAGIC
# MAGIC **What they do:** `lag(col)` grabs `col` from the **previous** row in the window order; `lead(col)` grabs it from the **next** row. If there is no such row — the first row has no previous — you get NULL. Both accept an offset: `lag("amount", 2)` looks two rows back.
# MAGIC
# MAGIC **In our example** (orders sorted by date; `order_id` breaks date ties):
# MAGIC
# MAGIC * Order 2's `prev_amount` is order 1's amount (500); order 1 itself has NULL — nothing precedes it.
# MAGIC * `days_since_prev_order` nests two functions: `datediff` compares each order's date with the *previous* order's date obtained via `lag("order_date")`.
# MAGIC
# MAGIC **When to reach for it:** day-over-day change, time between events, gap detection, "compare with the previous row" — all impossible with `groupBy`, because they need the *position* of neighbors, not just group totals.

# COMMAND ----------

# DBTITLE 1,lag / lead on sales
# 3) lag / lead — pull the value from the previous / next row
w = Window.orderBy("order_date", "order_id")   # order_id breaks date ties

sales.withColumns({
    "prev_amount": lag("amount").over(w),
    "next_amount": lead("amount").over(w),
    "days_since_prev_order": datediff("order_date", lag("order_date").over(w))
}).show()

# COMMAND ----------

# DBTITLE 1,About running totals
# MAGIC %md
# MAGIC ### 4) Running total — meet the window FRAME
# MAGIC
# MAGIC So far the window was "all rows in my partition". A **frame** narrows that to a range of rows around the current one:
# MAGIC
# MAGIC ```python
# MAGIC .rowsBetween(Window.unboundedPreceding, Window.currentRow)
# MAGIC ```
# MAGIC
# MAGIC means *start at the very first row of the partition and include everything up to and including the current row*. Aggregating with `sum` over that frame gives a cumulative total:
# MAGIC
# MAGIC | order | amount | running_total |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | 500 | 500 |
# MAGIC | 2 | 700 | 1200 |
# MAGIC | 3 | 300 | 1500 |
# MAGIC | 6 | 1000 | 3600 |
# MAGIC
# MAGIC Each row's total = all amounts seen so far. Classic uses: cumulative sales per day, running balances, year-to-date metrics.
# MAGIC
# MAGIC Keep this frame in mind — the next cell builds on it, and it also explains the surprise waiting in the `first`/`last` cell at the end.

# COMMAND ----------

# DBTITLE 1,Running total
# 4) Running (cumulative) total — frame: first row ... current row
w = (Window.orderBy("order_date", "order_id")
     .rowsBetween(Window.unboundedPreceding, Window.currentRow))

sales.withColumn("running_total", sum("amount").over(w)).show()

# COMMAND ----------

# DBTITLE 1,About moving averages
# MAGIC %md
# MAGIC ### 5) 3-row moving average — `rowsBetween(-2, currentRow)`
# MAGIC
# MAGIC Same frame idea, but now relative to the current row: `-2` means "two rows back". So each average covers [2 rows back, 1 row back, current row] — a 3-row trailing window:
# MAGIC
# MAGIC * Row 1: just itself → 500.0
# MAGIC * Row 2: rows 1–2 → (500+700)/2 = 600.0
# MAGIC * Row 3: rows 1–3 → (500+700+300)/3 = 500.0
# MAGIC * From row 4 on: always the last 3 rows — watch the big 900 enter and the small 500 drop out of the average.
# MAGIC
# MAGIC **`rowsBetween` vs `rangeBetween`:** `rowsBetween` counts *rows*; `rangeBetween` counts *order-key values* — e.g. "all orders within the trailing 3 **days**" instead of "the last 3 orders". Time-based moving windows need `rangeBetween` with a numeric key (like epoch seconds).

# COMMAND ----------

# DBTITLE 1,3-row moving average
# 5) 3-row moving average — 2 rows back + the current row
w = (Window.orderBy("order_date", "order_id")
     .rowsBetween(-2, Window.currentRow))

sales.withColumn("moving_avg_3", avg("amount").over(w)).show()

# rowsBetween counts ROWS; rangeBetween counts ORDER-KEY values
# (e.g. a trailing 3-DAY window needs rangeBetween on seconds)

# COMMAND ----------

# DBTITLE 1,About percent_rank, cume_dist, ntile
# MAGIC %md
# MAGIC ### 6) `percent_rank()` / `cume_dist()` / `ntile()` — where does this row sit?
# MAGIC
# MAGIC With 5 salaries ordered ascending, these describe each row's *relative position*:
# MAGIC
# MAGIC * **`percent_rank()`** = `(rank - 1) / (n - 1)` → 0.0 for the lowest salary (Ravi, 70000), 1.0 for the highest (Priya, 90000). Answers *"what percentile am I in?"*.
# MAGIC * **`cume_dist()`** = rows at or below current ÷ total rows → 0.2, 0.4, 0.6, 0.8, 1.0. Answers *"what fraction of rows are at or below me?"*.
# MAGIC * **`ntile(4)`** cuts the ordered rows into 4 near-equal **buckets by position**: the lowest-paid ~25% land in bucket 1, the next in bucket 2, and so on — like quartiles *by position*, not by value.
# MAGIC
# MAGIC **When to reach for them:** percentile-based grading ("top 10% get a bonus"), A/B/C tiering, salary benchmarking.

# COMMAND ----------

# DBTITLE 1,percent_rank, cume_dist, ntile
# 6) percent_rank / cume_dist / ntile
w = Window.orderBy("salary")

emp_dept.withColumns({
    "pct_rank": percent_rank().over(w),   # (rank - 1) / (n - 1) -> 0.0..1.0
    "cume_dist": cume_dist().over(w),     # rows <= current / total rows
    "salary_quartile": ntile(4).over(w)  # splits rows into 4 buckets
}).show()

# COMMAND ----------

# DBTITLE 1,About window aggregates vs groupBy
# MAGIC %md
# MAGIC ### 7) Window aggregate WITHOUT `orderBy` — `groupBy` without collapsing
# MAGIC
# MAGIC This is the core mental model of the whole topic:
# MAGIC
# MAGIC * **`groupBy().agg()`** — each group becomes **one row**; the individuals disappear.
# MAGIC * **`avg(...).over(partitionBy("dept_name"))`** — each row **keeps its identity** and *gains* the group metric as a new column.
# MAGIC
# MAGIC So Rahul and Amit each show `dept_avg_salary = 77500.0` (the Engineering average) on their **own** row — no join required. The `groupBy` version in the same cell collapses Engineering into a single row for comparison.
# MAGIC
# MAGIC **Why this pattern is everywhere:** "salary vs department average", "this store vs its region's average", "% of team total" — all need a row-level value AND a group-level value at the same time. `groupBy` alone can't do that; you'd need a join back.

# COMMAND ----------

# DBTITLE 1,Window aggregate vs groupBy
# 7) Window aggregate WITHOUT orderBy — dept metric on EVERY row, no join needed
w = Window.partitionBy("dept_name")

emp_dept.withColumns({
    "dept_avg_salary": avg("salary").over(w),
    "dept_headcount": count("*").over(w)
}).show()

# the groupBy equivalent collapses each dept to a single row:
emp_dept.groupBy("dept_name").agg(avg("salary").alias("dept_avg")).show()

# COMMAND ----------

# DBTITLE 1,About first / last
# MAGIC %md
# MAGIC ### 8) `first()` / `last()` — and why the FRAME really matters
# MAGIC
# MAGIC `first(col)` over an ordered window = the value from the frame's **first row**. Here's the catch: an ordered window's **default frame is `unboundedPreceding → currentRow`** — the exact frame you met in the running-total cell. So:
# MAGIC
# MAGIC * `first("name")` over salary-desc works fine — the frame *starts* at the top earner.
# MAGIC * `last("name")` over the same window would return... **the current row itself**, because the default frame *ends* at the current row!
# MAGIC
# MAGIC The fix is explicit: extend the frame with `.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)` so `last` sees the whole partition and returns the department's lowest earner.
# MAGIC
# MAGIC **Takeaway:** whenever `first`/`last` (or any aggregate) over an ordered window looks subtly wrong, ask *"what is my frame?"* — the default is not the whole partition.

# COMMAND ----------

# DBTITLE 1,first / last value
# 8) first / last — and why the FRAME matters
# Default frame for an ordered window ends at the CURRENT row,
# so "last" would only see rows up to now. Extend it for the true last.
w_desc = Window.partitionBy("dept_name").orderBy(col("salary").desc())
w_full = w_desc.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

emp_dept.withColumns({
    "dept_top_earner": first("name").over(w_desc),
    "dept_lowest_earner": last("name").over(w_full)
}).show()