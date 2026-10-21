# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 🧩 PySpark Interview Question — Medium
# MAGIC
# MAGIC ## **Problem: Consecutive Login Streak with At Least One Failure**
# MAGIC
# MAGIC You are given a dataset of user login attempts with the following schema:
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `login_id` | int | Unique login attempt ID |
# MAGIC | `user_id` | int | User who attempted login |
# MAGIC | `login_date` | date | Date of the attempt |
# MAGIC | `status` | string | `"success"` or `"failure"` |
# MAGIC
# MAGIC **Task:** For each `user_id`, find the **longest streak of consecutive successful login days** where the streak is immediately followed by at least one `"failure"` on the next calendar day. A streak is defined as consecutive days (no gaps) where `status = "success"`. Return `user_id`, `streak_start_date`, `streak_end_date`, and `streak_length`.
# MAGIC
# MAGIC ### Rules
# MAGIC * A "consecutive day" means `login_date` values are back-to-back calendar days with no gap.
# MAGIC * The streak must consist **only** of `"success"` days.
# MAGIC * The day **immediately after** the streak's last success must be a `"failure"` (if the streak runs to the last available date for that user, it does NOT qualify).
# MAGIC * If a user has multiple qualifying streaks, return only the **longest** one. If there's a tie, return the one with the earliest `streak_start_date`.
# MAGIC * If no streak qualifies, that user should not appear in the output.
# MAGIC
# MAGIC ### Example Input
# MAGIC
# MAGIC | login_id | user_id | login_date | status |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | 1 | 2026-01-01 | success |
# MAGIC | 2 | 1 | 2026-01-02 | success |
# MAGIC | 3 | 1 | 2026-01-03 | failure |
# MAGIC | 4 | 1 | 2026-01-05 | success |
# MAGIC | 5 | 1 | 2026-01-06 | success |
# MAGIC | 6 | 1 | 2026-01-07 | success |
# MAGIC | 7 | 1 | 2026-01-08 | failure |
# MAGIC | 8 | 2 | 2026-01-01 | success |
# MAGIC | 9 | 2 | 2026-01-03 | failure |
# MAGIC | 10 | 3 | 2026-01-02 | success |
# MAGIC | 11 | 3 | 2026-01-03 | success |
# MAGIC | 12 | 3 | 2026-01-04 | success |
# MAGIC
# MAGIC ### Expected Output
# MAGIC
# MAGIC | user_id | streak_start_date | streak_end_date | streak_length |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | 2026-01-05 | 2026-01-07 | 3 |
# MAGIC | 3 | 2026-01-02 | 2026-01-04 | 3 |
# MAGIC
# MAGIC ### Explanation
# MAGIC * **User 1**: Two streaks — Jan 1–2 (length 2, followed by failure on Jan 3) and Jan 5–7 (length 3, followed by failure on Jan 8). Longest = Jan 5–7, length 3.
# MAGIC * **User 2**: Only one success on Jan 1, and Jan 2 has no record (gap, not a failure). Streak doesn't qualify.
# MAGIC * **User 3**: Jan 2–4 all success, but Jan 5 has no record — streak runs to the last date, so it does NOT qualify. Wait — actually in this dataset Jan 4 is the last day for user 3 with no failure after. Let me fix this...
# MAGIC
# MAGIC > **Correction to expected output:** User 3's streak (Jan 2–4) has no failure on Jan 5 (no record at all), so it does **not** qualify. Updated expected output:
# MAGIC
# MAGIC | user_id | streak_start_date | streak_end_date | streak_length |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | 2026-01-05 | 2026-01-07 | 3 |
# MAGIC
# MAGIC ### Key Concepts Tested
# MAGIC * **Window functions** — `lead()` / `lag()` for peeking at adjacent rows
# MAGIC * **Date arithmetic** — `datediff()` for detecting consecutive calendar days
# MAGIC * **Conditional aggregation** — grouping consecutive rows into streaks
# MAGIC * **Filtering** — requiring a failure immediately after the streak
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Write your solution below 👇

# COMMAND ----------

# ===== SETUP: Sample Data (run this first) =====
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, when, datediff, row_number, max as _max, min as _min, count as _count, to_date, count
from pyspark.sql.window import Window

login_data = [
    (1, 1, "2026-01-01", "success"),
    (2, 1, "2026-01-02", "success"),
    (3, 1, "2026-01-03", "failure"),
    (4, 1, "2026-01-05", "success"),
    (5, 1, "2026-01-06", "success"),
    (6, 1, "2026-01-07", "success"),
    (7, 1, "2026-01-08", "failure"),
    (8, 2, "2026-01-01", "success"),
    (9, 2, "2026-01-03", "failure"),
    (10, 3, "2026-01-02", "success"),
    (11, 3, "2026-01-03", "success"),
    (12, 3, "2026-01-04", "success"),
]

columns = ["login_id", "user_id", "login_date", "status"]
logins = spark.createDataFrame(login_data, columns)
logins.show()

# ===== YOUR SOLUTION BELOW =====
# TODO: Find the longest streak of consecutive successful login days per user,
#       where the streak is immediately followed by a "failure" on the next calendar day.
#
# Hints:
#   1. Use a window ordered by login_date per user_id
#   2. Use lag()/lead() to peek at the previous and next row's status and date
#   3. Use datediff() to check if consecutive rows are back-to-back calendar days
#   4. A new streak starts when: (prev row is not success) OR (date gap > 1)
#   5. A streak ends when: (next row is failure) AND (date gap == 1)
#   6. Use cumulative sum of "is_streak_start" flag to create a streak group ID
#   7. Group by user_id + streak_group, aggregate for start/end/length
#   8. Filter: only streaks where the day after streak_end is a "failure"
#   9. Pick the longest streak per user (tie-break by earliest start_date)

# COMMAND ----------

from pyspark.sql.functions import (
    col, lag, when, datediff, row_number,
    sum as _sum, min as _min, max as _max,
    count as _count, to_date, date_add
)
from pyspark.sql.window import Window

# Step 1: Convert login_date to date type
logins = logins.withColumn(
    'login_date', to_date('login_date')
)

# Step 2: Find previous status and date
w = Window.partitionBy('user_id').orderBy('login_date')

win1 = logins.withColumns({
    'prev_status': lag('status').over(w),
    'prev_date': lag('login_date').over(w)
})

# Step 3: Mark the beginning of every success streak
win1 = win1.withColumn(
    'is_streak_start',
    when(
        (col('status') == 'success') &
        (
            col('prev_status').isNull() |
            (col('prev_status') != 'success') |
            (datediff(col('login_date'), col('prev_date')) > 1)
        ),
        1
    ).otherwise(0)
)

# Step 4: Create a streak ID using cumulative sum
w_cumsum = (
    Window.partitionBy('user_id')
    .orderBy('login_date')
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

win1 = win1.withColumn(
    'streak_group',
    _sum('is_streak_start').over(w_cumsum)
)

# Step 5: Aggregate consecutive successful days
streaks = (
    win1.filter(col('status') == 'success')
    .groupBy('user_id', 'streak_group')
    .agg(
        _min('login_date').alias('streak_start_date'),
        _max('login_date').alias('streak_end_date'),
        _count('*').alias('streak_length')
    )
)

# Step 6: Keep only streaks followed by a failure
failures = (
    logins.filter(col('status') == 'failure')
    .select(
        'user_id',
        col('login_date').alias('failure_date')
    )
)

qualified = (
    streaks.join(
        failures,
        (streaks.user_id == failures.user_id) &
        (failures.failure_date == date_add(streaks.streak_end_date, 1)),
        'inner'
    )
    .select(
        streaks.user_id,
        'streak_start_date',
        'streak_end_date',
        'streak_length'
    )
)

# Step 7: Select the longest streak per user.
# Earliest start date wins ties.
w_rank = Window.partitionBy('user_id').orderBy(
    col('streak_length').desc(),
    col('streak_start_date').asc()
)

result = (
    qualified
    .withColumn('rn', row_number().over(w_rank))
    .filter(col('rn') == 1)
    .drop('rn')
)

result.orderBy('user_id').show()

# COMMAND ----------

# DBTITLE 1,Complete Solution
from pyspark.sql.functions import col, lag, lead, when, datediff, sum as _sum, row_number, to_date, max as _max, min as _min, count as _count
from pyspark.sql.window import Window

# ============================================================
# STEP 1: Convert login_date from string to date type
#         (needed for datediff to work correctly)
# ============================================================
df = logins.withColumn('login_date', to_date(col('login_date')))

# ============================================================
# STEP 2: Peek at adjacent rows using lag() and lead()
#         This tells us what came before and after each row
#         within each user's timeline (ordered by date)
# ============================================================
w = Window.partitionBy('user_id').orderBy('login_date')

df = df.withColumns({
    'prev_status': lag('status').over(w),
    'prev_date':   lag('login_date').over(w),
    'next_status': lead('status').over(w),
    'next_date':   lead('login_date').over(w)
})

print('=== After lag/lead ===')
df.orderBy('user_id', 'login_date').show()

# ============================================================
# STEP 3: Flag where a new streak STARTS
#         A new streak starts when:
#           - current row is 'success' AND
#           - (no previous row OR previous != 'success' OR date gap > 1)
#         This identifies the first row of each consecutive-success run
# ============================================================
df = df.withColumn(
    'is_streak_start',
    when(
        (col('status') == 'success') &
        (
            col('prev_status').isNull() |
            (col('prev_status') != 'success') |
            (datediff(col('login_date'), col('prev_date')) > 1)
        ),
        1
    ).otherwise(0)
)

print('=== After streak-start flags ===')
df.select('user_id', 'login_date', 'status', 'prev_status', 'prev_date', 'is_streak_start').orderBy('user_id', 'login_date').show()

# ============================================================
# STEP 4: Create a streak_group ID using cumulative sum
#         The cumulative sum of is_streak_start gives each
#         consecutive-success run a unique group number.
#         Failure rows get NULL (they break streaks).
# ============================================================
w_cumsum = Window.partitionBy('user_id').orderBy('login_date') \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = df.withColumn(
    'streak_group',
    when(col('status') == 'success', _sum('is_streak_start').over(w_cumsum)).otherwise(None)
)

print('=== After streak grouping ===')
df.select('user_id', 'login_date', 'status', 'is_streak_start', 'streak_group').orderBy('user_id', 'login_date').show()

# ============================================================
# STEP 5: Aggregate each streak group
#         Get start date, end date, and length (row count)
#         for each (user_id, streak_group) pair
# ============================================================
streaks = (df.filter(col('status') == 'success')
            .groupBy('user_id', 'streak_group')
            .agg(
                _min('login_date').alias('streak_start_date'),
                _max('login_date').alias('streak_end_date'),
                _count('login_id').alias('streak_length')
            ))

print('=== Streaks (before qualification) ===')
streaks.orderBy('user_id', 'streak_group').show()

# ============================================================
# STEP 6: Get the next_status and next_date for the LAST
#         success row in each streak. We need to know what
#         comes immediately after the streak ends.
# ============================================================
w_last = Window.partitionBy('user_id', 'streak_group') \
    .orderBy(col('login_date').desc())

last_success = (df.filter(col('status') == 'success')
                .withColumn('rn', row_number().over(w_last))
                .filter(col('rn') == 1)
                .select('user_id', 'streak_group', 'next_status', 'next_date'))

print('=== Last success row per streak (with next row info) ===')
last_success.orderBy('user_id', 'streak_group').show()

# ============================================================
# STEP 7: Join streaks with last_success and FILTER
#         A streak qualifies ONLY if:
#           - next_status = 'failure'  (the day after is a failure)
#           - datediff(next_date, streak_end_date) = 1  (it's the
#             immediately following calendar day, no gap)
#         This excludes streaks that run to the last available
#         date (next_status would be NULL) or have a gap after.
# ============================================================
streaks_with_next = streaks.join(last_success, ['user_id', 'streak_group'], 'inner')

qualifying = streaks_with_next.filter(
    (col('next_status') == 'failure') &
    (datediff(col('next_date'), col('streak_end_date')) == 1)
)

print('=== Qualifying streaks (followed by failure on next day) ===')
qualifying.select('user_id', 'streak_start_date', 'streak_end_date', 'streak_length').orderBy('user_id', 'streak_start_date').show()

# ============================================================
# STEP 8: Pick the LONGEST streak per user
#         Tie-break: earliest streak_start_date wins
#         Use row_number() with the right ordering, then keep rn=1
# ============================================================
w_best = Window.partitionBy('user_id') \
    .orderBy(col('streak_length').desc(), col('streak_start_date').asc())

result = (qualifying
          .withColumn('rn', row_number().over(w_best))
          .filter(col('rn') == 1)
          .select('user_id', 'streak_start_date', 'streak_end_date', 'streak_length')
          .orderBy('user_id'))

print('=== FINAL RESULT: Longest qualifying streak per user ===')
result.show()