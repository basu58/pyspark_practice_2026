# Databricks notebook source
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
from pyspark.sql.functions import col, lag, lead, when, datediff, row_number, max as _max, min as _min, count as _count
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