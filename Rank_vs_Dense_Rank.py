# Databricks notebook source
# DBTITLE 1,RANK vs DENSE_RANK
# MAGIC %md
# MAGIC # RANK vs DENSE_RANK — When Each Is Required
# MAGIC
# MAGIC One medium question that **cannot** be solved with DENSE_RANK. The difference: RANK counts *positions* (ties occupy multiple positions, so gaps appear: 1, 2, 2, 5), while DENSE_RANK counts *distinct levels* (no gaps: 1, 2, 2, 3).

# COMMAND ----------

# DBTITLE 1,Question: Top-3 Placement Offers (RANK required)
# MAGIC %md
# MAGIC ## Question (Medium): Top-3 Placement Offers — solvable ONLY with RANK
# MAGIC
# MAGIC A company makes offers to the **top 3 positions** in a placement test. The position rule: a candidate's `position` = **1 + the number of candidates who scored strictly higher**. Candidates who tie share a position — so a tie can expand the offer cohort beyond 3 (anyone tied with an offer-receiver also gets an offer).
# MAGIC
# MAGIC Given the `candidates` table, return every offer-receiver: `candidate_id`, `candidate_name`, `score`, `position`. Order by `position`, then `candidate_name`.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | candidate_id | candidate_name | score | position |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Aarav | 95 | 1 |
# MAGIC | 2 | Bela | 90 | 2 |
# MAGIC | 3 | Chirag | 90 | 2 |
# MAGIC | 4 | Diya | 90 | 2 |
# MAGIC
# MAGIC **Why DENSE_RANK fails here:** three candidates tied at 90 push Eshan (85) to *position 5* — outside the top 3 — so he gets no offer. But 85 is only the 3rd **distinct level** (95, 90, 85), so `DENSE_RANK() OVER (ORDER BY score DESC) <= 3` ranks Eshan 3 and hands him an offer he shouldn't get. After solving with RANK, re-run your query with DENSE_RANK and watch Eshan wrongly appear.
# MAGIC
# MAGIC **The rule of thumb:**
# MAGIC
# MAGIC * **RANK** → quotas, cutoffs, and positions: "top N people", "how many are ahead of me", " medalists". Ties take up slots.
# MAGIC * **DENSE_RANK** → Nth *distinct value*: "2nd highest salary", "3rd best score level". Ties don't take up slots.
# MAGIC * **ROW_NUMBER** → exactly N rows, arbitrary tie-breaking.

# COMMAND ----------

# DBTITLE 1,Question: Sample Data
# Sample data for the placement question
candidates = spark.createDataFrame(
    [(1, "Aarav", 95),
     (2, "Bela", 90),
     (3, "Chirag", 90),
     (4, "Diya", 90),
     (5, "Eshan", 85),
     (6, "Farida", 80)],
    ["candidate_id", "candidate_name", "score"]
)
candidates.createOrReplaceTempView("candidates")
display(candidates)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

w = Window.orderBy(col('score').desc())
candidates.withColumn('position', rank().over(w)).filter(col('position') <= 3).orderBy('position', 'candidate_name').show()