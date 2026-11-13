# Databricks notebook source
# DBTITLE 1,DE 1st Round Interview Q&A Bank
# MAGIC %md
# MAGIC # DE 1st Round — Interview Q&A Bank
# MAGIC
# MAGIC Transcribed questions and passing answers from the "DE 1st round" interview script (22 Oct 2026), plus additional similar practice questions with model answers. Blocks: A — Data modeling, B — SQL, C — Query performance & RDBMS operations, D — Pipelines and migration.

# COMMAND ----------

# DBTITLE 1,Block A — Data modeling (from image)
# MAGIC %md
# MAGIC ## Block A — Data modeling (15 min) — from the image
# MAGIC
# MAGIC **A1. "Describe a schema you designed, or substantially changed. Walk me through it: the main tables, the keys, and the one relationship that was hardest to get right."** Probe once: *"Why that key?"* or *"What did that decision cost you later?"*
# MAGIC
# MAGIC **Passing answer contains:** concrete table and key names; at least one trade-off they chose deliberately (normalize vs denormalize, a key change, a split) **and what it cost**; something that turned out badly afterwards.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **A2. "A table holds one row per user per day with a status value. The business now wants to know a user's status on any given date — including dates where no row exists. What do you change, and what do you refuse to change?"**
# MAGIC
# MAGIC **Passing answer contains:** the observation that **a missing row is ambiguous** — it can mean "unknown" or "unchanged", and the business has to say which; then either (a) validity intervals (`valid_from` / `valid_to` per status change) or (b) a date spine joined at read time. States the cost of the option they pick — storage and write volume vs query complexity.
# MAGIC
# MAGIC **Model answer (A2):** "First I refuse to guess the semantics — I ask the business whether a day with no row means the status is *unknown* (the feed failed) or *unchanged* (status persisted). Those need different fixes. Say they confirm it means unchanged: I'd stop storing one row per user per day and instead store a change history — `user_status_changes(user_id, status, valid_from, valid_to)` — one row per status change, with the current row's `valid_to` open (NULL or 9999-12-31). Point-in-time lookup becomes: `WHERE user_id = ? AND valid_from <= :date AND (valid_to > :date OR valid_to IS NULL)`. What I refuse to do is backfill one row per user per day — it multiplies storage ~30x for zero information and re-creates the same gaps on the next feed outage. Cost I state: writes go from a daily append to an upsert on change, storage shrinks, and queries gain one extra predicate; the date-spine alternative keeps the daily table but needs a generated calendar join at read time — simpler writes, more expensive queries."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Model answer (A1) — a worked example you can adapt:** "I designed an e-commerce reporting schema: `customers(customer_id PK)`, `products(product_id PK)`, `orders(order_id PK, customer_id FK, order_ts)`, and `order_items(order_id, product_id, quantity, unit_price)` with a composite PK of (order_id, product_id). The hardest relationship was order-to-product — it's many-to-many, so I put a bridge table `order_items` between them and carried `unit_price` on the bridge, because the price *at purchase time* is a fact of the order, not of the product. A deliberate trade-off: I denormalized `customer_city` onto `orders` so regional dashboards avoid joining the 500M-row fact to the customer dimension — the cost was that address changes now fan out as updates across orders, and we accepted slightly stale city data for old orders. What turned out badly: we initially keyed `order_items` by a surrogate id, which let a rerun of the backfill load duplicate rows silently — we caught it weeks later, switched the key to (order_id, product_id) and made ingestion a MERGE upsert."

# COMMAND ----------

# DBTITLE 1,Block B — SQL (from image)
# MAGIC %md
# MAGIC ## Block B — SQL (15 min) — from the image
# MAGIC
# MAGIC Given tables:
# MAGIC ```
# MAGIC tenants(tenant_id, name)
# MAGIC customers(tenant_id, customer_id, last_active_at, status)
# MAGIC ```
# MAGIC
# MAGIC **B1. "Per tenant, count the customers whose last_active_at is within the last 30 days. Every tenant must appear in the result, including tenants where that count is zero."**
# MAGIC
# MAGIC **Passing answer:** LEFT JOIN from tenants, with the date predicate in the ON clause (or a pre-filtered subquery / `COUNT(...) FILTER`-style conditional aggregate), and a COUNT of a column from the right-hand table — **not COUNT(*)**.
# MAGIC
# MAGIC Model SQL:
# MAGIC ```sql
# MAGIC SELECT t.tenant_id,
# MAGIC        t.name,
# MAGIC        COUNT(c.customer_id) AS active_customers_30d
# MAGIC FROM tenants t
# MAGIC LEFT JOIN customers c
# MAGIC        ON c.tenant_id = t.tenant_id
# MAGIC       AND c.last_active_at >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY t.tenant_id, t.name;
# MAGIC ```
# MAGIC
# MAGIC Why it works: the date predicate lives in the **ON clause**, not the WHERE clause — putting it in WHERE would turn the LEFT JOIN into an inner join and drop the zero-count tenants. And `COUNT(c.customer_id)` counts non-NULL values only, so unmatched tenants correctly show 0 (`COUNT(*)` would show 1 for them).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **B2. "Someone added a join to a working query and now the result has duplicate rows. How do you find which join caused it? And how do you fix it without using SELECT DISTINCT?"**
# MAGIC
# MAGIC **Passing answer contains:** count rows before and after each join, or group by the key and look for counts greater than one, to locate the one-to-many; then fix by aggregating the many side before joining, or by joining against a deduplicated/keyed subquery. Can say why DISTINCT is a patch — it hides the fan-out instead of resolving it, and it changes the answer if the duplication is real.
# MAGIC
# MAGIC **Model answer (B2):** "I bisect the joins: run the query up to just before each join, count rows after each one, and the count that jumps is the guilty join — or prove it directly with `SELECT join_key, COUNT(*) FROM right_table GROUP BY 1 HAVING COUNT(*) > 1` to expose the one-to-many. A concrete example: someone joined `orders` to a `payments` table where an order can have multiple partial payments — every order row now multiplies. Two clean fixes, no DISTINCT: either **aggregate the many side first** (`SELECT order_id, SUM(amount) FROM payments GROUP BY order_id`, then join) if I need one number per order; or **dedupe with a window** (`ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC) = 1`) if I need one *row* per order. If the duplicates are in the base table itself, that's a data bug, not a query bug — fix the ingestion, don't paper over it. DISTINCT is wrong here because it silently collapses legitimate multiple payments too — it changes the totals, not just the row count."

# COMMAND ----------

# DBTITLE 1,Block C — Query performance (from image)
# MAGIC %md
# MAGIC ## Block C — Query performance and RDBMS operations (10 min) — from the image
# MAGIC
# MAGIC **C1. "A query against a 50-million-row table has become slow as the table grew. Walk me through what you do, in order."**
# MAGIC
# MAGIC **Passing answer contains:** read the execution plan first (EXPLAIN); compare rows examined against rows returned; check whether an index is used and whether its columns are selective; consider composite index column order; **measure before and after**.
# MAGIC
# MAGIC **Model answer (C1), in order:** "(1) Establish the baseline — record the current runtime and capture `EXPLAIN ANALYZE`. (2) Read the plan: is it a full scan, and how many rows are *examined* vs *returned*? Examining 50M rows to return 200 is the smoking gun. (3) Check the WHERE/JOIN predicates: are they sargable (no functions wrapped around the columns), and are the filtered columns selective enough for an index to help? (4) If a composite index exists or is proposed, order its columns: equality predicates first, then the range/sort column — e.g. for `WHERE tenant_id = ? AND created_at >= ?`, index (tenant_id, created_at), not the reverse. (5) Check statistics are fresh — a stale-stats misestimate can flip the plan to a scan. (6) Apply one change at a time, re-run EXPLAIN, re-measure against the baseline — a change isn't an improvement until it's measured."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **C2. "You have a composite index on (a, b). Which of these can use it?"** — filter on a only · filter on b only · filter on both · filter on a and order by b
# MAGIC
# MAGIC **Correct:** a only — **yes**. b only — **no** (leftmost prefix). Both — **yes**. Filter a + order by b — **yes**, and the sort can be avoided.
# MAGIC
# MAGIC **Why:** the index physically stores rows sorted by a, then by b *within each a*. Filtering on b alone can't skip anything — b's ordering is only meaningful inside a values of a, so the engine would have to scan it all (leftmost-prefix rule). Filtering on a narrows to one contiguous slice. And within that slice rows are already ordered by b, so `WHERE a = 5 ORDER BY b` needs no sort step — the index delivers the order for free.

# COMMAND ----------

# DBTITLE 1,Block D — Pipelines and migration (from image)
# MAGIC %md
# MAGIC ## Block D — Pipelines and migration (10 min) — from the image
# MAGIC
# MAGIC **D1. "Tell me about a time you moved data from one store to another. How did you prove the migration was correct?"**
# MAGIC
# MAGIC **Passing answer contains:** a specific verification method — row counts and aggregate checksums per partition, key-level sampling, dual-read or shadow comparison over a window — plus a defined cutover and a rollback path.
# MAGIC
# MAGIC **Model answer (D1):** "Last year we moved a Postgres order-history store to a lakehouse table. Verification, before any cutover: (1) per-partition row counts on both sides, exact match per day; (2) aggregate checksums per partition — `SUM(amount)`, `COUNT(DISTINCT customer_id)` — so a row swap that keeps counts equal still gets caught; (3) key-level sampling — pulled ~1000 random `order_id`s and compared every column side by side. Then a shadow period: for two weeks we wrote to both stores and read from the old one while a job diffed the new store's answers against the old — mismatches went to a report, not a failure. Cutover was a defined moment: switch reads at a low-traffic window after two clean days of shadow comparison, with rollback defined in advance — the old store stayed populated and read-only for 30 days, so reverting was a config flip, not a data-recovery exercise."

# COMMAND ----------

# DBTITLE 1,Similar questions — Block A
# MAGIC %md
# MAGIC ---
# MAGIC # Similar Practice Questions & Answers
# MAGIC
# MAGIC ## Block A style — Data modeling
# MAGIC
# MAGIC **A3. "A fact table got duplicate rows after a backfill reran. How do you design the table so this can't happen again?"**
# MAGIC
# MAGIC **Model answer:** The root cause is that ingestion was not idempotent. Fix at the design level: define a business key (e.g. `order_id` + `event_date`, not a surrogate or event timestamp), then write with `MERGE INTO ... ON business_key` (upsert) or `INSERT OVERWRITE` the affected partition instead of blind append. For recovery, dedupe with `ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY ingested_at DESC) = 1` into a new table, then swap. Mention that a surrogate auto-increment key hides duplicates, which is exactly why detection was late.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **A4. "When would you deliberately denormalize a schema, and what does it cost you?"**
# MAGIC
# MAGIC **Model answer:** Denormalize when read queries dominate and joins are the bottleneck — e.g. pre-joining a wide `orders_enriched` table for a dashboard, or storing `customer_city` on orders so analysts don't join a 500M-row fact to a dimension. Costs: storage grows, a single fact about the world (the customer's city) lives in many rows so updates must fan out, and stale copies drift — the classic update anomaly. The trade to state: pay at write time to make reads cheap and simple, and accept staleness or fan-out updates.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **A5. "Explain SCD Type 1 vs Type 2. A business says 'we need to know what a customer's segment was at the time of each order' — which do you pick and why?"**
# MAGIC
# MAGIC **Model answer:** Type 1 overwrites in place — the dimension always shows the current state; history is lost. Type 2 adds `valid_from` / `valid_to` (or an `is_current` flag) and inserts a new row per change, preserving history. For point-in-time questions you must pick Type 2, and the order fact joins on both `customer_id` **and** the order date falling in the validity interval. Cost to state: dimension grows with change frequency, the join becomes a range join (more expensive), and queries must remember the date predicate.

# COMMAND ----------

# DBTITLE 1,Similar questions — Block B
# MAGIC %md
# MAGIC ## Block B style — SQL
# MAGIC
# MAGIC **B3. "Return each customer's second-latest order (customer_id, order_id, order_date). What if two orders share the same date?"**
# MAGIC
# MAGIC **Model answer:** Window function, not `LIMIT 2`: `DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC)` and keep `rank = 2`. State the tie rule: DENSE_RANK treats same-date orders as equal rank, so "second distinct date"; if instead they want the second row regardless of ties, use `ROW_NUMBER()`. The choice must be stated, not accidental. `RANK()` differs from both (gaps after ties) — a strong answer names all three.
# MAGIC
# MAGIC ```sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT customer_id, order_id, order_date,
# MAGIC          DENSE_RANK() OVER (PARTITION BY customer_id
# MAGIC                             ORDER BY order_date DESC) AS rnk
# MAGIC   FROM orders)
# MAGIC SELECT customer_id, order_id, order_date
# MAGIC FROM ranked WHERE rnk = 2;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **B4. "Find customers who placed orders in January AND February but NOT in March."**
# MAGIC
# MAGIC **Model answer:** Group by customer and use conditional aggregation — one pass over the table:
# MAGIC ```sql
# MAGIC SELECT customer_id
# MAGIC FROM orders
# MAGIC WHERE order_date >= DATE '2026-01-01' AND order_date < DATE '2026-04-01'
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(DISTINCT month(order_date)) = 2   -- Jan and Feb present
# MAGIC    AND SUM(CASE WHEN month(order_date) = 3 THEN 1 ELSE 0 END) = 0;
# MAGIC ```
# MAGIC Or with set operators: intersect the Jan and Feb sets, then `EXCEPT`/anti-join the March set. The failing answer is three separate queries joined in application code.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **B5. "What is the difference between COUNT(*), COUNT(col), and COUNT(DISTINCT col)? Why does the choice matter in a LEFT JOIN?"**
# MAGIC
# MAGIC **Model answer:** `COUNT(*)` counts rows including NULLs; `COUNT(col)` counts non-NULL values of that column; `COUNT(DISTINCT col)` counts distinct non-NULL values. In a LEFT JOIN, `COUNT(*)` returns at least 1 for every left row (the row with NULLs on the right), so it reports zero-count groups as 1 — that's why B1 requires counting a right-hand column. Also note `COUNT(DISTINCT a, b)` is unavailable in some engines; the portable version is `COUNT(DISTINCT concat(a, '|', b))` with care for NULLs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **B6. "A join between two tables silently returns fewer rows than expected. No error was raised. Give three possible causes."**
# MAGIC
# MAGIC **Model answer:** (1) Type mismatch on the key — e.g. `int` vs `string` or `"1"` vs `"1 "` (trailing space) — makes equality fail silently. (2) NULLs in the join key: `NULL = NULL` is not true, so those rows drop from an inner join. (3) Wrong grain — joining monthly aggregates to daily facts on customer_id only multiplies rows, but the reverse mistake (joining on a too-specific key, or a half-applied filter on one side) can drop rows. Diagnostic habit: compare row counts and distinct key counts on each side before and after the join.

# COMMAND ----------

# DBTITLE 1,Similar questions — Block C
# MAGIC %md
# MAGIC ## Block C style — Query performance & operations
# MAGIC
# MAGIC **C3. "The plan shows a full table scan even though an index exists on the filtered column. Give four reasons this can happen."**
# MAGIC
# MAGIC **Model answer:** (1) Non-sargable predicate — a function or expression wraps the column: `WHERE YEAR(order_date) = 2026` or `WHERE UPPER(name) = 'X'` can't use a plain index; rewrite as a range predicate or use a function-based index. (2) Leading wildcard: `LIKE '%smith'` cannot seek. (3) Low selectivity — the filter matches a large fraction of rows, so the optimizer correctly prefers a scan. (4) Stale statistics or implicit type conversion (filtering a string column with a number) preventing the optimizer from using the index. Bonus: the table is small enough that a scan is cheaper.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **C4. "EXPLAIN shows a nested-loop join over 10M × 10M rows. What do you check and change?"**
# MAGIC
# MAGIC **Model answer:** Nested loop over two large inputs means the optimizer thinks one side is tiny (bad stats or a row estimate blow-up from an earlier operator) or no equi-join key was available. Check: row estimates vs reality in the plan, whether the join predicate is an equality (inequality joins force nested loops), and whether indexes exist on both join keys. Fixes: refresh statistics, add indexes on join keys, supply a `BROADCAST`/`HASH` hint, or pre-aggregate one side. Then **re-run EXPLAIN and measure** — same discipline as C1.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **C5. "A table is partitioned by event_date. One query filters on event_date and runs in seconds; a nearly identical one scans everything. What is the difference likely to be?"**
# MAGIC
# MAGIC **Model answer:** The slow one almost certainly wraps the partition column in a function or cast — `WHERE date_format(event_date, 'yyyy-MM') = '2026-01'` or `CAST(event_date AS STRING) = '2026-01-01'` — which defeats partition pruning because the engine can't map the expression to a partition list. The fix is to write the predicate as a raw range on the partition column: `event_date >= DATE '2026-01-01' AND event_date < DATE '2026-02-01'`. Same principle as the sargability rule in C3: keep the indexed/partitioned column bare on one side of the comparison.

# COMMAND ----------

# DBTITLE 1,Similar questions — Block D
# MAGIC %md
# MAGIC ## Block D style — Pipelines, migration & reliability
# MAGIC
# MAGIC **D2. "How do you make a pipeline re-runnable so that a failure and retry never duplicates data?"**
# MAGIC
# MAGIC **Model answer:** Idempotent writes: `MERGE` keyed on a business key, or overwrite-by-partition (`REPLACE WHERE` / dynamic partition overwrite) for time-windowed batches, so rerunning a window replaces exactly that window. Include dedup at read (`ROW_NUMBER ... = 1`) as a safety net for sources that can't guarantee exactly-once, and record a watermark/high-water mark per run so reruns resume instead of replaying everything. The failing answer is "append and hope" or relying on at-most-once delivery, which loses data instead.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **D3. "An upstream feed delivers records late — sometimes by hours, sometimes by days. How do you handle it, and how would you detect it?"**
# MAGIC
# MAGIC **Model answer:** Handle: don't process by event time windows that close too early — use a reprocessing window (recompute the last N days each run) or streaming watermarks with allowed lateness, and design downstream consumers to tolerate corrections (MERGE, not append). Detect: track the lag between `event_time` and `ingested_time` (a distribution, not just an average), and alert on records arriving outside the expected window. Verification mindset from D1 applies: dual-count what the pipeline produced vs what the source says it sent for a given event-date window.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **D4. "What data-quality checks would you put on a pipeline, and where do bad records go?"**
# MAGIC
# MAGIC **Model answer:** Row-count deltas vs the previous run and vs source, freshness (max event_time), null/duplicate rates on keys, referential integrity (orphans vs dimension), and business invariants (amounts >= 0, statuses in an enum). Bad records are **quarantined, not dropped**: write them to a separate dead-letter table with the failure reason and run id, alert on volume, and make reprocessing possible. The failing answer is either failing the whole run on one bad row or silently skipping rows — both destroy trust in the numbers.