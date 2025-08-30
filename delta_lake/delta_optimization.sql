/*
Delta Lake Optimization Cheat Sheet (Databricks)
================================================

Purpose: Highly actionable commands, playbooks, and rules-of-thumb to keep Delta tables fast and healthy.

Quick Rules of Thumb
--------------------
- Write fewer, bigger files: target ~128–512 MB data files.
- Partition by low-cardinality, frequently filtered columns (e.g., date), not by high-cardinality.
- Use Z-ORDER for high-cardinality columns used in filters/joins. Prefer ≤4 columns.
- Prefer Liquid Clustering over periodic Z-ORDER for large, evolving tables.
- Enable optimizeWrite and autoCompact at table/cluster level to curb small files from day one.
- Schedule OPTIMIZE daily and VACUUM weekly (≥7 days retention in prod).
- Always test VACUUM with DRY RUN first; don’t break time travel requirements.
*/

-- 0) Minimal Table Definition You’ll Reuse (SQL)
----------------------------------------------
-- Managed table
CREATE TABLE IF NOT EXISTS my_catalog.my_schema.sales (
	id BIGINT,
	ts TIMESTAMP,
	country STRING,
	amount DOUBLE
) USING DELTA
PARTITIONED BY (date(ts))
TBLPROPERTIES (
	delta.minReaderVersion = '2',
	delta.minWriterVersion = '7',
	delta.appendOnly = 'false',
	delta.autoOptimize.optimizeWrite = true,
	delta.autoOptimize.autoCompact = true,
	delta.logRetentionDuration = '30 days',
	delta.deletedFileRetentionDuration = '7 days'
);

-- 1) Optimize Write + Auto Compaction
-----------------------------------
-- Table-level (recommended)
ALTER TABLE my_catalog.my_schema.sales SET TBLPROPERTIES (
	delta.autoOptimize.optimizeWrite = true,
	delta.autoOptimize.autoCompact = true
);

-- Session-level toggles (quick experiments; not for prod permanence)
SET spark.databricks.delta.optimizeWrite.enabled = true;
SET spark.databricks.delta.autoCompact.enabled = true;

-- Python (Spark) session options
-- spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
-- spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

-- When to use: Always enable on write-heavy tables (batch or streaming) to reduce small-file proliferation at the source.


-- 2) OPTIMIZE + Z-ORDER (Query Speed via File/Locality Layout)
------------------------------------------------------------
-- Compact small files and co-locate related data (bin-packing + Z-ORDER)
OPTIMIZE my_catalog.my_schema.sales
ZORDER BY (ts, country);

-- Ad-hoc optimize on recent data only
OPTIMIZE my_catalog.my_schema.sales
WHERE ts >= date_sub(current_date(), 30)
ZORDER BY (ts);

-- Python API
-- from delta.tables import DeltaTable
-- delta_table = DeltaTable.forName(spark, "my_catalog.my_schema.sales")
-- delta_table.optimize().executeCompaction()
-- delta_table.optimize().executeZOrderBy("ts", "country")

/*
Guidance:
- Use after large batch writes or when many small files accumulate.
- Z-ORDER high-cardinality, frequently filtered/joined columns (≤4 cols).
- Don’t Z-ORDER low-cardinality columns (country/boolean) — partition those instead.
*/

-- 3) Liquid Clustering (Self-Healing Layout at Scale)
---------------------------------------------------
-- Turn on Liquid Clustering for a table (replaces periodic manual Z-ORDER)
ALTER TABLE my_catalog.my_schema.sales CLUSTER BY (ts, id);

-- Inspect clustering
DESCRIBE DETAIL my_catalog.my_schema.sales; -- look for clustering columns
SHOW TBLPROPERTIES my_catalog.my_schema.sales LIKE 'delta.liquid%';

-- Apply maintenance (reclustering runs under OPTIMIZE)
OPTIMIZE my_catalog.my_schema.sales; -- respects liquid clustering

-- Turn off Liquid Clustering
ALTER TABLE my_catalog.my_schema.sales DROP CLUSTER BY;

-- When to use: Large/hot tables with continuous writes and diverse query patterns. Prefer LC over repeated Z-ORDER for simpler ops and more stable performance.


4) Partitioning (Pruning, Not a Silver Bullet)
----------------------------------------------
-- Create with partitioning (low-cardinality columns only)
CREATE TABLE my_catalog.my_schema.events USING DELTA
PARTITIONED BY (dt) AS
SELECT *, CAST(ts AS DATE) AS dt FROM source_view;

-- Query with partition filters for pruning
SELECT * FROM my_catalog.my_schema.events WHERE dt BETWEEN '2025-01-01' AND '2025-01-31';

-- Changing partitioning = CTAS into a new table (no in-place repartition)
CREATE OR REPLACE TABLE my_catalog.my_schema.events_repart USING DELTA
PARTITIONED BY (dt)
AS SELECT *, CAST(ts AS DATE) AS dt FROM my_catalog.my_schema.events;

/*
Guidance:
- Choose partitions you almost always filter by (date, region). Keep count per partition reasonable.
- Avoid high-cardinality partitions (user_id) — leads to too many tiny files/directories.
- Combine with LC or Z-ORDER to improve within-partition data skipping.
*/

-- 5) Bucketing (Don’t for Delta on Databricks)
--------------------------------------------
/*
- Classic Hive-style bucketing is not supported for Delta Lake on Databricks and generally not recommended.
- Prefer partitioning + Liquid Clustering or Z-ORDER.
- If you see CLUSTER BY in CTAS/INSERT, that’s a write-time shuffle/ordering hint, not persistent bucketing.
*/

-- 6) VACUUM (Safe File Cleanup)
-----------------------------
-- Always start with a dry run
VACUUM my_catalog.my_schema.sales DRY RUN;

-- Safe cleanup (default minimum retention = 7 days)
VACUUM my_catalog.my_schema.sales RETAIN 168 HOURS; -- 7 days

-- If you must go lower (dev only), disable safety check for your session (NOT prod)
-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
-- VACUUM my_catalog.my_schema.sales RETAIN 24 HOURS;

/*
Best practices:
- Keep ≥7 days in prod to preserve time travel SLAs and avoid data loss during long-running jobs.
- Schedule weekly during maintenance windows; test with DRY RUN.
- Align delta.deletedFileRetentionDuration with your recovery requirements.
*/

7) Health Checks & When to Act
------------------------------
-- Table details & history
DESCRIBE DETAIL my_catalog.my_schema.sales;
DESCRIBE HISTORY my_catalog.my_schema.sales;

-- Basic signal: file count vs average file size (needs OPTIMIZE if too many small files)
ANALYZE TABLE my_catalog.my_schema.sales COMPUTE STATISTICS;

-- Heuristic: tables with many files and small avg file size
SELECT 
	table_schema,
	table_name,
	num_files,
	size_in_bytes / NULLIF(num_files, 0) AS avg_file_bytes,
	CASE WHEN num_files > 1000 AND (size_in_bytes / NULLIF(num_files, 1)) < 134217728 THEN 'NEEDS_OPTIMIZE' ELSE 'OK' END AS status
FROM system.information_schema.table_storage
WHERE table_catalog = 'my_catalog' AND table_schema = 'my_schema'
ORDER BY num_files DESC
LIMIT 100;


-- 8) Maintenance Jobs (Templates)
-------------------------------
-- Daily OPTIMIZE (off-peak)
-- for table in ["catalog.schema.table1", "catalog.schema.table2"]:
--     spark.sql(f"OPTIMIZE {table}")

-- Weekly VACUUM (30 days retention)
-- for table in ["catalog.schema.table1", "catalog.schema.table2"]:
--     spark.sql(f"VACUUM {table} RETAIN 720 HOURS")

-- With Liquid Clustering enabled, a simple OPTIMIZE will recluster as needed.

-- 9) Copy/Paste Fast Start Block (SQL)
-------------------------------------
-- Enable table-level auto optimizations
ALTER TABLE my_catalog.my_schema.sales SET TBLPROPERTIES (
	delta.autoOptimize.optimizeWrite = true,
	delta.autoOptimize.autoCompact = true
);

-- Optimize + Z-ORDER (or rely on Liquid Clustering if enabled)
OPTIMIZE my_catalog.my_schema.sales ZORDER BY (ts, country);

-- Turn on Liquid Clustering (optional, modern default for big tables)
ALTER TABLE my_catalog.my_schema.sales CLUSTER BY (ts, id);

-- Weekly cleanup
VACUUM my_catalog.my_schema.sales RETAIN 168 HOURS; -- 7 days

-- Health
DESCRIBE DETAIL my_catalog.my_schema.sales;
DESCRIBE HISTORY my_catalog.my_schema.sales;

-- 11) Decision Guide (1-minute)
-----------------------------
/*
- Small files growing? Enable optimizeWrite + autoCompact; run OPTIMIZE on hot partitions.
- Slow point lookups/joins on high-card columns? Z-ORDER or enable Liquid Clustering on those columns.
- Heavy daily appends on big tables? Prefer Liquid Clustering over recurring Z-ORDER.
- Queries always filter by date? Partition by date and include date filter in queries.
- Considering bucketing? Don’t on Delta in Databricks; use LC/Z-ORDER instead.
*/