-- 🔧 Delta Lake Maintenance & Best Practices (SQL)
-- Copy/paste into a Databricks SQL editor or notebook cell.

-- 1) Table creation patterns -------------------------------------------------
-- Managed table (stored in workspace-managed storage)
CREATE TABLE IF NOT EXISTS my_catalog.my_schema.sales (
  id BIGINT,
  ts TIMESTAMP,
  country STRING,
  amount DOUBLE
) USING DELTA
TBLPROPERTIES (
  delta.minReaderVersion = '2',
  delta.minWriterVersion = '7',
  delta.appendOnly = 'false'
);

-- External table (Unity Catalog) using an external location
-- CREATE TABLE my_catalog.my_schema.sales_ext
-- USING DELTA LOCATION 's3://your-bucket/path/to/table';

-- 2) Table properties you’ll actually use ------------------------------------
ALTER TABLE my_catalog.my_schema.sales SET TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true,
  delta.logRetentionDuration = '30 days',
  delta.deletedFileRetentionDuration = '7 days'
);

-- 3) OPTIMIZE + Z-ORDER for faster queries ----------------------------------
OPTIMIZE my_catalog.my_schema.sales
ZORDER BY (ts, country);

-- 4) VACUUM to remove old files (safe retention) -----------------------------
-- Default minimum retention is 7 days. To vacuum earlier, you must override
-- the safety check for the session (not recommended in prod).
-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;  -- use with caution
VACUUM my_catalog.my_schema.sales RETAIN 168 HOURS; -- 7 days

-- 5) Evolve schema safely -----------------------------------------------------
-- Add column
ALTER TABLE my_catalog.my_schema.sales ADD COLUMNS (channel STRING);
-- Drop column
ALTER TABLE my_catalog.my_schema.sales DROP COLUMN channel;

-- 6) Constraints and generated columns --------------------------------------
ALTER TABLE my_catalog.my_schema.sales ADD CONSTRAINT non_negative_amount CHECK (amount >= 0);

ALTER TABLE my_catalog.my_schema.sales ADD COLUMNS (
  dt DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
);

-- 7) Convert Parquet to Delta (one-time) -------------------------------------
-- CONVERT TO DELTA parquet.`s3://your-bucket/path/to/parquet-folder`;

-- 8) Time travel & cloning ----------------------------------------------------
-- Query a previous version
SELECT * FROM my_catalog.my_schema.sales VERSION AS OF 3;
SELECT * FROM my_catalog.my_schema.sales TIMESTAMP AS OF '2025-01-01T00:00:00Z';

-- Create a shallow clone (fast dev/test copy)
CREATE OR REPLACE TABLE my_catalog.my_schema.sales_dev SHALLOW CLONE my_catalog.my_schema.sales;

-- 9) Table health -------------------------------------------------------------
DESCRIBE DETAIL my_catalog.my_schema.sales;
DESCRIBE HISTORY my_catalog.my_schema.sales;

-- 10) Compact small files ad-hoc ---------------------------------------------
-- Rewrites files into ~128MB chunks (bin-packing)
OPTIMIZE my_catalog.my_schema.sales WHERE ts >= date_sub(current_date(), 30);

-- Pro Tip: Schedule OPTIMIZE nightly and VACUUM weekly; Z-ORDER on hottest columns.


-- ============================================================================
-- Databricks Delta Lake Optimization: Production Performance Guide
-- ============================================================================

-- --- 1. OPTIMIZE: File Compaction for Query Performance ---------------------
-- Use when you have many small files (< 128MB) degrading query speed.

-- See earlier section for OPTIMIZE/Z-ORDER SQL examples. Below are Python APIs.

-- Python API for OPTIMIZE in notebooks (commented for SQL file compatibility)
-- from delta.tables import DeltaTable
-- delta_table = DeltaTable.forPath(spark, "/path/to/delta/table")
-- delta_table.optimize().executeCompaction()

-- Z-ORDER via Python API
-- delta_table.optimize().executeZOrderBy("user_id", "date", "country")

-- --- 2. VACUUM: Clean Up Old Files -----------------------------------------
-- Removes files older than retention period (default: 7 days).
-- WARNING: Never VACUUM with retention < 7 days in production!

-- VACUUM with custom retention (30 days for extra safety)
VACUUM my_catalog.my_schema.my_table RETAIN 720 HOURS;

-- Dry run to see what would be deleted (always test first!)
VACUUM my_catalog.my_schema.my_table DRY RUN;

-- Python API for VACUUM
-- delta_table.vacuum(retentionHours=720)  -- 30 days

-- --- 3. Automated Maintenance Jobs -----------------------------------------
-- Schedule these commands as Databricks jobs for hands-off maintenance.

-- Daily OPTIMIZE job (run during off-peak hours)
-- Frequency: Daily at 2 AM
-- Cluster: Single node (sufficient for maintenance tasks)
-- tables_to_optimize = [
--     "catalog.schema.table1",
--     "catalog.schema.table2", 
--     "catalog.schema.table3"
-- ]
-- for table in tables_to_optimize:
--     print(f"Optimizing {table}...")
--     spark.sql(f"OPTIMIZE {table}")
--     print(f"Completed {table}")

-- Weekly VACUUM job (run during maintenance window)
-- Frequency: Weekly on Sunday at 1 AM
-- for table in tables_to_optimize:
--     print(f"Vacuuming {table}...")
--     spark.sql(f"VACUUM {table} RETAIN 720 HOURS")
--     print(f"Vacuumed {table}")

-- --- 4. Monitoring & Metrics -----------------------------------------------
-- Check table health and optimization opportunities.

-- View table details and file statistics (see earlier section for DESCRIBE DETAIL/HISTORY)

-- Check file count and sizes (too many small files = needs OPTIMIZE)
SHOW TABLE EXTENDED LIKE 'my_table';

-- Analyze table statistics after OPTIMIZE
ANALYZE TABLE my_catalog.my_schema.my_table COMPUTE STATISTICS;

-- Query to find tables needing optimization (many small files)
SELECT 
  table_name,
  num_files,
  size_in_bytes / (1024 * 1024 * 1024) as size_gb,
  CASE 
    WHEN num_files > 1000 AND (size_in_bytes / num_files) < 134217728 -- 128MB
    THEN 'NEEDS_OPTIMIZE'
    ELSE 'OK'
  END as optimization_status
FROM information_schema.tables
WHERE table_schema = 'my_schema'
  AND table_type = 'EXTERNAL'
ORDER BY num_files DESC;

-- --- 5. Best Practices ------------------------------------------------------
-- OPTIMIZE Timing: 
--   - After large batch writes
--   - When query performance degrades
--   - When you see >1000 files per partition

-- Z-ORDER Columns (max 4 columns):
--   - High-cardinality columns used in WHERE/JOIN
--   - Columns with range queries (dates, numeric IDs)
--   - NOT low-cardinality (country, gender, etc.)

-- VACUUM Safety:
--   - Never < 7 days retention in production
--   - Test with DRY RUN first
--   - Schedule during maintenance windows
--   - Consider time travel requirements

-- Partitioning + Z-ORDER:
--   - Partition by low-cardinality (date, region)
--   - Z-ORDER by high-cardinality within partitions

-- Pro Tip: Monitor Delta metrics in Databricks SQL or create alerts
-- for tables with degrading performance indicators.
-- More guides at jakublasak.com
