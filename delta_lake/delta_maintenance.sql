-- 🔧 Delta Lake Maintenance (SQL + PySpark)
-- Copy/paste into a Databricks SQL editor or notebook cell.
-- Scope: maintenance essentials excluding VACUUM, Z-ORDER, Liquid Clustering,
--         Partitioning, Bucketing, Auto Compaction, Optimize Write.

-- ============================================================================
-- 1) Create/Register Tables Safely
-- ============================================================================
-- Managed table (Unity Catalog recommended)
CREATE TABLE IF NOT EXISTS my_catalog.my_schema.sales (
	id BIGINT GENERATED ALWAYS AS IDENTITY,
	ts TIMESTAMP,
	country STRING,
	amount DOUBLE,
	-- Useful derived column for pruning
	dt DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
) USING DELTA
TBLPROPERTIES (
	delta.minReaderVersion = '2',
	delta.minWriterVersion = '7',
	-- Enables safe column renames/drops with name-based column mapping
	delta.columnMapping.mode = 'name',
	-- Retention and housekeeping (see §3)
	delta.logRetentionDuration = '30 days',
	delta.deletedFileRetentionDuration = '7 days'
);

-- External table (register an existing Delta location)
-- CREATE TABLE my_catalog.my_schema.sales_ext
-- USING DELTA LOCATION 's3://your-bucket/path/to/delta-table';

-- Convert existing Parquet folder (one-time)
-- CONVERT TO DELTA parquet.`s3://your-bucket/path/to/parquet-folder`;


-- ============================================================================
-- 2) Schema Enforcement & Evolution
-- ============================================================================
-- Explicit changes
ALTER TABLE my_catalog.my_schema.sales ADD COLUMNS (channel STRING);
ALTER TABLE my_catalog.my_schema.sales RENAME COLUMN channel TO sales_channel;
ALTER TABLE my_catalog.my_schema.sales DROP COLUMN sales_channel;

-- Widening/compatible type change (when supported)
-- ALTER TABLE my_catalog.my_schema.sales ALTER COLUMN amount SET DATA TYPE DECIMAL(18,2);

-- Enable automatic schema merges for MERGE/append (session-level)
-- Use sparingly and log changes.
SET spark.databricks.delta.schema.autoMerge.enabled = true;

-- Upsert pattern (SCD0/1-like)
MERGE INTO my_catalog.my_schema.sales as t
USING my_catalog.my_schema.sales_updates as s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;


-- ============================================================================
-- 3) Housekeeping Table Properties
-- ============================================================================
-- Adjust retention (affects time travel and log size)
ALTER TABLE my_catalog.my_schema.sales SET TBLPROPERTIES (
	delta.logRetentionDuration = '30 days',            -- keep transaction log
	delta.deletedFileRetentionDuration = '7 days',     -- keep deleted files
	delta.checkpointInterval = 10,                     -- commits between checkpoints
	delta.enableChangeDataFeed = true,                 -- enable CDF
	delta.appendOnly = 'false'                         -- set to 'true' for append-only tables
);

-- Inspect current properties
SHOW TBLPROPERTIES my_catalog.my_schema.sales;


-- ============================================================================
-- 4) Constraints, Identity, Defaults, Generated Columns
-- ============================================================================
-- Data quality checks
ALTER TABLE my_catalog.my_schema.sales ADD CONSTRAINT non_negative_amount CHECK (amount >= 0);
-- Drop constraint
-- ALTER TABLE my_catalog.my_schema.sales DROP CONSTRAINT non_negative_amount;

-- NOT NULL & DEFAULT values
ALTER TABLE my_catalog.my_schema.sales ALTER COLUMN country SET NOT NULL;
ALTER TABLE my_catalog.my_schema.sales ALTER COLUMN country SET DEFAULT 'UNK';

-- Identity already defined in DDL. Example for a new table:
-- id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)


-- ============================================================================
-- 5) Time Travel, Restore, and Clones
-- ============================================================================
-- Query historical data
SELECT * FROM my_catalog.my_schema.sales VERSION AS OF 10;
SELECT * FROM my_catalog.my_schema.sales TIMESTAMP AS OF '2025-01-01 00:00:00';

-- Restore table to a previous version/timestamp (atomic)
RESTORE TABLE my_catalog.my_schema.sales TO VERSION AS OF 10;
-- RESTORE TABLE my_catalog.my_schema.sales TO TIMESTAMP AS OF '2025-01-01 00:00:00';

-- Cloning for dev/test
CREATE OR REPLACE TABLE my_catalog.my_schema.sales_dev SHALLOW CLONE my_catalog.my_schema.sales;
-- Deep clone makes a full physical copy at the clone time
CREATE OR REPLACE TABLE my_catalog.my_schema.sales_backup DEEP CLONE my_catalog.my_schema.sales;


-- ============================================================================
-- 6) Change Data Feed (CDF)
-- ============================================================================
-- Enable CDF via table property (see §3). Query changes with SQL:
SELECT *
FROM table_changes('my_catalog.my_schema.sales', 100, 120); -- versions [start, end]

-- Read CDF in PySpark (batch)
-- from pyspark.sql.functions import col
-- df = (spark.read
--   .format("delta")
--   .option("readChangeFeed", "true")
--   .option("startingVersion", 100)
--   .option("endingVersion", 120)
--   .table("my_catalog.my_schema.sales"))
-- display(df)

-- Read CDF in Structured Streaming
-- (spark.readstream for incremental pipelines)
-- sdf = (spark.readStream
--   .format("delta")
--   .option("readChangeFeed", "true")
--   .option("startingVersion", "latest")
--   .table("my_catalog.my_schema.sales"))
-- query = (sdf.writeStream
--   .format("delta")
--   .option("checkpointLocation", "/chk/sales_cdf_consumer")
--   .toTable("my_catalog.my_schema.sales_cdc_out"))


-- ============================================================================
-- 7) Table Health, Monitoring, and Stats
-- ============================================================================
-- Quick health checks
DESCRIBE DETAIL my_catalog.my_schema.sales;     -- size, file count, path, stats
DESCRIBE HISTORY my_catalog.my_schema.sales;    -- operations log

-- Compute/refresh stats to aid optimizers (table and columns)
ANALYZE TABLE my_catalog.my_schema.sales COMPUTE STATISTICS;
ANALYZE TABLE my_catalog.my_schema.sales COMPUTE STATISTICS FOR COLUMNS id, ts, amount;

-- Identify tables with many versions (potentially long history)
-- (Adjust the filter/thresholds to your environment.)
SELECT table_name, max(version) as latest_version
FROM my_catalog.information_schema.table_changes
WHERE table_schema = 'my_schema'
GROUP BY table_name
HAVING latest_version > 500
ORDER BY latest_version DESC;

-- Manifest for external readers (Presto/Athena/Trino)
-- GENERATE symlink_format_manifest FOR TABLE my_catalog.my_schema.sales;


-- ============================================================================
-- 8) Data Cleanup & Dedup Patterns
-- ============================================================================
-- Targeted delete
DELETE FROM my_catalog.my_schema.sales WHERE dt < date_sub(current_date(), 365);

-- Null normalization (example)
UPDATE my_catalog.my_schema.sales SET country = 'UNK' WHERE country IS NULL;

-- Idempotent dedup to latest record per id (keep newest ts)
CREATE OR REPLACE TABLE my_catalog.my_schema.sales_dedup AS
WITH ranked AS (
	SELECT *,
				 row_number() OVER (PARTITION BY id ORDER BY ts DESC) AS rn
	FROM my_catalog.my_schema.sales
)
SELECT * FROM ranked WHERE rn = 1;


-- ============================================================================
-- 9) Location & Ownership Ops (Admin-lite)
-- ============================================================================
-- Move/attach table to a new location (ensure permissions set on new path)
-- ALTER TABLE my_catalog.my_schema.sales SET LOCATION 's3://new-bucket/new/path';

-- Ownership and grants (Unity Catalog)
-- ALTER TABLE my_catalog.my_schema.sales OWNER TO `data_engineering_team`;
-- GRANT SELECT, MODIFY ON TABLE my_catalog.my_schema.sales TO `analyst_role`;


-- ============================================================================
-- 10) Minimal Playbooks
-- ============================================================================
-- A) New table rollout (safe defaults)
-- 1. Create with identity, generated date, column mapping, and retention.
-- 2. Add constraints (NOT NULL, CHECK) and defaults.
-- 3. Enable CDF if downstream consumers need change capture.
-- 4. Compute stats.

-- B) Upsert pipeline (MERGE) with evolving schema
-- 1. SET spark.databricks.delta.schema.autoMerge.enabled = true (session).
-- 2. MERGE with UPDATE SET * / INSERT *.
-- 3. Add explicit DDL for intentional type changes.
-- 4. DESCRIBE HISTORY to audit the write.

-- C) Incident recovery
-- 1. Validate with time travel SELECT ... VERSION AS OF.
-- 2. RESTORE TABLE to the known-good version.
-- 3. Re-compute stats and notify stakeholders.


-- ============================================================================
-- Rules of Thumb (No-guesswork Guide)
-- ============================================================================
-- • Keep delta.logRetentionDuration >= delta.deletedFileRetentionDuration.
-- • Enable delta.columnMapping.mode = 'name' early to allow future column renames.
-- • Use NOT NULL, CHECK, DEFAULT, and GENERATED columns to encode data contracts.
-- • Prefer identity columns over client-generated surrogate keys to avoid collisions.
-- • Enable CDF only when you have consumers; otherwise keep it off to reduce log churn.
-- • After structural changes or large merges, ANALYZE TABLE to refresh stats.
-- • Use SHALLOW CLONE for fast dev copies; DEEP CLONE for point-in-time backups.
-- • Document every ALTER TABLE in version control and track via DESCRIBE HISTORY.


-- ============================================================================
-- PySpark Helper Snippets (commented for SQL file compatibility)
-- ============================================================================
-- from delta.tables import DeltaTable
-- 
-- # Table handle
-- dt = DeltaTable.forName(spark, "my_catalog.my_schema.sales")
-- 
-- # Simple upsert
-- (dt.alias("t")
--   .merge(spark.table("my_catalog.my_schema.sales_updates").alias("s"), "t.id = s.id")
--   .whenMatchedUpdateAll()
--   .whenNotMatchedInsertAll()
--   .execute())
-- 
-- # Inspect history
-- display(spark.sql("DESCRIBE HISTORY my_catalog.my_schema.sales"))
-- 
-- # Read Change Data Feed (batch)
-- cdf = (spark.read
--   .format("delta")
--   .option("readChangeFeed", "true")
--   .option("startingVersion", 100)
--   .table("my_catalog.my_schema.sales"))
-- display(cdf)

