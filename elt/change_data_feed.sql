-- 🚀 Automate Downstream CDF Propagation in Databricks
-- Use this MERGE to incrementally update silver tables from bronze CDF changes.
-- Tracks versions dynamically via a metadata table for easy automation.

-- Fetch dynamic version range (assumes metadata_table exists)
DECLARE @start_version BIGINT = (SELECT COALESCE(MAX(processed_version), 0) + 1 
                                 FROM metadata_table 
                                 WHERE table_name = 'my_bronze_table');

DECLARE @end_version BIGINT = (SELECT MAX(_commit_version) 
                               FROM table_changes('my_bronze_table',
                               @start_version));       

-- MERGE changes into silver table
MERGE INTO my_silver_table AS target
USING (
  SELECT * 
  FROM table_changes('my_bronze_table', @start_version, @end_version)
  -- Get deltas
  WHERE _change_type IN ('insert', 'update_postimage', 'delete') -- Filter ops
) AS changes
ON target.id = changes.id
WHEN MATCHED AND changes._change_type = 'update_postimage' THEN
  UPDATE SET *
WHEN MATCHED AND changes._change_type = 'delete' THEN
  DELETE
WHEN NOT MATCHED AND changes._change_type = 'insert' THEN
  INSERT *;

-- Update metadata with latest processed version
MERGE INTO metadata_table AS meta
USING (SELECT 'my_bronze_table' AS table_name, @end_version AS processed_version) AS src
ON meta.table_name = src.table_name
WHEN MATCHED THEN UPDATE SET processed_version = src.processed_version
WHEN NOT MATCHED THEN INSERT (table_name, processed_version) VALUES (src.table_name, src.processed_version);

-- Pro Tip: Run this in a scheduled job for incremental ETL. 
