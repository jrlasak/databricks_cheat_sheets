-- 🌊 SQL Streaming Tables & APPLY CHANGES INTO
-- Modern SQL-based streaming patterns in Databricks Runtime

-- 1) Basic streaming table with Auto Loader
CREATE OR REFRESH STREAMING TABLE bronze_events (
  id STRING,
  event_ts TIMESTAMP,
  user_id STRING,
  event_type STRING,
  payload STRING,
  _rescued_data STRING
)
AS SELECT *
FROM cloud_files(
  "/mnt/raw/events/",
  "json",
  map(
    "cloudFiles.schemaLocation", "/mnt/schemas/events",
    "cloudFiles.rescuedDataColumn", "_rescued_data"
  )
);

-- 2) Transform with streaming table
CREATE OR REFRESH STREAMING TABLE silver_events (
  id STRING NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  user_id STRING NOT NULL,
  event_type STRING,
  amount DOUBLE,
  country STRING,
  processed_ts TIMESTAMP
)
AS SELECT
  id,
  event_ts,
  user_id,
  event_type,
  CAST(get_json_object(payload, '$.amount') AS DOUBLE) as amount,
  get_json_object(payload, '$.country') as country,
  current_timestamp() as processed_ts
FROM STREAM(bronze_events)
WHERE _rescued_data IS NULL  -- Filter out malformed records
  AND event_ts IS NOT NULL;

-- 3) APPLY CHANGES INTO for CDC patterns (SCD Type 1)
CREATE OR REFRESH STREAMING TABLE customer_dim (
  customer_id STRING,
  name STRING,
  email STRING,
  status STRING,
  updated_ts TIMESTAMP,
  __metadata STRUCT<operation_type: STRING, source_ts: TIMESTAMP>
);

-- Apply CDC changes from upstream
APPLY CHANGES INTO customer_dim
FROM STREAM(customer_changes)
KEYS (customer_id)
APPLY AS DELETE WHEN operation_type = "DELETE"
APPLY AS TRUNCATE WHEN operation_type = "TRUNCATE"
SEQUENCE BY source_ts
COLUMNS * EXCEPT (operation_type, source_ts);

-- 4) APPLY CHANGES INTO for SCD Type 2
CREATE OR REFRESH STREAMING TABLE customer_history (
  customer_id STRING,
  name STRING,
  email STRING,
  status STRING,
  effective_start TIMESTAMP,
  effective_end TIMESTAMP,
  is_current BOOLEAN,
  __metadata STRUCT<operation_type: STRING, source_ts: TIMESTAMP>
);

-- SCD2 pattern with history tracking
APPLY CHANGES INTO customer_history
FROM STREAM(customer_changes)
KEYS (customer_id)
APPLY AS DELETE WHEN operation_type = "DELETE"
SEQUENCE BY source_ts
STORED AS SCD TYPE 2
TRACK HISTORY ON name, email, status;

-- 5) Aggregation streaming table
CREATE OR REFRESH STREAMING TABLE hourly_metrics (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  event_type STRING,
  country STRING,
  event_count BIGINT,
  total_amount DOUBLE,
  unique_users BIGINT
)
AS SELECT
  window_start,
  window_end,
  event_type,
  country,
  COUNT(*) as event_count,
  SUM(amount) as total_amount,
  COUNT(DISTINCT user_id) as unique_users
FROM STREAM(silver_events)
GROUP BY
  window(event_ts, "1 hour"),
  event_type,
  country;

-- 6) Late data handling with watermarks
CREATE OR REFRESH STREAMING TABLE events_with_watermark (
  id STRING,
  event_ts TIMESTAMP,
  user_id STRING,
  processed_ts TIMESTAMP
)
TBLPROPERTIES (
  "delta.autoOptimize.optimizeWrite" = "true"
)
AS SELECT *
FROM STREAM(bronze_events TIMESTAMP AS OF INTERVAL 2 HOURS);

-- 7) Error handling and monitoring
CREATE OR REFRESH STREAMING TABLE event_errors (
  id STRING,
  original_payload STRING,
  error_type STRING,
  error_message STRING,
  failed_ts TIMESTAMP
)
AS SELECT
  COALESCE(id, 'unknown') as id,
  _rescued_data as original_payload,
  'schema_validation' as error_type,
  'Unable to parse JSON payload' as error_message,
  current_timestamp() as failed_ts
FROM STREAM(bronze_events)
WHERE _rescued_data IS NOT NULL;

-- Pro Tips:
-- - Use APPLY CHANGES INTO for CDC patterns instead of complex MERGE operations
-- - Streaming tables handle checkpointing automatically
-- - Monitor with DESCRIBE HISTORY and table metrics
-- - Test incremental processing with smaller time windows first