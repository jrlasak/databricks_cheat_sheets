-- 🧩 APPLY CHANGES (AUTO CDC) Cheatsheet for SCD1 and SCD2 in Lakeflow Declarative Pipelines
-- Note: APPLY CHANGES has been replaced by AUTO CDC with the same SQL syntax. Prefer AUTO CDC.

-- What it does
-- Reads CDC events from a streaming source and applies them to a streaming target table, handling
-- out-of-order events via SEQUENCE BY. Supports SCD Type 1 (overwrite) and SCD Type 2 (history).

-- Key clauses
-- KEYS (...): business/natural key columns
-- SEQUENCE BY <col or STRUCT(...)>: strictly increasing logical order (no NULLs)
-- APPLY AS DELETE WHEN <cond>: treat matching events as deletes
-- APPLY AS TRUNCATE WHEN <cond>: full truncate (SCD1 only)
-- COLUMNS * EXCEPT (...): exclude technical columns from target
-- STORED AS SCD TYPE {1|2}
-- TRACK HISTORY ON {col list | * EXCEPT (...)}: SCD2 partial-history tracking
-- IGNORE NULL UPDATES: ignore NULLs in updates and keep existing values

-- Target table creation
-- Always declare a streaming target table first. You can omit an explicit schema and let it evolve
-- from the source columns included via COLUMNS. For SCD2 with an explicit schema, include
-- __START_AT and __END_AT with the same type as your SEQUENCE BY column.

-- Example: explicit SCD2 target with BIGINT sequencing
-- CREATE OR REFRESH STREAMING TABLE dim_customer_scd2 (
--   userId BIGINT,
--   name STRING,
--   city STRING,
--   __START_AT BIGINT,
--   __END_AT BIGINT
-- );

-- 1) SCD TYPE 1 from CDF
-- Overwrites current values, no history kept.
CREATE OR REFRESH STREAMING TABLE dim_customer_scd1;

CREATE FLOW dim_customer_scd1_flow AS AUTO CDC INTO
	dim_customer_scd1
FROM
	stream(cdc_data.users)                      -- source must be streaming
KEYS
	(userId)                                    -- natural/business key
APPLY AS DELETE WHEN
	operation = "DELETE"                        -- map DELETE ops
APPLY AS TRUNCATE WHEN
	operation = "TRUNCATE"                      -- optional, SCD1 only
SEQUENCE BY
	sequenceNum                                 -- strictly increasing; no NULLs
COLUMNS * EXCEPT
	(operation, sequenceNum)                    -- exclude technical cols
STORED AS
	SCD TYPE 1;                                 -- default is 1, set explicitly for clarity

-- Optional: allow partial updates without nulling unspecified fields
-- CREATE FLOW dim_customer_scd1_partial AS AUTO CDC INTO dim_customer_scd1 FROM stream(cdc_data.users)
-- KEYS (userId) IGNORE NULL UPDATES SEQUENCE BY sequenceNum COLUMNS * EXCEPT (operation, sequenceNum) STORED AS SCD TYPE 1;


-- 2) SCD TYPE 2 from CDF
-- Retains history, Lakeflow manages __START_AT and __END_AT based on SEQUENCE BY.
CREATE OR REFRESH STREAMING TABLE dim_customer_scd2;

CREATE FLOW dim_customer_scd2_flow AS AUTO CDC INTO
	dim_customer_scd2
FROM
	stream(cdc_data.users)
KEYS
	(userId)
APPLY AS DELETE WHEN
	operation = "DELETE"
SEQUENCE BY
	sequenceNum
COLUMNS * EXCEPT
	(operation, sequenceNum)
STORED AS
	SCD TYPE 2;                                 -- creates/maintains __START_AT, __END_AT

-- 2a) SCD2 with partial history tracking (only track changes for selected columns)
-- Changes to untracked columns are updated in place without a new history row.
-- CREATE FLOW dim_customer_scd2_track_subset AS AUTO CDC INTO dim_customer_scd2 FROM stream(cdc_data.users)
-- KEYS (userId) APPLY AS DELETE WHEN operation = "DELETE"
-- SEQUENCE BY sequenceNum COLUMNS * EXCEPT (operation, sequenceNum)
-- STORED AS SCD TYPE 2 TRACK HISTORY ON * EXCEPT (city);


-- 3) Multi-column sequencing to break ties (timestamp + id)
-- CREATE FLOW flow_seq_struct AS AUTO CDC INTO dim_customer_scd2 FROM stream(cdc_data.users)
-- KEYS (userId) SEQUENCE BY STRUCT(event_ts, event_id)
-- COLUMNS * EXCEPT (operation, event_ts, event_id) STORED AS SCD TYPE 2;


-- 4) DML on target streaming tables (Unity Catalog only, DBR 13.3LTS+)
-- Ensure __START_AT/__END_AT are set correctly for ordering.
-- INSERT INTO my_catalog.my_schema.dim_customer_scd2
--   (userId, name, city, __START_AT, __END_AT) VALUES (123, 'John', 'Oaxaca', 5, NULL);


-- Tips & gotchas
-- - Prefer AUTO CDC over MERGE for CDC to handle out-of-order events correctly.
-- - Exactly one distinct update per key per SEQUENCE value; SEQUENCE cannot be NULL.
-- - If the source is a streaming table that itself changes (DML), set skipChangeCommits when reading it (Python API).
-- - APPLY AS TRUNCATE WHEN is only supported for SCD TYPE 1.
-- - For explicit SCD2 schemas, match __START_AT/__END_AT data types to SEQUENCE BY.
-- - KEYS must uniquely identify rows; otherwise results are undefined.

-- Quick peek: latest snapshot
-- SELECT * FROM dim_customer_scd1;
-- SELECT * FROM dim_customer_scd2;           -- SCD2 view hides tombstones automatically

-- Reading CDF from the SCD target (UC, DBR 15.2+): same as any Delta table CDF.
-- SELECT * FROM table_changes('my_catalog.my_schema.dim_customer_scd2', TIMESTAMP '2024-01-01');

-- Docs:
-- - AUTO CDC SQL reference: https://docs.databricks.com/dlt-ref/dlt-sql-ref-apply-changes-into.html
-- - CDC overview and examples: https://docs.databricks.com/dlt/cdc.html
