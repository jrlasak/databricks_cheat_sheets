-- 🧩 Slowly Changing Dimension Type 2 (SCD2) with Delta MERGE
-- Assumes a natural key (business_key) and change detection via hash.

-- Target table schema (example)
-- business_key STRING,
-- attr1 STRING,
-- attr2 STRING,
-- effective_start TIMESTAMP,
-- effective_end TIMESTAMP,
-- is_current BOOLEAN,
-- record_hash STRING

-- 1) Compute source with hash for change detection
WITH source_updates AS (
  SELECT
    s.business_key,
    s.attr1,
    s.attr2,
    sha2(concat_ws('||', s.business_key, s.attr1, s.attr2), 256) AS record_hash,
    current_timestamp() AS change_ts
  FROM staging_source s
)

-- 2) Close out changed current rows
MERGE INTO dim_customer AS t
USING source_updates AS s
ON t.business_key = s.business_key AND t.is_current = true
WHEN MATCHED AND t.record_hash <> s.record_hash THEN UPDATE SET
  t.effective_end = s.change_ts,
  t.is_current = false
WHEN MATCHED AND t.record_hash = s.record_hash THEN UPDATE SET
  t.effective_end = t.effective_end -- no-op to avoid rewrite
-- When there is no current row for the business key (new key) or the current row
-- was closed above (changed attributes), insert a new current row.
WHEN NOT MATCHED THEN INSERT (
  business_key,
  attr1,
  attr2,
  effective_start,
  effective_end,
  is_current,
  record_hash
) VALUES (
  s.business_key,
  s.attr1,
  s.attr2,
  s.change_ts,
  TIMESTAMP('2999-12-31'),
  true,
  s.record_hash
)

-- Note: For streaming CDC, prefer APPLY CHANGES INTO for SCD1/2 semantics.