# 🚀 Databricks Idempotent ETL Cheat Sheet
# Use these patterns to build fault-tolerant pipelines that prevent duplicate data.

# Pattern 1: Idempotent Batch Job using MERGE
# Use for daily/hourly loads into your Silver/Gold tables.

MERGE INTO silver_table AS target
USING (SELECT * FROM bronze_table_updates) AS source
ON target.unique_business_key = source.unique_business_key
WHEN MATCHED THEN
  UPDATE SET
    target.col1 = source.col1,
    target.col2 = source.col2,
    target.last_updated_ts = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (unique_business_key, col1, col2, created_ts, last_updated_ts)
  VALUES (source.unique_business_key, source.col1, source.col2, current_timestamp(), current_timestamp());

# ====================================================================
# Pattern 2: Idempotent Streaming Job using foreachBatch

def upsert_to_delta(micro_batch_df, batch_id):
  target_table = "silver_streaming_table"
  
  # Perform the MERGE operation on the micro-batch
  micro_batch_df.createOrReplaceTempView("source_updates")
  
  micro_batch_df.sparkSession.sql(f"""
    MERGE INTO {target_table} AS target
    USING source_updates AS source
    ON target.unique_event_id = source.unique_event_id
    WHEN MATCHED THEN
      UPDATE SET
        target.payload = source.payload,
        target.event_timestamp = source.event_timestamp
    WHEN NOT MATCHED THEN
      INSERT (unique_event_id, payload, event_timestamp)
      VALUES (source.unique_event_id, source.payload, source.event_timestamp)
  """)

# Define the stream
streaming_df = (
  spark.readStream
    .format("delta")
    .table("bronze_streaming_source")
)

# Write the stream using foreachBatch and a checkpoint
(
  streaming_df.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/path/to/your/checkpoint/dir")
    .trigger(availableNow=True) # Or .trigger(processingTime='1 minute')
    .start()
)
# linkedin.com/in/jrlasak/