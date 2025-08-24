# Databricks Structured Streaming Best Practices (PySpark)
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# 1) Reliable checkpoints and exactly-once semantics
checkpoint_path = "/mnt/checkpoints/my_app/topic_x"
output_table = "main.analytics.topic_x_bronze"

schema = "id STRING, event_ts TIMESTAMP, payload STRING"

stream_df = (
    spark.readStream
        .format("cloudFiles")                       # or "kafka"
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/topic_x")
        .schema(schema)
        .load("/mnt/raw/topic_x/")
        .withColumn("ingest_ts", F.current_timestamp())
)

(
    stream_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)                  # or processingTime="1 minute"
        .outputMode("append")
        .toTable(output_table)
)

# 2) Watermarks + event-time windows to bound state
clicks = spark.readStream.table(output_table)

agg = (
    clicks
      .withWatermark("event_ts", "2 hours")
      .groupBy(F.window("event_ts", "10 minutes"))
      .agg(F.count("*").alias("events"))
)

(
    agg.writeStream
      .format("delta")
      .option("checkpointLocation", "/mnt/checkpoints/my_app/topic_x_agg")
      .outputMode("append")
      .trigger(availableNow=True)
      .toTable("main.analytics.topic_x_agg")
)

# 3) foreachBatch for upserts (idempotent)

def upsert(batch_df, batch_id: int):
    batch_df.createOrReplaceTempView("updates")
    batch_df.sparkSession.sql(
        """
        MERGE INTO main.analytics.topic_x_silver AS t
        USING updates AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

(
    clicks.writeStream
      .foreachBatch(upsert)
      .option("checkpointLocation", "/mnt/checkpoints/my_app/topic_x_upsert")
      .trigger(availableNow=True)
      .start()
)

# Tips:
# - Keep one checkpoint location per sink.
# - Use availableNow for micro-batch catch-ups.
# - Always watermark when aggregating to avoid unbounded state.
