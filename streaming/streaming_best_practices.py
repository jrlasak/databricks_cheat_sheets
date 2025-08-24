# 🌊 Databricks Structured Streaming: Production Best Practices
# Complete guide to building robust streaming applications in Databricks

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import *
from typing import Optional

spark = SparkSession.builder.getOrCreate()

# =============================================================================
# 1. Reliable Checkpointing & Exactly-Once Semantics
# =============================================================================

def create_reliable_stream(source_path: str, 
                          target_table: str,
                          checkpoint_location: str,
                          schema: Optional[StructType] = None) -> StreamingQuery:
    """
    Create a production-ready streaming pipeline with proper checkpointing
    """
    # Auto Loader configuration for file-based sources
    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_location}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.maxFilesPerTrigger", 1000)    # Control batch size
        .load(source_path)
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    
    # Write with fault-tolerant configuration
    query = (
        stream_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)                        # Batch-like processing
        .outputMode("append")
        .option("mergeSchema", "true")                     # Handle schema evolution
        .toTable(target_table)
    )
    
    print(f"✅ Streaming query started: {target_table}")
    return query

# =============================================================================
# 2. Windowing & Watermarking for Aggregations
# =============================================================================

def create_windowed_aggregation(source_table: str,
                               target_table: str,
                               window_duration: str = "10 minutes",
                               watermark_duration: str = "2 hours") -> StreamingQuery:
    """
    Create windowed aggregations with proper watermarking
    """
    checkpoint_path = f"/mnt/checkpoints/{target_table.replace('.', '_')}_agg"
    
    # Read from source table
    events_stream = spark.readStream.table(source_table)
    
    # Apply watermark to handle late data
    windowed_agg = (
        events_stream
        .withWatermark("event_timestamp", watermark_duration)
        .groupBy(
            F.window("event_timestamp", window_duration),
            "event_type",
            "country"
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum("amount").alias("total_amount"),
            F.countDistinct("user_id").alias("unique_users"),
            F.avg("amount").alias("avg_amount"),
            F.max("event_timestamp").alias("latest_event")
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )
    
    # Write aggregated results
    query = (
        windowed_agg.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="2 minutes")
        .outputMode("append")                              # Only complete windows
        .toTable(target_table)
    )
    
    return query

# =============================================================================
# 3. foreachBatch for Complex Processing & Upserts
# =============================================================================

def upsert_to_target(batch_df, batch_id: int):
    """
    Idempotent upsert operation using MERGE
    """
    if batch_df.count() == 0:
        print(f"Batch {batch_id}: No data to process")
        return
    
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    
    # Create temporary view for MERGE operation
    batch_df.createOrReplaceTempView("batch_updates")
    
    # Execute MERGE for idempotent upserts
    spark.sql(f"""
        MERGE INTO main.silver.customer_profile AS target
        USING batch_updates AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN 
            UPDATE SET 
                target.name = source.name,
                target.email = source.email,
                target.last_updated = source.event_timestamp
        WHEN NOT MATCHED THEN 
            INSERT (customer_id, name, email, created_at, last_updated)
            VALUES (source.customer_id, source.name, source.email, 
                   source.event_timestamp, source.event_timestamp)
    """)
    
    print(f"Batch {batch_id} processed successfully")

def create_upsert_stream(source_table: str) -> StreamingQuery:
    """
    Create streaming upsert pipeline using foreachBatch
    """
    checkpoint_path = "/mnt/checkpoints/customer_profile_upsert"
    
    customer_updates = (
        spark.readStream
        .table(source_table)
        .filter(F.col("event_type") == "profile_update")
        .select(
            "customer_id",
            "name", 
            "email",
            "event_timestamp"
        )
    )
    
    query = (
        customer_updates.writeStream
        .foreachBatch(upsert_to_target)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    return query

# =============================================================================
# 4. Multi-Sink Streaming (Fan-out Pattern)
# =============================================================================

def create_multi_sink_stream(source_table: str) -> list[StreamingQuery]:
    """
    Fan out streaming data to multiple sinks with different processing logic
    """
    source_stream = spark.readStream.table(source_table)
    queries = []
    
    # Sink 1: Real-time alerts (high-priority events)
    alerts_stream = source_stream.filter(F.col("priority") == "high")
    
    alerts_query = (
        alerts_stream.writeStream
        .format("delta")
        .option("checkpointLocation", "/mnt/checkpoints/alerts")
        .trigger(processingTime="10 seconds")              # Low latency
        .outputMode("append")
        .toTable("main.alerts.high_priority_events")
    )
    queries.append(alerts_query)
    
    # Sink 2: Batch analytics (all events, less frequent)
    analytics_query = (
        source_stream.writeStream
        .format("delta") 
        .option("checkpointLocation", "/mnt/checkpoints/analytics")
        .trigger(processingTime="5 minutes")               # Higher latency, larger batches
        .outputMode("append")
        .toTable("main.analytics.all_events")
    )
    queries.append(analytics_query)
    
    # Sink 3: Kafka output for downstream systems
    kafka_query = (
        source_stream.select(
            F.col("event_id").cast("string").alias("key"),
            F.to_json(F.struct("*")).alias("value")
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka-broker:9092")
        .option("topic", "processed_events")
        .option("checkpointLocation", "/mnt/checkpoints/kafka_sink")
        .trigger(processingTime="1 minute")
        .start()
    )
    queries.append(kafka_query)
    
    return queries

# =============================================================================
# 5. Stream Monitoring & Health Checks
# =============================================================================

def monitor_streaming_query(query: StreamingQuery, query_name: str) -> None:
    """
    Monitor streaming query health and performance
    """
    try:
        progress = query.lastProgress
        if not progress:
            print(f"⚠️  No progress available for {query_name}")
            return
            
        print(f"📊 Stream Health Report: {query_name}")
        print("-" * 40)
        print(f"Status: {query.status}")
        print(f"Batch ID: {progress.get('batchId', 'N/A')}")
        print(f"Input Rows/sec: {progress.get('inputRowsPerSecond', 0):.2f}")
        print(f"Processing Rate: {progress.get('processingTimerInMs', {}).get('getNextBatch', 0)}ms")
        
        # Check for performance issues
        trigger_time = progress.get('durationMs', {}).get('triggerExecution', 0)
        if trigger_time > 60000:  # > 1 minute
            print(f"⚠️  WARNING: High processing time ({trigger_time/1000:.1f}s)")
        
        # Check for data delays
        watermark = progress.get('eventTime', {}).get('watermark')
        if watermark:
            print(f"Watermark: {watermark}")
        
    except Exception as e:
        print(f"❌ Error monitoring {query_name}: {str(e)}")

def create_stream_health_dashboard(queries: list[StreamingQuery]) -> None:
    """
    Create comprehensive health dashboard for multiple streams
    """
    print("🏥 STREAMING HEALTH DASHBOARD")
    print("=" * 60)
    
    for i, query in enumerate(queries):
        query_name = f"Query_{i+1}" if not hasattr(query, 'name') else query.name
        monitor_streaming_query(query, query_name)
        print()

# =============================================================================
# 6. Error Handling & Recovery Patterns
# =============================================================================

def create_error_handling_stream(source_path: str, 
                                target_table: str,
                                error_table: str) -> StreamingQuery:
    """
    Streaming pipeline with comprehensive error handling
    """
    def process_batch_with_error_handling(batch_df, batch_id: int):
        try:
            print(f"Processing batch {batch_id}")
            
            # Validate data quality
            valid_records = batch_df.filter(
                F.col("event_id").isNotNull() & 
                F.col("event_timestamp").isNotNull()
            )
            
            invalid_records = batch_df.subtract(valid_records).withColumn(
                "error_reason", F.lit("Missing required fields")
            ).withColumn("batch_id", F.lit(batch_id))
            
            # Write valid records to main table
            if valid_records.count() > 0:
                valid_records.write.mode("append").saveAsTable(target_table)
            
            # Write invalid records to error table
            if invalid_records.count() > 0:
                invalid_records.write.mode("append").saveAsTable(error_table)
                print(f"⚠️  {invalid_records.count()} invalid records written to error table")
                
            print(f"✅ Batch {batch_id} completed successfully")
            
        except Exception as e:
            print(f"❌ Error processing batch {batch_id}: {str(e)}")
            # Log error details to monitoring system
            error_df = batch_df.withColumn("error_message", F.lit(str(e)))
            error_df.write.mode("append").saveAsTable(error_table)
    
    source_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(source_path)
    )
    
    query = (
        source_stream.writeStream
        .foreachBatch(process_batch_with_error_handling)
        .option("checkpointLocation", f"/mnt/checkpoints/{target_table.replace('.', '_')}")
        .trigger(processingTime="1 minute")
        .start()
    )
    
    return query

# =============================================================================
# 7. Production Deployment Examples
# =============================================================================

# Example 1: Simple file-to-table streaming
# bronze_query = create_reliable_stream(
#     source_path="/mnt/raw/events/",
#     target_table="main.bronze.events",
#     checkpoint_location="/mnt/checkpoints/bronze_events"
# )

# Example 2: Real-time aggregations
# agg_query = create_windowed_aggregation(
#     source_table="main.bronze.events",
#     target_table="main.silver.event_metrics",
#     window_duration="5 minutes",
#     watermark_duration="1 hour"
# )

# Example 3: Complex upserts
# upsert_query = create_upsert_stream("main.bronze.customer_events")

# Example 4: Multi-sink fan-out
# fan_out_queries = create_multi_sink_stream("main.silver.processed_events")

print("🌊 Streaming best practices loaded!")
print("💡 Production Tips:")
print("   • Always use checkpointing for fault tolerance")
print("   • Set appropriate watermarks for late data handling")
print("   • Monitor stream health continuously")
print("   • Use availableNow trigger for catch-up processing")
print("   • Implement proper error handling and data validation")

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
