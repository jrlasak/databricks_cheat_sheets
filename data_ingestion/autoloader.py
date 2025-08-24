# 📥 Databricks Auto Loader: Production-Ready Data Ingestion
# Complete reference for building fault-tolerant streaming ingestion pipelines

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

# =============================================================================
# 1. Basic Auto Loader Configuration
# =============================================================================

# Define paths (customize for your environment)
source_data_path = "/mnt/raw/events/"                    # Cloud storage landing zone
schema_location = "/mnt/schemas/autoloader/events"       # Schema evolution tracking
checkpoint_location = "/mnt/checkpoints/bronze_events"   # Streaming checkpoint
target_table = "main.bronze.events"                     # Unity Catalog table

# Basic Auto Loader stream configuration
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")                 # json, parquet, csv, avro
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load(source_data_path)
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# Write to Delta table with basic configuration
query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)                          # Batch-like processing
    .toTable(target_table)
    .awaitTermination()
)

# =============================================================================
# 2. Advanced Auto Loader with Error Handling
# =============================================================================

def create_autoloader_stream(
    source_path: str,
    target_table: str,
    file_format: str = "json",
    trigger_interval: str = "5 minutes"
) -> StreamingQuery:
    """
    Create a robust Auto Loader stream with comprehensive error handling
    """
    
    # Dynamic paths based on table name
    table_parts = target_table.split(".")
    schema_path = f"/mnt/schemas/autoloader/{table_parts[-1]}"
    checkpoint_path = f"/mnt/checkpoints/{table_parts[-1]}"
    
    try:
        stream_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            
            # Performance optimizations
            .option("cloudFiles.maxFilesPerTrigger", 1000)
            .option("cloudFiles.maxBytesPerTrigger", "1g")
            
            # File notification (requires setup, improves latency)
            .option("cloudFiles.useNotifications", "false")  # Set to true if configured
            
            .load(source_path)
            .withColumn("_ingest_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_processing_date", F.current_date())
        )
        
        # Write stream with comprehensive options
        query = (
            stream_df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            
            # Trigger configuration
            .trigger(processingTime=trigger_interval)
            
            # Delta optimizations
            .option("delta.autoOptimize.optimizeWrite", "true")
            .option("delta.autoOptimize.autoCompact", "true")
            
            .outputMode("append")
            .toTable(target_table)
        )
        
        print(f"✅ Auto Loader stream started for {target_table}")
        return query
        
    except Exception as e:
        print(f"❌ Failed to create Auto Loader stream: {str(e)}")
        raise

# Example usage
# query = create_autoloader_stream(
#     source_path="/mnt/raw/events/",
#     target_table="main.bronze.events",
#     file_format="json",
#     trigger_interval="2 minutes"
# )

# =============================================================================
# 3. Multi-Format Auto Loader Examples
# =============================================================================

# JSON with nested schema
json_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/json_events")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .option("multiline", "true")                         # For multi-line JSON
    .load("/mnt/raw/json_events/")
)

# CSV with custom options
csv_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/csv_data")
    .option("header", "true")
    .option("inferSchema", "false")                      # Use schema evolution instead
    .option("delimiter", ",")
    .option("escape", '"')
    .load("/mnt/raw/csv_data/")
)

# Parquet (most efficient for structured data)
parquet_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/parquet_data")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/mnt/raw/parquet_data/")
)

# =============================================================================
# 4. Production Best Practices
# =============================================================================

def monitor_stream_health(query: StreamingQuery, table_name: str):
    """
    Monitor streaming query health and log metrics
    """
    try:
        progress = query.lastProgress
        if progress:
            print(f"📊 Stream Health for {table_name}:")
            print(f"   • Batch ID: {progress.get('batchId', 'N/A')}")
            print(f"   • Input Rows: {progress.get('inputRowsPerSecond', 0):.2f}/sec")
            print(f"   • Processing Time: {progress.get('durationMs', {}).get('triggerExecution', 0)}ms")
            
            # Check for errors or delays
            if progress.get('durationMs', {}).get('triggerExecution', 0) > 300000:  # > 5 mins
                print("⚠️  WARNING: Processing time is high, consider scaling up")
                
    except Exception as e:
        print(f"❌ Error monitoring stream: {str(e)}")

# Example monitoring usage
# monitor_stream_health(query, "bronze.events")

# =============================================================================
# 5. Common Troubleshooting Patterns
# =============================================================================

# Check for rescued data (malformed records)
def check_data_quality(table_name: str):
    """
    Analyze data quality in bronze table
    """
    df = spark.table(table_name)
    
    total_records = df.count()
    rescued_records = df.filter(F.col("_rescued_data").isNotNull()).count()
    
    print(f"📊 Data Quality Report for {table_name}:")
    print(f"   • Total Records: {total_records:,}")
    print(f"   • Rescued Records: {rescued_records:,}")
    print(f"   • Success Rate: {((total_records - rescued_records) / total_records * 100):.2f}%")
    
    if rescued_records > 0:
        print("\n🔍 Sample rescued data:")
        df.filter(F.col("_rescued_data").isNotNull()).select("_rescued_data").show(5, truncate=False)

# Schema evolution tracking
def view_schema_evolution(schema_location: str):
    """
    View schema evolution history
    """
    try:
        schema_files = dbutils.fs.ls(schema_location)
        print(f"📋 Schema Evolution Files in {schema_location}:")
        for file in schema_files:
            print(f"   • {file.name} ({file.size} bytes)")
    except Exception as e:
        print(f"❌ Error accessing schema location: {str(e)}")

# =============================================================================
# 6. Job Configuration Template (for scheduling)
# =============================================================================

# When scheduling as a Databricks Job, use these parameters:
# - Cluster: Use autoscaling (1-4 workers for most workloads)
# - Notebook parameters:
#   • source_path: /mnt/raw/events/
#   • target_table: main.bronze.events  
#   • trigger_interval: 5 minutes
#   • file_format: json

# Schedule frequency: Every 15-30 minutes for most use cases
# Timeout: 2-3 hours to handle backlog processing

print("🚀 Auto Loader cheat sheet loaded!")
print("💡 Pro Tips:")
print("   • Use availableNow trigger for catch-up processing")
print("   • Monitor rescued data regularly for schema issues") 
print("   • Enable file notifications for sub-minute latency")
print("   • Schedule OPTIMIZE on target tables weekly")