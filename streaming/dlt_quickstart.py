# 🚀 Delta Live Tables (DLT): Production Pipeline Quickstart
# Complete reference for building declarative data pipelines in Databricks

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# =============================================================================
# Configuration Variables (customize for your environment)
# =============================================================================

RAW_PATH = "/mnt/raw/events"
SCHEMA_PATH = "/mnt/schemas/dlt/events"
CATALOG_NAME = "main"  # Unity Catalog name
SCHEMA_NAME = "dlt_demo"  # Schema within catalog

# =============================================================================
# 1. Bronze Layer: Raw Data Ingestion with Auto Loader
# =============================================================================

@dlt.table(
    name="bronze_events",
    comment="Raw JSON events ingested via Auto Loader with schema evolution",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "event_ts IS NOT NULL")
def bronze_events():
    """
    Ingest raw events from cloud storage with Auto Loader
    Handles schema evolution and malformed records automatically
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.maxFilesPerTrigger", 1000)
        .load(RAW_PATH)
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_processing_date", F.current_date())
    )

# =============================================================================
# 2. Bronze to Silver: Data Cleaning and Transformation
# =============================================================================

@dlt.view(
    name="events_cleaned",
    comment="Cleaned events with standardized data types and basic validation"
)
def events_cleaned():
    """
    Clean and standardize bronze data before silver layer processing
    """
    return (
        dlt.read_stream("bronze_events")
        .withColumn("event_date", F.to_date(F.col("event_ts")))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("user_id", F.trim(F.col("user_id")))
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .filter(F.col("_rescued_data").isNull())  # Filter out malformed records
    )

@dlt.table(
    name="silver_events",
    comment="Validated and enriched events ready for analytics",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
@dlt.expect("non_negative_amount", "amount >= 0")
@dlt.expect("valid_event_types", "event_type IN ('purchase', 'view', 'click', 'signup')")
@dlt.expect_or_drop("future_events", "event_date <= current_date()")
@dlt.expect_all_or_drop({
    "required_fields": "user_id IS NOT NULL AND event_type IS NOT NULL",
    "valid_amounts": "amount IS NULL OR amount >= 0"
})
def silver_events():
    """
    Silver layer with comprehensive data quality expectations
    """
    return (
        dlt.read_stream("events_cleaned")
        .withColumn("is_weekend", 
                   F.dayofweek(F.col("event_date")).isin([1, 7]))
        .withColumn("hour_of_day", 
                   F.hour(F.col("event_ts")))
        .withColumn("amount_usd", 
                   F.when(F.col("currency") == "EUR", F.col("amount") * 1.1)
                   .when(F.col("currency") == "GBP", F.col("amount") * 1.25)
                   .otherwise(F.col("amount")))
    )

# =============================================================================
# 3. Silver to Gold: Business Logic and Aggregations
# =============================================================================

@dlt.table(
    name="gold_daily_metrics",
    comment="Daily business metrics aggregated from silver events",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def gold_daily_metrics():
    """
    Daily aggregated metrics for business reporting
    """
    return (
        dlt.read("silver_events")
        .groupBy("event_date", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("amount_usd").alias("total_revenue_usd"),
            F.avg("amount_usd").alias("avg_revenue_usd"),
            F.countDistinct("user_id").alias("unique_users"),
            F.sum(F.when(F.col("is_weekend"), 1).otherwise(0)).alias("weekend_events"),
            F.max("_ingest_timestamp").alias("last_processed_at")
        )
        .withColumn("revenue_per_user", 
                   F.col("total_revenue_usd") / F.col("unique_users"))
    )

@dlt.table(
    name="gold_user_metrics",
    comment="User-level aggregated metrics",
    table_properties={"quality": "gold"}
)
def gold_user_metrics():
    """
    User-level metrics for customer analytics
    """
    return (
        dlt.read("silver_events")
        .groupBy("user_id")
        .agg(
            F.min("event_date").alias("first_activity_date"),
            F.max("event_date").alias("last_activity_date"),
            F.count("*").alias("total_events"),
            F.sum("amount_usd").alias("lifetime_value"),
            F.countDistinct("event_type").alias("unique_event_types"),
            F.avg("amount_usd").alias("avg_transaction_value")
        )
        .withColumn("days_active", 
                   F.datediff("last_activity_date", "first_activity_date") + 1)
        .withColumn("user_segment",
                   F.when(F.col("lifetime_value") >= 1000, "high_value")
                   .when(F.col("lifetime_value") >= 100, "medium_value")
                   .otherwise("low_value"))
    )

# =============================================================================
# 4. Data Quality Monitoring Tables
# =============================================================================

@dlt.table(
    name="data_quality_metrics",
    comment="Data quality monitoring for pipeline health"
)
def data_quality_metrics():
    """
    Track data quality metrics across the pipeline
    """
    return (
        dlt.read("bronze_events")
        .groupBy(F.window("_ingest_timestamp", "1 hour"))
        .agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("malformed_records"),
            F.countDistinct("_source_file").alias("files_processed"),
            F.max("_ingest_timestamp").alias("batch_timestamp")
        )
        .withColumn("data_quality_score", 
                   (F.col("total_records") - F.col("malformed_records")) / F.col("total_records") * 100)
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )

# =============================================================================
# 5. Advanced DLT Patterns
# =============================================================================

# Change Data Capture (CDC) example
@dlt.table(
    name="silver_customer_profile",
    comment="Customer profiles with SCD Type 1 updates"
)
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
def silver_customer_profile():
    """
    Apply CDC changes to customer profiles
    """
    return (
        dlt.read_stream("bronze_events")
        .filter(F.col("event_type") == "profile_update")
        .select(
            "customer_id",
            "name",
            "email", 
            "address",
            F.col("event_ts").alias("last_updated")
        )
    )

# Streaming table with APPLY CHANGES INTO (CDC)
dlt.create_streaming_table("customer_changes")

dlt.apply_changes(
    target="customer_changes",
    source="silver_customer_profile",
    keys=["customer_id"],
    sequence_by=F.col("last_updated"),
    apply_as_deletes=None,
    except_column_list=[],
    stored_as_scd_type=1
)

# =============================================================================
# 6. Pipeline Configuration Templates
# =============================================================================

# JSON Configuration for DLT Pipeline (paste into Databricks UI)
PIPELINE_CONFIG = """
{
  "id": "your-pipeline-id",
  "name": "events-dlt-pipeline",
  "storage": "/mnt/dlt/storage",
  "configuration": {
    "catalog": "main",
    "schema": "dlt_demo"
  },
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "min_workers": 1,
        "max_workers": 5
      }
    }
  ],
  "libraries": [
    {
      "notebook": {
        "path": "/Repos/your-org/databricks_cheat_sheets/streaming/dlt_quickstart"
      }
    }
  ],
  "filters": {
    "include": ["*"],
    "exclude": []
  },
  "development": false,
  "continuous": false,
  "channel": "CURRENT",
  "edition": "ADVANCED",
  "photon": false,
  "serverless": false
}
"""

# =============================================================================
# 7. Monitoring and Observability
# =============================================================================

# Monitor pipeline events (run in separate notebook)
def monitor_dlt_pipeline():
    """
    Query DLT system tables for pipeline monitoring
    """
    # Pipeline execution history
    pipeline_events = spark.sql("""
        SELECT 
            timestamp,
            message,
            level,
            details
        FROM event_log('{pipeline_id}')
        WHERE timestamp >= current_timestamp() - INTERVAL 1 DAY
        ORDER BY timestamp DESC
    """)
    
    # Data quality expectations
    quality_metrics = spark.sql("""
        SELECT 
            dataset,
            constraint,
            passed_records,
            failed_records
        FROM expectations('{pipeline_id}')
        WHERE timestamp >= current_timestamp() - INTERVAL 1 DAY
    """)
    
    return pipeline_events, quality_metrics

print("🚀 DLT Pipeline quickstart loaded!")
print("📋 Next Steps:")
print("   1. Create a new DLT pipeline in Databricks UI")
print("   2. Configure source paths and Unity Catalog settings")
print("   3. Add this notebook as a library")
print("   4. Set up appropriate cluster configuration")
print("   5. Run in development mode first, then production")
print("\n💡 Pro Tips:")
print("   • Use expectations for data quality enforcement")
print("   • Monitor pipeline health with system tables")
print("   • Start with development mode for testing")
print("   • Use continuous mode for real-time processing")
