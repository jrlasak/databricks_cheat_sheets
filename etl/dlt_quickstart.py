# Delta Live Tables (DLT) quickstart with expectations (Python)
# Create a DLT pipeline in the UI and point to this file.

import dlt
from pyspark.sql import functions as F

RAW_PATH = "/mnt/raw/events"
SCHEMA_PATH = "/mnt/schemas/dlt/events"

@dlt.table(
    name="bronze_events",
    comment="Raw JSON events ingested via Auto Loader",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
def bronze_events():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", SCHEMA_PATH)
            .load(RAW_PATH)
            .withColumn("ingest_ts", F.current_timestamp())
    )

@dlt.view(
    name="clean_events",
    comment="Light cleanup and typing"
)
def clean_events():
    return (
        dlt.read_stream("bronze_events")
          .withColumn("event_date", F.to_date("event_ts"))
          .withColumn("amount", F.col("amount").cast("double"))
    )

@dlt.table(
    name="silver_events",
    comment="Validated events",
    table_properties={"quality": "silver"},
)
@dlt.expect("non_negative_amount", "amount >= 0")
def silver_events():
    return (
        dlt.read_stream("clean_events")
    )

@dlt.table(
    name="gold_daily_revenue",
    comment="Daily aggregated revenue",
    table_properties={"quality": "gold"},
)
def gold_daily_revenue():
    return (
        dlt.read("silver_events")
          .groupBy("event_date")
          .agg(F.sum("amount").alias("revenue"))
    )

# Pipeline config (example):
# {
#   "pipelines": [{
#     "name": "events-dlt",
#     "clusters": [{"label": "default"}],
#     "development": true,
#     "channel": "CURRENT",
#     "edition": "ADVANCED",
#     "libraries": [{"notebook": {"path": "/Repos/you/databricks_cheat_sheets/dlt_quickstart.py"}}]
#   }]
# }
