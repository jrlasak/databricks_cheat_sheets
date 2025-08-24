# Databricks Autoloader: Production-Ready Ingestion Pipeline

# --- 1. Define Paths ---
# Use descriptive variables for maintainability.
source_data_path = "/path/to/raw/landing/zone/"
bronze_table_path = "/path/to/delta/bronze_table/"
schema_location = "/path/to/autoloader/schema_location/"
checkpoint_location = "/path/to/streaming/checkpoint/"

# --- 2. Configure the Autoloader Stream ---
# Set up the readStream with all the necessary production options.
raw_stream_df = (
    spark.readStream
    .format("cloudFiles")

    # Specify the format of the source files
    .option("cloudFiles.format", "json")

    # Enable schema inference and evolution
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") 
    # Options: addNewColumns, rescue, fail

    # Capture malformed data instead of failing the stream
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")

    # Use file notifications for better scalability (requires setup)
    .option("cloudFiles.useNotifications", "true") 
    # Set to false for simple directory listing

    # Load the data from the source directory
    .load(source_data_path)
)

# --- 3. Write the Stream to a Delta Table ---
# Configure the writeStream to be fault-tolerant and efficient.
(
    raw_stream_df.writeStream
    .format("delta")

    # Set the checkpoint location for fault tolerance
    .option("checkpointLocation", checkpoint_location)

    # Use Trigger.AvailableNow for batch-like execution 
    # that processes all available data
    .trigger(availableNow=True) 
    # Or .trigger(processingTime='5 minutes') for continuous streams

    # Merge schema changes from the stream into the target table
    .option("mergeSchema", "true")

    # Start the stream and write to the Delta table path
    .toTable("my_catalog.my_schema.bronze_table") 
    # Or .start(bronze_table_path)
)

# Pro Tip: Schedule this notebook as a Databricks Job to run incrementally.
# More info on dataengineer.wiki