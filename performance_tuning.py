# Databricks Performance Tuning: Spark Configuration & Optimization

from pyspark.sql import functions as F
from pyspark.sql.types import *

# --- 1. Essential Spark Configurations ---
# Set these in cluster configuration or at notebook/job level.

# Adaptive Query Execution (AQE) - Enable for automatic optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Dynamic partition pruning for better join performance
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Broadcast joins threshold (increase for large dimension tables)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200MB")

# Shuffle partitions (rule: 200 partitions per TB of data)
# Default 200 is too low for big data. Adjust based on data size.
spark.conf.set("spark.sql.shuffle.partitions", "2000")  # For ~10TB dataset

# Memory optimization
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # Pandas UDF optimization
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")

# --- 2. Partitioning Strategies ---
# Proper partitioning is crucial for query performance.

# Write with optimal partitioning (by commonly filtered columns)
df.write \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .partitionBy("year", "month") \
  .option("maxRecordsPerFile", 1000000) \
  .saveAsTable("my_catalog.my_schema.partitioned_table")

# Repartition before expensive operations
df_optimized = df.repartition(200, "user_id")  # Even distribution by key

# Coalesce to reduce small files (use after filtering)
df_coalesced = df.filter(F.col("active") == True).coalesce(50)

# --- 3. Caching Strategies ---
# Cache DataFrames that are reused multiple times.

# Cache for multiple operations on same DataFrame
df_cached = df.cache()
df_cached.count()  # Materialize cache

# Persist with custom storage level for memory-constrained environments
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Unpersist when done to free memory
df_cached.unpersist()

# --- 4. Join Optimizations ---

# Broadcast small dimension tables (< 200MB)
from pyspark.sql.functions import broadcast

large_df = spark.table("fact_table")
small_df = spark.table("dim_table")

# Force broadcast join
result = large_df.join(broadcast(small_df), "join_key")

# Sort-merge join hint for large-to-large joins
result = large_df.join(
    small_df.hint("SHUFFLE_MERGE"), 
    "join_key"
)

# Bucketed joins (pre-partition both tables by join key)
# Define during table creation for optimal join performance
df.write \
  .bucketBy(10, "user_id") \
  .sortBy("timestamp") \
  .saveAsTable("bucketed_table")

# --- 5. Data Skew Handling ---
# Detect and handle data skew that causes stragglers.

# Detect skew in your data
df.groupBy("skewed_column").count().orderBy(F.desc("count")).show(20)

# Salt technique for skewed joins
import random

def add_salt(df, column, salt_range=100):
    return df.withColumn(
        f"salted_{column}", 
        F.concat(F.col(column), F.lit("_"), F.lit(random.randint(0, salt_range-1)))
    )

# Apply salting to both DataFrames before join
salted_df1 = add_salt(df1, "skewed_key")
salted_df2 = add_salt(df2, "skewed_key") 

# --- 6. I/O Optimizations ---

# Delta table optimizations
# Enable auto-compaction for writes
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Optimize Delta write throughput
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# Parquet optimizations
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")  # Fast compression
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")  # Skip schema merging

# --- 7. Memory and Garbage Collection Tuning ---

# For memory-intensive workloads (ML, complex aggregations)
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryFraction", "0.8")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")

# G1GC for large heaps (>6GB)
spark.conf.set("spark.executor.extraJavaOptions", 
               "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1PrintRegionSummary")

# --- 8. Monitoring and Debugging ---

# Enable SQL metrics for detailed query analysis
spark.conf.set("spark.sql.adaptive.logLevel", "INFO")

# Monitor long-running queries
def analyze_query_plan(df):
    """Print physical plan to identify bottlenecks"""
    print("== Physical Plan ==")
    df.explain("formatted")
    print("== Execution Stats ==")
    print(f"Partitions: {df.rdd.getNumPartitions()}")

# Example usage
analyze_query_plan(df.groupBy("category").agg(F.sum("amount")))

# Check for data skew in partitions
def check_partition_skew(df):
    partition_sizes = df.glom().map(len).collect()
    print(f"Partition sizes: {partition_sizes}")
    print(f"Max/Min ratio: {max(partition_sizes) / min(partition_sizes):.2f}")

# --- 9. Cluster Auto-scaling Configuration ---
# Set in cluster configuration UI or via API

cluster_config = {
    "autoscale": {
        "min_workers": 2,
        "max_workers": 20
    },
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.databricks.cluster.profile": "serverless",
        "spark.databricks.passthrough.enabled": "true"
    }
}

# --- 10. Performance Testing Template ---

import time

def benchmark_query(query_func, description, iterations=3):
    """Benchmark query performance"""
    print(f"\n🚀 Testing: {description}")
    times = []
    
    for i in range(iterations):
        start_time = time.time()
        result_count = query_func().count()  # Force materialization
        end_time = time.time()
        duration = end_time - start_time
        times.append(duration)
        print(f"  Run {i+1}: {duration:.2f}s ({result_count:,} rows)")
    
    avg_time = sum(times) / len(times)
    print(f"  ⭐ Average: {avg_time:.2f}s")
    return avg_time

# Example usage
def test_cached_vs_uncached():
    df = spark.table("large_table")
    
    # Test without cache
    benchmark_query(
        lambda: df.filter(F.col("status") == "active"), 
        "Without Cache"
    )
    
    # Test with cache
    df_cached = df.cache()
    df_cached.count()  # Materialize
    
    benchmark_query(
        lambda: df_cached.filter(F.col("status") == "active"),
        "With Cache"
    )
    
    df_cached.unpersist()

# Pro Tips: 
# 1. Always profile before optimizing - use Spark UI extensively
# 2. Start with AQE enabled - it auto-tunes many parameters  
# 3. Monitor cluster utilization - scale up cores before memory
# 4. Use Delta Lake's built-in optimizations when possible

# More performance guides at jakublasak.com