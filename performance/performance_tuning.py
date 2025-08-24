# ⚡ Databricks Performance Tuning: Complete Optimization Guide
# Production-tested patterns for maximizing Spark performance on Databricks

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import StorageLevel
import time
from typing import Callable, Dict, Any

# =============================================================================
# 1. Essential Spark Configuration (Cluster-level Settings)
# =============================================================================

def configure_spark_for_performance():
    """
    Set optimal Spark configurations for most workloads
    Apply these in cluster configuration or notebook init
    """
    
    # Adaptive Query Execution (AQE) - Must-have for modern Spark
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    
    # Dynamic partition pruning for better join performance
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    
    # Broadcast joins (increase threshold for larger dimension tables)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500MB")
    
    # Vectorized operations for better performance
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
    
    # Delta Lake optimizations
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    
    print("✅ Spark configured for optimal performance")

def configure_shuffle_partitions(data_size_gb: float) -> int:
    """
    Calculate optimal shuffle partitions based on data size
    Rule of thumb: 128-256MB per partition
    """
    target_mb_per_partition = 200
    partitions = max(200, int((data_size_gb * 1024) / target_mb_per_partition))
    
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    print(f"🔧 Set shuffle partitions to {partitions} for {data_size_gb}GB dataset")
    return partitions

# Example usage
# configure_spark_for_performance()
# configure_shuffle_partitions(100.0)  # 100GB dataset

# =============================================================================
# 2. Advanced Partitioning Strategies
# =============================================================================

def write_optimized_table(df, table_name: str, partition_columns: list = None):
    """
    Write DataFrame with production-optimized settings
    """
    writer = df.write.mode("overwrite").option("overwriteSchema", "true")
    
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    
    # Optimize file sizes (aim for 128-256MB files)
    writer = writer.option("maxRecordsPerFile", 1000000)
    
    # Enable optimized writes for Delta
    writer = (writer
              .option("delta.autoOptimize.optimizeWrite", "true")
              .option("delta.autoOptimize.autoCompact", "true"))
    
    writer.saveAsTable(table_name)
    print(f"✅ Optimized table written: {table_name}")

def smart_repartition(df, partition_count: int = None, partition_column: str = None):
    """
    Intelligently repartition DataFrame based on data characteristics
    """
    current_partitions = df.rdd.getNumPartitions()
    row_count = df.count()
    
    if not partition_count:
        # Calculate optimal partitions (aim for 100K-1M rows per partition)
        target_rows_per_partition = 500000
        partition_count = max(1, row_count // target_rows_per_partition)
    
    if partition_column:
        df_repartitioned = df.repartition(partition_count, partition_column)
        print(f"🔄 Repartitioned by {partition_column}: {current_partitions} → {partition_count} partitions")
    else:
        df_repartitioned = df.repartition(partition_count)
        print(f"🔄 Repartitioned evenly: {current_partitions} → {partition_count} partitions")
    
    return df_repartitioned

# =============================================================================
# 3. Intelligent Caching Strategies
# =============================================================================

def smart_cache(df, storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK):
    """
    Cache DataFrame with optimal storage level based on size estimation
    """
    # Estimate DataFrame size
    sample_size = df.sample(0.001, 42).collect()
    estimated_mb = len(str(sample_size)) * 1000 / 1024  # Rough estimate
    
    if estimated_mb < 1000:  # < 1GB, use memory
        storage_level = StorageLevel.MEMORY_ONLY
        print(f"💾 Caching in memory (estimated {estimated_mb:.0f}MB)")
    else:  # Larger datasets, use memory + disk
        storage_level = StorageLevel.MEMORY_AND_DISK_SER
        print(f"💾 Caching in memory + disk (estimated {estimated_mb:.0f}MB)")
    
    df_cached = df.persist(storage_level)
    df_cached.count()  # Trigger caching
    return df_cached

def cache_with_monitoring(df, name: str):
    """
    Cache with performance monitoring
    """
    start_time = time.time()
    df_cached = df.cache()
    count = df_cached.count()  # Trigger caching
    cache_time = time.time() - start_time
    
    print(f"📊 Cached {name}: {count:,} rows in {cache_time:.2f}s")
    return df_cached

# =============================================================================
# 4. Advanced Join Optimization
# =============================================================================

def optimize_join(large_df, small_df, join_keys: list, join_type: str = "inner"):
    """
    Optimize join performance based on table sizes and characteristics
    """
    # Check if small table can be broadcast
    small_count = small_df.count()
    
    if small_count < 1000000:  # Less than 1M rows, try broadcast
        print(f"🚀 Using broadcast join (small table: {small_count:,} rows)")
        result = large_df.join(F.broadcast(small_df), join_keys, join_type)
    else:
        # Use bucket join if tables are bucketed on join key
        print(f"🔗 Using sort-merge join")
        result = large_df.join(small_df.hint("SHUFFLE_MERGE"), join_keys, join_type)
    
    return result

def create_bucketed_table(df, table_name: str, bucket_column: str, num_buckets: int = 10):
    """
    Create bucketed table for optimal join performance
    """
    (df.write
     .mode("overwrite")
     .bucketBy(num_buckets, bucket_column)
     .sortBy(bucket_column)
     .option("path", f"/mnt/delta/{table_name}")
     .saveAsTable(table_name))
    
    print(f"🪣 Created bucketed table {table_name} with {num_buckets} buckets on {bucket_column}")

# =============================================================================
# 5. Data Skew Detection and Mitigation
# =============================================================================

def detect_skew(df, column: str, threshold: float = 0.1) -> Dict[str, Any]:
    """
    Detect data skew in a column
    """
    print(f"🔍 Analyzing skew in column: {column}")
    
    # Get value distribution
    distribution = (df.groupBy(column)
                   .count()
                   .orderBy(F.desc("count"))
                   .limit(20)
                   .collect())
    
    total_count = df.count()
    top_value_count = distribution[0]["count"] if distribution else 0
    skew_ratio = top_value_count / total_count if total_count > 0 else 0
    
    is_skewed = skew_ratio > threshold
    
    print(f"📊 Skew Analysis Results:")
    print(f"   • Total records: {total_count:,}")
    print(f"   • Top value frequency: {skew_ratio:.2%}")
    print(f"   • Is skewed (>{threshold:.1%}): {is_skewed}")
    
    if is_skewed:
        print("   • Top skewed values:")
        for i, row in enumerate(distribution[:5]):
            value = row[column]
            count = row["count"]
            pct = count / total_count * 100
            print(f"     {i+1}. '{value}': {count:,} ({pct:.1f}%)")
    
    return {
        "is_skewed": is_skewed,
        "skew_ratio": skew_ratio,
        "top_values": distribution[:10]
    }

def apply_salt_join(df1, df2, skewed_column: str, salt_range: int = 100):
    """
    Apply salting technique to mitigate join skew
    """
    print(f"🧂 Applying salt join on {skewed_column} with range {salt_range}")
    
    # Add salt to both DataFrames
    df1_salted = df1.withColumn(
        f"salted_{skewed_column}",
        F.concat(F.col(skewed_column), F.lit("_"), 
                (F.rand() * salt_range).cast("int").cast("string"))
    )
    
    df2_salted = df2.withColumn(
        f"salted_{skewed_column}",
        F.concat(F.col(skewed_column), F.lit("_"), 
                (F.rand() * salt_range).cast("int").cast("string"))
    )
    
    # Join on salted key
    result = df1_salted.join(df2_salted, f"salted_{skewed_column}")
    
    # Remove salt column
    result = result.drop(f"salted_{skewed_column}")
    
    return result

# =============================================================================
# 6. Performance Monitoring and Benchmarking
# =============================================================================

def benchmark_operation(operation_func: Callable, 
                       description: str, 
                       iterations: int = 3) -> float:
    """
    Benchmark any Spark operation with detailed metrics
    """
    print(f"\n🏁 Benchmarking: {description}")
    times = []
    
    for i in range(iterations):
        # Clear cache to ensure fair comparison
        spark.catalog.clearCache()
        
        start_time = time.time()
        try:
            result = operation_func()
            if hasattr(result, 'count'):
                count = result.count()
                print(f"   Run {i+1}: {time.time() - start_time:.2f}s ({count:,} rows)")
            else:
                print(f"   Run {i+1}: {time.time() - start_time:.2f}s")
        except Exception as e:
            print(f"   Run {i+1}: FAILED - {str(e)}")
            continue
            
        duration = time.time() - start_time
        times.append(duration)
    
    if times:
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        print(f"   📊 Results: avg={avg_time:.2f}s, min={min_time:.2f}s, max={max_time:.2f}s")
        return avg_time
    return float('inf')

def analyze_query_performance(df, operation_description: str = "Query"):
    """
    Comprehensive query performance analysis
    """
    print(f"🔍 Performance Analysis: {operation_description}")
    print("=" * 50)
    
    # Basic metrics
    print(f"📊 Partitions: {df.rdd.getNumPartitions()}")
    
    # Physical plan analysis
    print("\n📋 Physical Plan:")
    df.explain("cost")
    
    # Check for common performance issues
    partitions = df.rdd.getNumPartitions()
    if partitions > 4000:
        print("⚠️  WARNING: Very high partition count, consider coalescing")
    elif partitions < 10:
        print("⚠️  WARNING: Very low partition count, consider repartitioning")
    
    # Partition size analysis
    try:
        partition_sizes = df.glom().map(len).collect()
        if len(partition_sizes) <= 100:  # Only for smaller partition counts
            max_size = max(partition_sizes) if partition_sizes else 0
            min_size = min(partition_sizes) if partition_sizes else 0
            avg_size = sum(partition_sizes) / len(partition_sizes) if partition_sizes else 0
            
            print(f"📦 Partition sizes: avg={avg_size:.0f}, min={min_size}, max={max_size}")
            
            if max_size > 0 and min_size > 0:
                skew_ratio = max_size / min_size
                if skew_ratio > 3:
                    print(f"⚠️  WARNING: Partition skew detected (ratio: {skew_ratio:.1f})")
    except:
        print("📦 Partition analysis skipped (too many partitions)")

# =============================================================================
# 7. Complete Performance Optimization Pipeline
# =============================================================================

def optimize_dataframe_pipeline(df, 
                               operation_name: str,
                               enable_caching: bool = True,
                               target_partitions: int = None):
    """
    Complete optimization pipeline for DataFrame operations
    """
    print(f"🚀 Optimizing pipeline: {operation_name}")
    print("=" * 50)
    
    # 1. Analyze current state
    analyze_query_performance(df, "Original DataFrame")
    
    # 2. Optimize partitioning
    if target_partitions:
        df = smart_repartition(df, target_partitions)
    
    # 3. Apply caching if beneficial
    if enable_caching:
        df = smart_cache(df)
    
    # 4. Final analysis
    print("\n✅ Optimization complete!")
    return df

# =============================================================================
# 8. Usage Examples and Templates
# =============================================================================

# Example 1: Optimize large aggregation
def optimize_large_aggregation():
    """Example: Optimize a large groupBy aggregation"""
    df = spark.table("large_fact_table")
    
    # Configure for the data size
    configure_shuffle_partitions(500)  # 500GB dataset
    
    # Optimize the aggregation
    result = (df
              .repartition(1000, "partition_key")  # Even distribution
              .groupBy("category", "region")
              .agg(F.sum("amount").alias("total_amount"),
                   F.count("*").alias("record_count"))
              .coalesce(50))  # Reduce output partitions
    
    return result

# Example 2: Optimize skewed join
def optimize_skewed_join():
    """Example: Handle a skewed join scenario"""
    large_df = spark.table("transactions")
    small_df = spark.table("customers")
    
    # Check for skew
    skew_info = detect_skew(large_df, "customer_id")
    
    if skew_info["is_skewed"]:
        # Apply salt join
        result = apply_salt_join(large_df, small_df, "customer_id")
    else:
        # Regular optimized join
        result = optimize_join(large_df, small_df, ["customer_id"])
    
    return result

# Example 3: Performance comparison
def compare_optimization_strategies():
    """Compare different optimization strategies"""
    df = spark.table("test_table")
    
    # Test different approaches
    strategies = {
        "no_optimization": lambda: df.groupBy("category").count(),
        "with_cache": lambda: df.cache().groupBy("category").count(),
        "with_repartition": lambda: df.repartition(200).groupBy("category").count(),
        "with_both": lambda: df.repartition(200).cache().groupBy("category").count()
    }
    
    results = {}
    for name, strategy in strategies.items():
        results[name] = benchmark_operation(strategy, name)
    
    # Find best strategy
    best_strategy = min(results, key=results.get)
    print(f"🏆 Best strategy: {best_strategy} ({results[best_strategy]:.2f}s)")
    
    return results

print("⚡ Performance tuning patterns loaded!")
print("🎯 Quick Start:")
print("   1. Run configure_spark_for_performance() first")
print("   2. Use analyze_query_performance(df) to identify bottlenecks")
print("   3. Apply appropriate optimization patterns")
print("   4. Benchmark before and after changes")
print("\n💡 Pro Tips:")
print("   • Enable AQE for automatic optimizations")
print("   • Monitor Spark UI for detailed execution metrics")
print("   • Use Delta Lake optimizations when possible")
print("   • Profile before optimizing - measure twice, cut once")