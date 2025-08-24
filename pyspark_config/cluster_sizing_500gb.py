# 📘 Databricks Cluster Sizing Cheat Sheet (500 GB workload)
# Practical, field-tested defaults and knobs to hit fast, cost-effective runs.

# TL;DR (apply these, then iterate)
spark.conf.set("spark.sql.adaptive.enabled", "true")            # AQE on
spark.conf.set("spark.sql.shuffle.partitions", "2560")          # ~200 MB per partition for 500 GB

# Executors: 4 cores each (2–5 is fine). Target ~128 total cores (e.g., 16 nodes × 8 cores).
# Memory: 4–8 GB per core; start with 6 GB. Use memory-optimized nodes. Enable autoscaling & spot.
# Skew: Let AQE fix most cases; salt hot keys if still skewed.

# ---------------------------------------------------------------
# 1) Partitioning strategy (tables < 1 TB)
# ---------------------------------------------------------------
# - Prefer Delta Liquid Clustering + auto-compaction over manual table partitioning.
# - Benefits: simpler pipelines, better file sizes/layout, less small-file overhead.
# - Still okay to partition on very selective columns in rare, well-understood cases.

-- Delta maintenance helpers (run post-writes periodically)
-- OPTIMIZE my_db.my_table ZORDER BY (optional_hot_column);
-- VACUUM my_db.my_table RETAIN 168 HOURS;  -- 7 days example; confirm retention policy

# ---------------------------------------------------------------
# 2) Shuffle partitions (where parallelism really happens)
# ---------------------------------------------------------------
# Rule of thumb: 128–256 MB per shuffle partition.
# Calculation for 500 GB: 500 × 1024 MB / 200 MB ≈ 2560 partitions

-- SQL-style
SET spark.sql.adaptive.enabled=true;
SET spark.sql.shuffle.partitions=2560;

# Python-style
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "2560")

# AQE will coalesce or split at runtime; don’t fear slight overestimation.

# ---------------------------------------------------------------
# 3) Cluster sizing: executors, cores, and memory
# ---------------------------------------------------------------
# Cores per executor: 2–5. Use 4 for balanced CPU utilization and JVM overhead.
# Total cores: You don’t need one core per partition. Aim for high parallelism without thrash.
# Example target for 500 GB: ~128 total cores processing ~128 partitions concurrently.

# Memory per core: 4–8 GB; start with ~6 GB and adjust based on spills/GC.
# Example: 128 cores × 6 GB = ~768 GB total executor memory across the cluster.

# Node type: Use memory-optimized instances for shuffle-heavy workloads.
# Cost: Use spot (preemptible) where acceptable and enable autoscaling: min small, max generous.

-- Example autoscaling (Databricks UI/API):
-- min_workers=4, max_workers=20, enable_elastic_disk=true

# ---------------------------------------------------------------
# 4) Handling data skew
# ---------------------------------------------------------------
# Detect: In Spark UI, look for tasks that run much longer or process far more data.
# Mitigate:
# - Keep AQE on (modern DBR: enabled by default). It can split skewed partitions and optimize joins.
# - If still skewed, apply salting: add a small random prefix to hot keys before shuffle joins and drop it after.

-- Example salting idea (SQL):
-- WITH salted_a AS (
--   SELECT concat_ws('#', key, CAST(rand(5)*10 AS INT)) AS salted_key, *
--   FROM a
-- ), salted_b AS (
--   SELECT concat_ws('#', key, CAST(rand(5)*10 AS INT)) AS salted_key, *
--   FROM b
-- )
-- SELECT ... FROM salted_a JOIN salted_b USING (salted_key);

# ---------------------------------------------------------------
# 5) Operational tips: monitor and iterate
# ---------------------------------------------------------------
# - Don’t trust defaults blindly—right-size spark.sql.shuffle.partitions for your data.
# - Watch for GC pauses, disk spills, long-tail tasks, and skew in Spark UI.
# - Start conservative, then scale up/down based on observed metrics.
# - Maintain tables: OPTIMIZE + VACUUM to keep file sizes healthy and reduce shuffle pain.

# ---------------------------------------------------------------
# 6) Tiny helpers
# ---------------------------------------------------------------
# Compute shuffle partitions for a given dataset size (GB) and target MB/partition:

-- SQL UDF-style calc (ad hoc):
-- SELECT CAST(ROUND((:data_gb * 1024.0) / :target_mb) AS INT) AS suggested_partitions;
-- -- Example: data_gb=500, target_mb=200 → ~2560

# Python quick calc
data_gb = 500
target_mb = 200
suggested = int(round((data_gb * 1024.0) / target_mb))
print(f"Suggested shuffle partitions: {suggested}")  # 2560

# ---------------------------------------------------------------
# 7) Summary table (rule of thumb)
# ---------------------------------------------------------------
# Configuration Aspect  | Rule of Thumb
# --------------------- | -----------------------------------------------
# Partitioning          | <1 TB: prefer Liquid Clustering & auto-compaction
# Shuffle Partitions    | Target 128–256 MB each; 500 GB → ~2560
# Executor Cores        | 2–5; start with 4 per executor
# Total Cores           | Aim ~128 for 500 GB (e.g., 16 nodes × 8 cores)
# Memory per Core       | 4–8 GB; start with 6 GB
# Node Type             | Memory-optimized; enable spot & autoscaling
# Skew Handling         | AQE on; use salting for persistent hotspot keys

# Happy tuning! Iterate based on metrics; every workload is a little different.
