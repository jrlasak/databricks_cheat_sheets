# 🚀 Databricks Cluster Sizing Cheatsheet for Unknown ETL Tasks
# Use this playbook to estimate and size clusters without data intel.
# Goal: <2hr runtime, <$50 cost. Add 20% buffer to all estimates.

# ---------------------------
# Step 1: Estimate size via storage API/Delta metadata (1-2 mins)

# Use storage metadata to get size without reading data.
%sh
aws s3 ls s3://your-bucket/path/ --recursive --human-readable --summarize | tail -1
# Output: Total size (e.g., 100 GB). Buffer: Plan for 120 GB.

# For Delta tables:
DESCRIBE DETAIL your_table;  # Check numFiles and sizeInBytes

# Quick sample for schema/rows:
df.sample(0.01)  # Infer from 1% sample

# ---------------------------
# Step 2: Classify Workload & Instance Type
# Light (<50 GB joins/aggs): 4-8 cores, 16-32 GB RAM
# Medium (50-200 GB transforms): 16-32 cores, 64-128 GB RAM
# Heavy (>200 GB complex/ML): 32+ cores, 128+ GB RAM

# CPU-bound (compute-heavy): Complex calcs/ML training - Use compute-optimized 
# Memory-bound: Shuffles/joins/aggs - Use memory-optimized to avoid spills

# ---------------------------
# Step 3: Calculate costs with DBU tool
# Use Databricks Pricing Calculator (search "Databricks pricing")
# Inputs: Workers (e.g., 4x 8-core), Runtime est. (data_GB / 100 per hour)
# DBU calc: e.g., 100 GB → 1-2 hrs → ~20 DBUs @ $0.55/DBU = $11
# Rule: Budget 2x for unknowns. Enable autoscaling (min 2, max 8)

# ---------------------------
# Step 4: Launch scaled with AQE
spark.sql.shuffle.partitions = data_GB * 10  # e.g., 100 GB → 1000
spark.sql.adaptive.enabled = true  # Auto-tuning

# ---------------------------
# Step 5: Monitor and adjust
# Monitor: Set Ganglia alerts >80% CPU/RAM. Scale mid-job if needed.
# Post-Job: VACUUM and OPTIMIZE tables to prevent bloat.

# See post for full guide! linkedin.com/in/jrlasak/