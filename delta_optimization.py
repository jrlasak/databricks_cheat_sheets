# Databricks Delta Lake Optimization: Production Performance Guide

# --- 1. OPTIMIZE: File Compaction for Query Performance ---
# Use when you have many small files (< 128MB) degrading query speed.

# Basic OPTIMIZE with automatic file sizing
OPTIMIZE my_catalog.my_schema.my_table;

# OPTIMIZE with Z-ORDER for multi-dimensional clustering
# Use for columns frequently used in WHERE clauses or joins
OPTIMIZE my_catalog.my_schema.my_table
ZORDER BY (user_id, date, country);

# OPTIMIZE specific partitions only (more efficient for large tables)
OPTIMIZE my_catalog.my_schema.my_table
WHERE date >= '2024-01-01';

# Python API for OPTIMIZE in notebooks
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/path/to/delta/table")
delta_table.optimize().executeCompaction()

# Z-ORDER via Python API
delta_table.optimize().executeZOrderBy("user_id", "date", "country")

# --- 2. VACUUM: Clean Up Old Files ---
# Removes files older than retention period (default: 7 days).
# ⚠️  WARNING: Never VACUUM with retention < 7 days in production!

# Standard VACUUM (removes files older than 7 days)
VACUUM my_catalog.my_schema.my_table;

# VACUUM with custom retention (30 days for extra safety)
VACUUM my_catalog.my_schema.my_table RETAIN 720 HOURS;

# Dry run to see what would be deleted (always test first!)
VACUUM my_catalog.my_schema.my_table DRY RUN;

# Python API for VACUUM
delta_table.vacuum(retentionHours=720)  # 30 days

# --- 3. Automated Maintenance Jobs ---
# Schedule these commands as Databricks jobs for hands-off maintenance.

# Daily OPTIMIZE job (run during off-peak hours)
# Frequency: Daily at 2 AM
# Cluster: Single node (sufficient for maintenance tasks)
tables_to_optimize = [
    "catalog.schema.table1",
    "catalog.schema.table2", 
    "catalog.schema.table3"
]

for table in tables_to_optimize:
    print(f"Optimizing {table}...")
    spark.sql(f"OPTIMIZE {table}")
    print(f"✅ Completed {table}")

# Weekly VACUUM job (run during maintenance window)
# Frequency: Weekly on Sunday at 1 AM
for table in tables_to_optimize:
    print(f"Vacuuming {table}...")
    spark.sql(f"VACUUM {table} RETAIN 720 HOURS")
    print(f"🧹 Vacuumed {table}")

# --- 4. Monitoring & Metrics ---
# Check table health and optimization opportunities.

# View table details and file statistics
DESCRIBE DETAIL my_catalog.my_schema.my_table;

# Check file count and sizes (too many small files = needs OPTIMIZE)
SHOW TABLE EXTENDED LIKE 'my_table';

# Analyze table statistics after OPTIMIZE
ANALYZE TABLE my_catalog.my_schema.my_table COMPUTE STATISTICS;

# Query to find tables needing optimization (many small files)
SELECT 
  table_name,
  num_files,
  size_in_bytes / (1024 * 1024 * 1024) as size_gb,
  CASE 
    WHEN num_files > 1000 AND (size_in_bytes / num_files) < 134217728 -- 128MB
    THEN 'NEEDS_OPTIMIZE'
    ELSE 'OK'
  END as optimization_status
FROM information_schema.tables
WHERE table_schema = 'my_schema'
  AND table_type = 'EXTERNAL'
ORDER BY num_files DESC;

# --- 5. Best Practices ---
# 🎯 OPTIMIZE Timing: 
#   - After large batch writes
#   - When query performance degrades
#   - When you see >1000 files per partition

# 🎯 Z-ORDER Columns (max 4 columns):
#   - High-cardinality columns used in WHERE/JOIN
#   - Columns with range queries (dates, numeric IDs)
#   - NOT low-cardinality (country, gender, etc.)

# 🎯 VACUUM Safety:
#   - Never < 7 days retention in production
#   - Test with DRY RUN first
#   - Schedule during maintenance windows
#   - Consider time travel requirements

# 🎯 Partitioning + Z-ORDER:
#   - Partition by low-cardinality (date, region)
#   - Z-ORDER by high-cardinality within partitions

# Pro Tip: Monitor Delta metrics in Databricks SQL or create alerts
# for tables with degrading performance indicators.
# More guides at jakublasak.com