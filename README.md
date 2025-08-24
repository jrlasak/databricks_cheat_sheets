# Databricks Cheat Sheets

A curated set of production-ready snippets for PySpark, Delta Lake, Unity Catalog, and Streaming. Copy/paste into Databricks notebooks or SQL editor.

## 📁 Organized by Category

### 📥 Data Ingestion
- [`autoloader.py`](data_ingestion/autoloader.py) — Production-ready Auto Loader ingestion pipeline

### 🔄 Data Processing 
- [`data_cleaning.py`](data_processing/data_cleaning.py) — Common PySpark data cleaning patterns
- [`data_exploration.py`](data_processing/data_exploration.py) — Lightweight exploration at scale

### 🌊 Streaming & Real-time Processing
- [`streaming_best_practices.py`](streaming/streaming_best_practices.py) — Structured Streaming patterns: watermarks, foreachBatch upserts
- [`dlt_quickstart.py`](streaming/dlt_quickstart.py) — Delta Live Tables pipeline with expectations
- [`sql_streaming_tables.sql`](streaming/sql_streaming_tables.sql) — SQL streaming tables and APPLY CHANGES INTO

### 🗂️ Delta Lake
- [`delta_maintenance.sql`](delta_lake/delta_maintenance.sql) — Delta table maintenance: OPTIMIZE, VACUUM, Z-ORDER, schema evolution
- [`change_data_feed.sql`](delta_lake/change_data_feed.sql) — Consume Change Data Feed (CDF) changes

### 🏛️ Administration & Security
- [`unity_catalog_admin.sql`](administration/unity_catalog_admin.sql) — Unity Catalog admin: catalogs, schemas, external locations, security
- [`widgets_and_secrets.py`](administration/widgets_and_secrets.py) — dbutils widgets, secrets, jobs utils, and notebook workflows
- [`uc_mounts_and_storage.py`](administration/uc_mounts_and_storage.py) — Storage patterns with Unity Catalog

### ⚡ Performance & Optimization
- [`performance_tuning.md`](performance/performance_tuning.md) — Spark/Delta performance tuning checklist
- [`performance_tuning.py`](performance/performance_tuning.py) — PySpark performance optimization techniques
- [`cluster_sizing.sql`](performance/cluster_sizing.sql) — Cluster sizing playbook and tips
- [`cluster_sizing_500gb.py`](performance/cluster_sizing_500gb.py) — Detailed cluster sizing for 500GB+ workloads

### 📋 Complete Examples & Patterns
- [`scd2_merge.sql`](examples/scd2_merge.sql) — Slowly Changing Dimension Type 2 implementation
- [`indempotent_etl.sql`](examples/indempotent_etl.sql) — Idempotent MERGE and streaming upserts
- [`jobs_api_examples.json`](examples/jobs_api_examples.json) — Multi-task Jobs API configuration

## 📝 Usage Notes

- Python files that reference `pyspark`/`dbutils`/`dlt` are meant to run in Databricks runtimes
- Replace `catalog/schema/table` names and paths with your environment
- Each file includes production-ready examples with proper error handling
- Files are organized by functional area for easier navigation
