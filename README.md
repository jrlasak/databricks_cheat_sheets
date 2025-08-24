# Databricks Cheat Sheets

A curated set of production-ready snippets for PySpark, Delta Lake, Unity Catalog, and Streaming. Copy/paste into Databricks notebooks or SQL editor.

## Index

- `autoloader.py` — Production-ready Auto Loader ingestion
- `change_data_feed.sql` — Consume CDF changes into downstream tables
- `cluster_sizing.sql` — Cluster sizing playbook and tips
- `data_cleaning.py` — Common PySpark data cleaning patterns
- `data_exploration.py` — Lightweight exploration at scale
- `indempotent_etl.sql` — Idempotent MERGE and streaming upserts

New additions:

- `delta_maintenance.sql` — Delta table maintenance: OPTIMIZE, VACUUM, Z-ORDER, schema evolution
- `streaming_best_practices.py` — Structured Streaming patterns: watermarks, foreachBatch upserts
- `unity_catalog_admin.sql` — UC admin: catalogs, schemas, external locations, security
- `widgets_and_secrets.py` — dbutils widgets, secrets, jobs utils, and notebook workflows
- `dlt_quickstart.py` — Delta Live Tables pipeline with expectations
- `scd2_merge.sql` — Slowly Changing Dimension Type 2 example
- `sql_streaming_tables.sql` — Streaming tables and APPLY CHANGES INTO
- `performance_tuning.md` — Spark/Delta performance tuning checklist
- `jobs_api_examples.json` — Example multi-task Jobs JSON payload
- `uc_mounts_and_storage.py` — Storage patterns with Unity Catalog

Notes:

- Python files that reference pyspark/dbutils/dlt are meant to run in Databricks runtimes.
- Replace catalog/schema/table names and paths with your environment.
