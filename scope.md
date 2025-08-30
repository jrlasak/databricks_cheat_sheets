# Databricks Cheat Sheets — Scope Rules

Purpose

- Define clear scopes for each top-level folder so assistants can route new content without overlap.
- Standardize snippet structure, naming, and cross-links so readers can navigate quickly.

Conventions

- Folder names are lowercase_snake_case. Prefer American spelling (optimization).
- Primary languages: SQL, Python (PySpark), Scala where relevant; shell only for setup.
- Keep copy‑paste‑ready snippets small and runnable; link to deeper patterns rather than duplicating.

## Front‑matter for every snippet (.md or .py)

title: <short-actionable-title>
folder: <one-of-the-folders-below>
tags: [topic, product, pattern]
level: starter|intermediate|advanced
runtimes: [dbr-14.x+]
cloud: [aws|azure|gcp|any]
requires: [uc|table-owner|workspace-admin|none]

---

Routing rules (decision tree)

1. Is it about permissions, catalogs/schemas/tables ownership, audit, lineage, or workspace policy? → administration_and_governance
2. Is it about moving data into the lakehouse from files/queues, schema inference/evolution, Auto Loader, or initial landing streams? → ingestion
3. Is it about shaping data after landing (joins, windowing, SCDs, CDF consumption, DLT pipelines)? → elt_and_transformation
4. Is it about Delta table properties, OPTIMIZE/VACUUM, clustering, constraints, or transaction tuning? → delta_lake_management_and_optimization
5. Is it about cluster/warehouse types, node sizes, autoscaling, driver/worker ratios, spot vs on‑demand? → compute_and_sizing
6. Is it about budget control, chargeback/tags, auto‑termination policies, job/warehouse cost tips? → cost_management_finops
7. Is it about domain modeling, medallion architecture, table naming/partitioning, SCD design, marts? → data_architecture_and_modeling
8. Is it about monitoring jobs/streams, data quality checks, alerting, SLAs/SLOs, retry/backoff? → observability_and_reliability
9. Is it about Jobs/Workflows (DAGs, triggers, parameters, conditional/fan‑out), deployment and scheduling? → orchestration_and_scheduling
10. Is it query authoring, dashboards, SQL warehouses, BI connectivity/caching? → sql_and_bi_consumption
11. Is it generic PySpark utilities that don’t belong to a single pipeline (profiling helpers, small UDF patterns)? → pyspark_code_snippets

Cross-linking

- If a snippet touches two areas, place it where the primary action occurs and add “Related” links at the bottom to the secondary folder paths.
- Do not duplicate identical snippets across folders; prefer a single source plus links.

========================================
Folder scopes
========================================

administration_and_governance

- Scope: Unity Catalog object model, permissions, grants/revokes, catalogs/schemas/tables/views/volumes/models, lineage view, audit logs, workspace policies.
- Include: role design, service principals, row/column masking examples, privilege tables, lineage how‑tos.
- Exclude: performance tuning (→ delta_lake_management_and_optimization), monitoring alerts (→ observability_and_reliability).

compute_and_sizing

- Scope: cluster and SQL warehouse selection/sizing; autoscaling, instance classes, driver/worker ratios, shuffle/storage considerations.
- Include: sizing calculators/checklists, spot vs on‑demand guidelines, serverless notes, GPU vs CPU hints.
- Exclude: cost showback/chargeback (→ cost_management_finops), SQL query patterns (→ sql_and_bi_consumption).

cost_management_finops

- Scope: cost visibility and control; tags, budgets, auto‑termination, idle‑time reduction, utilization tactics.
- Include: recommended policies, example tag maps, warehouse/cluster policy settings, cost review checklists.
- Exclude: performance-only tuning (→ compute_and_sizing or delta_lake_management_and_optimization).

data_architecture_and_modeling

- Scope: medallion layers, domain/data product boundaries, naming/partitioning, modeling patterns (star/snowflake), SCD design choices.
- Include: table naming conventions, partition/zoning standards, CDC topology choices, semantic layer notes.
- Exclude: actual DDL/ETL code beyond exemplars (those live in elt_and_transformation or delta_lake_management_and_optimization).

delta_lake_management_and_optimization

- Scope: Delta features and maintenance: OPTIMIZE/compaction, clustering strategies, VACUUM/retention, constraints, table properties, data skipping.
- Include: cadence playbooks, when to use clustering vs partitioning, safe VACUUM settings, maintenance runbooks.
- Exclude: ingestion logic or business transforms (→ ingestion, elt_and_transformation).

elt_and_transformation

- Scope: transforming landed data (batch or streaming): joins, window functions, aggregations, UDFs, SCD1/2, CDF consumption, DLT pipeline code.
- Include: reusable SQL/PySpark transform templates, DLT expectations, merge patterns, incremental processing logic.
- Exclude: initial file/queue ingestion (→ ingestion), table maintenance (→ delta_lake_management_and_optimization).

ingestion

- Scope: getting data into the platform: Auto Loader, file notification/listing, structured streaming sources (files, message buses), checkpoints, schema inference/evolution.
- Include: source connectors, ingestion reliability patterns, schema evolution options, landing-zone layout.
- Exclude: downstream business transforms (→ elt_and_transformation).

observability_and_reliability

- Scope: pipeline/job/stream health: metrics, logs, alerts, SLAs/SLOs, retries/backoff, data quality checks/expectations, circuit breakers.
- Include: monitoring dashboards, alert examples, expectations libraries, on-failure runbooks.
- Exclude: lineage/privileges (→ administration_and_governance).

orchestration_and_scheduling

- Scope: Jobs/Workflows DAGs, task parameters, conditional branches, fan‑out/fan‑in, triggers (time/event), deployment and run configuration.
- Include: job JSON/YAML examples, environment passing, notebook/script/task templates, promotion between environments.
- Exclude: cost governance (→ cost_management_finops), cluster sizing (→ compute_and_sizing).

pyspark_code_snippets

- Scope: small, generic PySpark utilities and helpers not tied to a single pipeline (e.g., quick profiling, null handling, schema diffs).
- Include: data_exploration.py, data_cleaning.py, UDF examples, reusable functions.
- Exclude: end‑to‑end pipelines (→ ingestion or elt_and_transformation), Delta maintenance (→ delta_lake_management_and_optimization).

sql_and_bi_consumption

- Scope: interactive analytics and BI: SQL warehouses, query patterns, caching, dashboards, parameters/variables, connectors.
- Include: common query templates, warehouse configuration tips, dashboard examples.
- Exclude: administrative grants (→ administration_and_governance), ETL merges (→ elt_and_transformation).

========================================
File layout inside each folder
========================================

- 01_concepts.md — one‑page cheat sheet of the most used ideas and knobs.
- 02_snippets/ — minimal, runnable copy‑paste code; one concern per file.
- 03_patterns.md — opinionated “when to use what” and rules of thumb.
- 04_checklists.md — setup/run/ops checklists.
- 05_troubleshooting.md — common failure modes and fixes.

Related links

- At the bottom of each file, add: “Related: ../<other-folder>/<file-or-folder>”. Keep links relative for portability.
