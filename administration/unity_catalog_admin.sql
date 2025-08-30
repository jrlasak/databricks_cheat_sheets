-- 🏛️ Unity Catalog Admin & Security Cheat Sheet (SQL)
-- Use in a UC-enabled workspace. Replace principals/paths to your org.

-- =========================================================================
-- 0) Conventions
-- - Catalog and schemas: production_data.{raw|bronze|silver|gold}
-- - Principals shown as groups or emails; prefer groups in production.
-- - Commented lines are examples/placeholders to customize before running.
-- =========================================================================

-- 1) Catalog and schema setup ----------------------------------------------
CREATE CATALOG IF NOT EXISTS production_data COMMENT 'Production analytics';
ALTER CATALOG production_data OWNER TO `account users`;
GRANT USE CATALOG ON CATALOG production_data TO `account users`;

CREATE SCHEMA IF NOT EXISTS production_data.raw     COMMENT 'Raw ingestion';
CREATE SCHEMA IF NOT EXISTS production_data.bronze  COMMENT 'Validated raw';
CREATE SCHEMA IF NOT EXISTS production_data.silver  COMMENT 'Conformed';
CREATE SCHEMA IF NOT EXISTS production_data.gold    COMMENT 'Analytics';

-- 2) Access control (least privilege) --------------------------------------
-- Catalog-level
GRANT CREATE SCHEMA ON CATALOG production_data TO `data-engineers@company.com`;

-- Schema-level
GRANT USE SCHEMA ON SCHEMA production_data.raw TO `data-engineers@company.com`;
GRANT CREATE TABLE ON SCHEMA production_data.raw TO `data-engineers@company.com`;

-- Read-only analysts (gold only)
GRANT USE SCHEMA ON SCHEMA production_data.gold TO `analysts@company.com`;
GRANT SELECT ON SCHEMA production_data.gold TO `analysts@company.com`;

-- Sensitive tables (explicit table-level)
-- GRANT SELECT ON TABLE production_data.gold.customer_metrics  TO `marketing-team@company.com`;
-- GRANT SELECT ON TABLE production_data.gold.financial_summary TO `finance-team@company.com`;

-- 3) External locations and volumes ----------------------------------------
-- Requires an existing STORAGE CREDENTIAL with access to your bucket/container.
-- CREATE STORAGE CREDENTIAL production_s3_credential TYPE KEYTAB|... OPTIONS (...);

CREATE EXTERNAL LOCATION IF NOT EXISTS s3_production_bucket
  URL 's3://production-data-bucket/path/'
  WITH (STORAGE CREDENTIAL production_s3_credential)
  COMMENT 'External location for ingestion';

GRANT READ FILES  ON EXTERNAL LOCATION s3_production_bucket TO `data-engineers@company.com`;
GRANT WRITE FILES ON EXTERNAL LOCATION s3_production_bucket TO `etl-service-principal`;

-- Optional: external Delta table bound to a subfolder
-- CREATE TABLE production_data.bronze.events USING DELTA LOCATION 's3://production-data-bucket/path/events';

-- Volumes (managed files)
CREATE VOLUME IF NOT EXISTS production_data.gold.files COMMENT 'Non-tabular files';
-- Access via dbfs:/Volumes/production_data/gold/files

-- 4) Row- and column-level security (policies) ------------------------------
-- Row filter policy function
CREATE OR REPLACE FUNCTION finance_filter(department STRING)
RETURNS BOOLEAN
RETURN CASE 
  WHEN is_member('finance-team@company.com') THEN TRUE
  WHEN is_member('sales-team@company.com')   THEN department = 'Sales'
  WHEN is_member('marketing-team@company.com') THEN department IN ('Sales','Marketing')
  ELSE FALSE
END;

-- Apply row filter to a table
-- ALTER TABLE production_data.gold.sales_data SET ROW FILTER finance_filter ON (department);

-- Column masks
CREATE OR REPLACE FUNCTION email_mask(email_value STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('customer-service@company.com') THEN email_value
  WHEN is_member('analysts@company.com') THEN concat('***@', split(email_value, '@')[1])
  ELSE '***@***.com'
END;

CREATE OR REPLACE FUNCTION salary_mask(salary_value DECIMAL(10,2))
RETURNS DECIMAL(10,2)
RETURN CASE
  WHEN is_member('hr-team@company.com') THEN salary_value
  WHEN is_member('managers@company.com') THEN CASE WHEN salary_value > 100000 THEN 100000.00 ELSE salary_value END
  ELSE NULL
END;

-- Apply masks to columns
-- ALTER TABLE production_data.silver.customers ALTER COLUMN email  SET MASK email_mask;
-- ALTER TABLE production_data.gold.payroll   ALTER COLUMN salary SET MASK salary_mask;

-- Optional: secure view pattern (simple masking without masks/policies)
-- CREATE OR REPLACE VIEW production_data.gold.customers_secured AS
-- SELECT
--   CASE WHEN is_member('pii_readers') THEN email ELSE NULL END AS email_masked,
--   /* list non-PII columns explicitly, e.g., */ customer_id, country, signup_ts
-- FROM production_data.gold.customers;
-- GRANT SELECT ON VIEW production_data.gold.customers_secured TO `analysts@company.com`;

-- 5) Service principals (automation) ----------------------------------------
-- CLI: create a service principal (run outside SQL)
-- databricks service-principals create --display-name "etl-service-principal"

-- Grant scoped access to SPN (object id or application id as principal)
GRANT USE CATALOG ON CATALOG production_data TO `12345678-abcd-efgh-ijkl-123456789012`;
GRANT USE SCHEMA  ON SCHEMA  production_data.bronze TO `12345678-abcd-efgh-ijkl-123456789012`;
GRANT SELECT, MODIFY ON SCHEMA production_data.bronze TO `12345678-abcd-efgh-ijkl-123456789012`;

-- Python (example, commented): use Databricks SQL connector with secrets
-- from databricks import sql
-- import os
-- client_id     = dbutils.secrets.get("service-principals", "etl-client-id")
-- client_secret = dbutils.secrets.get("service-principals", "etl-client-secret")
-- conn = sql.connect(server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
--                    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
--                    client_id=client_id, client_secret=client_secret)

-- 6) Auditing and lineage ----------------------------------------------------
-- Information Schema: describe columns
SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_catalog = 'production_data' AND table_schema = 'gold'
ORDER BY table_name, column_name;

-- Access audit (requires account-level audit logs configured)
SELECT event_time,
       user_identity.email AS user_email,
       action_name,
       request_params.full_name_arg AS object_name,
       source_ip_address
FROM system.access.audit
WHERE action_name IN ('getTable','createTable','deleteTable','alterTable')
  AND event_time >= current_timestamp() - INTERVAL 7 DAYS
  AND request_params.full_name_arg LIKE 'production_data%'
ORDER BY event_time DESC
LIMIT 100;

-- 7) Delta Sharing (minimal flow) -------------------------------------------
-- CREATE SHARE ext_share COMMENT 'External consumers';
-- ALTER SHARE ext_share ADD TABLE production_data.gold.customers_secured;
-- CREATE RECIPIENT acme_recipient USING TOKEN;  -- exchanged out-of-band
-- GRANT SELECT ON SHARE ext_share TO RECIPIENT acme_recipient;

-- 8) Secrets management ------------------------------------------------------
-- CLI (run outside SQL):
-- databricks secrets create-scope --scope database-creds
-- databricks secrets put --scope database-creds --key host
-- databricks secrets put --scope database-creds --key username
-- databricks secrets put --scope database-creds --key password

-- Python example (commented):
-- def get_database_connection():
--     return {
--         "host": dbutils.secrets.get("database-creds", "host"),
--         "username": dbutils.secrets.get("database-creds", "username"),
--         "password": dbutils.secrets.get("database-creds", "password"),
--         "database": dbutils.secrets.get("database-creds", "database"),
--     }

-- 9) Compliance and data classification -------------------------------------
ALTER TABLE production_data.silver.customers
  SET TAGS ('data_classification' = 'PII', 'retention_policy' = '7_years');

ALTER TABLE production_data.gold.financial_reports
  SET TAGS ('data_classification' = 'CONFIDENTIAL', 'compliance' = 'SOX');

-- Query tables by classification tag
SELECT t.table_catalog, t.table_schema, t.table_name, t.comment
FROM information_schema.tables t
JOIN information_schema.table_tags tt
  ON t.table_catalog = tt.catalog_name
 AND t.table_schema  = tt.schema_name
 AND t.table_name    = tt.table_name
WHERE tt.tag_name = 'data_classification' AND tt.tag_value = 'PII';

-- 10) Security monitoring template ------------------------------------------
WITH security_events AS (
  SELECT event_time,
         user_identity.email AS email,
         action_name,
         request_params.full_name_arg AS resource,
         response.status_code,
         source_ip_address
  FROM system.access.audit
  WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
    AND action_name IN ('getTable','createTable','deleteTable','alterTable')
)
SELECT email,
       COUNT(*) AS action_count,
       COUNT(DISTINCT resource) AS unique_resources,
       MAX(event_time) AS last_activity
FROM security_events
GROUP BY email
HAVING action_count > 50  -- Tune threshold
ORDER BY action_count DESC;

-- 11) Best practices (checklist) --------------------------------------------
-- - Prefer groups to individuals; review access regularly.
-- - Separate catalogs for dev/staging/prod; clear schema roles.
-- - Use row filters and column masks for tenant/PII protection.
-- - Secure external locations with least-privilege credentials.
-- - Store secrets in scopes; avoid hardcoding.
-- - Enable audit logs; monitor for unusual activity.
-- - Document policies; automate in CI/CD where possible.

-- Complete Unity Catalog guide at jakublasak.com
