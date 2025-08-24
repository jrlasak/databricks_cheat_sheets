-- Databricks Security & Access Control: Unity Catalog Guide
-- Production-ready security patterns for data governance.

-- --- 1. Unity Catalog Hierarchy Setup ---
-- Organize your data with proper governance structure.

-- Create catalog (equivalent to database in traditional systems)
CREATE CATALOG IF NOT EXISTS production_data
COMMENT 'Production data catalog for business analytics';

-- Create schemas within catalog (logical groupings)
CREATE SCHEMA IF NOT EXISTS production_data.raw
COMMENT 'Raw ingested data from source systems';

CREATE SCHEMA IF NOT EXISTS production_data.bronze  
COMMENT 'Validated and cleaned raw data';

CREATE SCHEMA IF NOT EXISTS production_data.silver
COMMENT 'Business-logic applied, conformed data';

CREATE SCHEMA IF NOT EXISTS production_data.gold
COMMENT 'Aggregated data ready for analytics and reporting';

-- --- 2. Table Access Control Patterns ---

-- Grant catalog permissions (admin level)
GRANT USE CATALOG ON CATALOG production_data TO `data-engineers@company.com`;
GRANT CREATE SCHEMA ON CATALOG production_data TO `data-engineers@company.com`;

-- Grant schema permissions 
GRANT USE SCHEMA ON SCHEMA production_data.raw TO `data-engineers@company.com`;
GRANT CREATE TABLE ON SCHEMA production_data.raw TO `data-engineers@company.com`;

-- Read-only access for analysts
GRANT USE CATALOG ON CATALOG production_data TO `analysts@company.com`;
GRANT USE SCHEMA ON SCHEMA production_data.gold TO `analysts@company.com`; 
GRANT SELECT ON SCHEMA production_data.gold TO `analysts@company.com`;

-- Table-level permissions for sensitive data
GRANT SELECT ON TABLE production_data.gold.customer_metrics TO `marketing-team@company.com`;
GRANT SELECT ON TABLE production_data.gold.financial_summary TO `finance-team@company.com`;

-- --- 3. Row-Level Security (RLS) ---
-- Control access to specific rows based on user attributes.

-- Enable row-level security on a table
ALTER TABLE production_data.gold.sales_data 
SET ROW FILTER finance_filter ON (department);

-- Create row filter function
CREATE OR REPLACE FUNCTION finance_filter(department STRING)
RETURNS BOOLEAN
RETURN 
  CASE 
    WHEN is_member('finance-team@company.com') THEN TRUE
    WHEN is_member('sales-team@company.com') AND department = 'Sales' THEN TRUE
    WHEN is_member('marketing-team@company.com') AND department IN ('Sales', 'Marketing') THEN TRUE
    ELSE FALSE
  END;

-- --- 4. Column-Level Security & Dynamic Masking ---
-- Protect sensitive data with column masking.

-- Apply column mask for PII data
ALTER TABLE production_data.silver.customers
ALTER COLUMN email SET MASK email_mask;

-- Create masking function
CREATE OR REPLACE FUNCTION email_mask(email_value STRING)
RETURNS STRING
RETURN 
  CASE
    WHEN is_member('customer-service@company.com') THEN email_value  -- Full access
    WHEN is_member('analysts@company.com') THEN 
      concat('***@', split(email_value, '@')[1])  -- Domain only
    ELSE '***@***.com'  -- Fully masked
  END;

-- Mask sensitive financial data
CREATE OR REPLACE FUNCTION salary_mask(salary_value DECIMAL(10,2))
RETURNS DECIMAL(10,2) 
RETURN 
  CASE
    WHEN is_member('hr-team@company.com') THEN salary_value
    WHEN is_member('managers@company.com') THEN 
      CASE WHEN salary_value > 100000 THEN 100000.00 ELSE salary_value END
    ELSE NULL
  END;

-- --- 5. Service Principal Management ---
-- Secure authentication for automated jobs and applications.

-- Create service principal for ETL jobs (via REST API or Databricks CLI)
-- databricks service-principals create --display-name "etl-service-principal"

-- Grant permissions to service principal
GRANT USE CATALOG ON CATALOG production_data TO `12345678-abcd-efgh-ijkl-123456789012`;
GRANT USE SCHEMA ON SCHEMA production_data.bronze TO `12345678-abcd-efgh-ijkl-123456789012`;
GRANT SELECT, MODIFY ON SCHEMA production_data.bronze TO `12345678-abcd-efgh-ijkl-123456789012`;

-- Python: Configure service principal authentication in job
import os
from databricks import sql

# Service principal credentials (store in Databricks secrets)
client_id = dbutils.secrets.get("service-principals", "etl-client-id")
client_secret = dbutils.secrets.get("service-principals", "etl-client-secret")

connection = sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    client_id=client_id,
    client_secret=client_secret
)

-- --- 6. Data Lineage and Auditing ---
-- Track data access and modifications for compliance.

-- Query lineage information
SELECT 
  table_catalog,
  table_schema, 
  table_name,
  column_name,
  data_type,
  is_nullable
FROM information_schema.columns 
WHERE table_catalog = 'production_data'
  AND table_schema = 'gold'
ORDER BY table_name, column_name;

-- Audit table access history
SELECT 
  event_time,
  user_identity.email as user_email,
  action_name,
  request_params.full_name_arg as table_name,
  source_ip_address
FROM system.access.audit
WHERE action_name IN ('getTable', 'createTable', 'deleteTable')
  AND event_time >= current_timestamp() - INTERVAL 7 DAYS
  AND request_params.full_name_arg LIKE 'production_data%'
ORDER BY event_time DESC
LIMIT 100;

-- --- 7. External Location Security ---
-- Secure access to cloud storage (S3, ADLS, GCS).

-- Create external location with secure access
CREATE EXTERNAL LOCATION IF NOT EXISTS s3_production_bucket
URL 's3://production-data-bucket/path/'
WITH (STORAGE CREDENTIAL production_s3_credential)
COMMENT 'Production S3 bucket for raw data ingestion';

-- Grant access to external location
GRANT READ FILES ON EXTERNAL LOCATION s3_production_bucket TO `data-engineers@company.com`;
GRANT WRITE FILES ON EXTERNAL LOCATION s3_production_bucket TO `etl-service-principal`;

-- --- 8. Secret Management ---
-- Secure storage and access of sensitive configuration.

-- Python: Access secrets in notebooks/jobs
def get_database_connection():
    """Secure database connection using Databricks secrets"""
    return {
        "host": dbutils.secrets.get("database-creds", "host"),
        "username": dbutils.secrets.get("database-creds", "username"), 
        "password": dbutils.secrets.get("database-creds", "password"),
        "database": dbutils.secrets.get("database-creds", "database")
    }

-- Databricks CLI: Manage secrets (run from terminal)
-- databricks secrets create-scope --scope database-creds
-- databricks secrets put --scope database-creds --key host
-- databricks secrets put --scope database-creds --key username  
-- databricks secrets put --scope database-creds --key password

-- --- 9. Compliance and Data Classification ---
-- Tag sensitive data for compliance reporting.

-- Tag tables with sensitivity levels
ALTER TABLE production_data.silver.customers 
SET TAGS ('data_classification' = 'PII', 'retention_policy' = '7_years');

ALTER TABLE production_data.gold.financial_reports
SET TAGS ('data_classification' = 'CONFIDENTIAL', 'compliance' = 'SOX');

-- Query tables by classification
SELECT 
  table_catalog,
  table_schema,
  table_name,
  comment
FROM information_schema.tables t
JOIN information_schema.table_tags tt ON 
  t.table_catalog = tt.catalog_name AND
  t.table_schema = tt.schema_name AND  
  t.table_name = tt.table_name
WHERE tt.tag_name = 'data_classification'
  AND tt.tag_value = 'PII';

-- --- 10. Security Best Practices Checklist ---

-- ✅ Catalog Organization:
-- - Separate catalogs for dev/staging/prod environments
-- - Clear schema naming conventions (raw/bronze/silver/gold)
-- - Proper catalog and schema ownership

-- ✅ Access Control:  
-- - Principle of least privilege
-- - Use groups instead of individual users
-- - Regular access reviews and cleanup
-- - Service principals for automation

-- ✅ Data Protection:
-- - Row-level security for multi-tenant data
-- - Column masking for PII/sensitive data  
-- - External location security for cloud storage
-- - Secret management for credentials

-- ✅ Monitoring & Compliance:
-- - Enable audit logging
-- - Monitor unusual access patterns
-- - Data classification tags
-- - Regular security assessments

-- ✅ Development Practices:
-- - Code reviews for security changes
-- - Automated security testing
-- - Secure CI/CD pipelines
-- - Documentation of security controls

-- Security monitoring query template
WITH security_events AS (
  SELECT 
    event_time,
    user_identity.email,
    action_name,
    request_params.full_name_arg as resource,
    response.status_code,
    source_ip_address
  FROM system.access.audit
  WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
    AND action_name IN ('getTable', 'createTable', 'deleteTable', 'alterTable')
)
SELECT 
  email,
  COUNT(*) as action_count,
  COUNT(DISTINCT resource) as unique_resources,
  MAX(event_time) as last_activity
FROM security_events  
GROUP BY email
HAVING action_count > 50  -- Flag high activity users
ORDER BY action_count DESC;

-- Pro Tips:
-- 1. Start with Unity Catalog - it's the foundation of Databricks security
-- 2. Use groups for access management, not individual users
-- 3. Implement data classification early in your data architecture  
-- 4. Monitor audit logs regularly for unusual patterns
-- 5. Test security controls in non-production environments first

-- Complete Unity Catalog guide at jakublasak.com