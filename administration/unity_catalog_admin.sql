-- 🏛️ Unity Catalog Administration: Complete Security & Governance Guide
-- Production-ready patterns for data governance, security, and access control

-- =============================================================================
-- 1. Catalog & Schema Management
-- =============================================================================

-- Create catalog hierarchy for data governance
CREATE CATALOG IF NOT EXISTS production_data
COMMENT 'Production data catalog for business analytics';

-- Set catalog ownership and permissions
ALTER CATALOG production_data OWNER TO `data-admin-group`;
GRANT USE CATALOG ON CATALOG production_data TO `all-users`;
GRANT CREATE SCHEMA ON CATALOG production_data TO `data-engineers`;

-- Create schemas following medallion architecture
CREATE SCHEMA IF NOT EXISTS production_data.raw
COMMENT 'Raw ingested data from source systems';

CREATE SCHEMA IF NOT EXISTS production_data.bronze  
COMMENT 'Validated and cleaned raw data';

CREATE SCHEMA IF NOT EXISTS production_data.silver
COMMENT 'Business-logic applied, conformed data';

CREATE SCHEMA IF NOT EXISTS production_data.gold
COMMENT 'Aggregated data ready for analytics and reporting';

-- Schema-level permissions
GRANT USAGE ON SCHEMA production_data.raw TO `data-engineers`;
GRANT CREATE TABLE, MODIFY ON SCHEMA production_data.raw TO `data-engineers`;

GRANT USAGE ON SCHEMA production_data.gold TO `analysts`;
GRANT SELECT ON SCHEMA production_data.gold TO `analysts`;
GRANT SELECT ON SCHEMA production_data.gold TO `business-users`;

-- =============================================================================
-- 2. External Storage Configuration
-- =============================================================================

-- Create storage credential (adjust for your cloud provider)
-- For AWS:
-- CREATE STORAGE CREDENTIAL aws_s3_credential
-- USING AWS (
--   aws_iam_role = 'arn:aws:iam::123456789012:role/databricks-unity-catalog-role'
-- ) COMMENT 'S3 access for Unity Catalog';

-- For Azure:
-- CREATE STORAGE CREDENTIAL azure_storage_credential
-- USING AZURE_MANAGED_IDENTITY (
--   managed_identity_id = '/subscriptions/sub-id/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/mi-name'
-- ) COMMENT 'Azure Storage access for Unity Catalog';

-- Create external location
CREATE EXTERNAL LOCATION IF NOT EXISTS production_raw_data
URL 's3://your-production-bucket/raw-data/'
WITH (STORAGE CREDENTIAL aws_s3_credential)
COMMENT 'External location for raw data ingestion';

-- Grant permissions on external location
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION production_raw_data TO `data-engineers`;
GRANT READ FILES ON EXTERNAL LOCATION production_raw_data TO `data-engineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION production_raw_data TO `etl-service-principal`;

-- Create external table using the location
CREATE TABLE IF NOT EXISTS production_data.raw.customer_events
USING DELTA
LOCATION 's3://your-production-bucket/raw-data/customer-events/'
COMMENT 'Raw customer events from external systems';

-- =============================================================================
-- 3. Volumes for File Management
-- =============================================================================

-- Create volumes for non-tabular data
CREATE VOLUME IF NOT EXISTS production_data.raw.landing_files
COMMENT 'Landing zone for incoming files';

CREATE VOLUME IF NOT EXISTS production_data.gold.reports
COMMENT 'Generated reports and exports';

-- Grant volume permissions
GRANT READ VOLUME ON VOLUME production_data.raw.landing_files TO `data-engineers`;
GRANT WRITE VOLUME ON VOLUME production_data.raw.landing_files TO `ingestion-service`;

-- Access volumes programmatically:
-- Python: dbutils.fs.ls('/Volumes/production_data/raw/landing_files/')
-- Path format: /Volumes/{catalog}/{schema}/{volume}/

-- =============================================================================
-- 4. Row-Level Security (RLS) Patterns
-- =============================================================================

-- Create row filter function for regional data access
CREATE OR REPLACE FUNCTION production_data.silver.regional_access_filter(region STRING)
RETURNS BOOLEAN
RETURN 
  CASE 
    WHEN is_member('global-access') THEN TRUE
    WHEN is_member('us-region-access') AND region = 'US' THEN TRUE
    WHEN is_member('eu-region-access') AND region IN ('EU', 'EMEA') THEN TRUE
    WHEN is_member('apac-region-access') AND region = 'APAC' THEN TRUE
    ELSE FALSE
  END;

-- Apply row filter to table
ALTER TABLE production_data.silver.customer_data 
SET ROW FILTER production_data.silver.regional_access_filter ON (region);

-- Time-based row filter (only show recent data to certain users)
CREATE OR REPLACE FUNCTION production_data.silver.time_based_filter(event_date DATE)
RETURNS BOOLEAN
RETURN 
  CASE 
    WHEN is_member('full-history-access') THEN TRUE
    WHEN is_member('recent-data-access') AND event_date >= current_date() - INTERVAL 90 DAYS THEN TRUE
    ELSE FALSE
  END;

-- =============================================================================
-- 5. Column-Level Security & Dynamic Data Masking
-- =============================================================================

-- Email masking function
CREATE OR REPLACE FUNCTION production_data.silver.email_mask(email_value STRING)
RETURNS STRING
RETURN 
  CASE
    WHEN is_member('pii-access') THEN email_value
    WHEN is_member('partial-pii-access') THEN 
      concat(left(split(email_value, '@')[0], 2), '***@', split(email_value, '@')[1])
    ELSE '***@***.com'
  END;

-- Apply column mask
ALTER TABLE production_data.silver.customers
ALTER COLUMN email SET MASK production_data.silver.email_mask;

-- Financial data masking
CREATE OR REPLACE FUNCTION production_data.silver.salary_mask(salary_value DECIMAL(10,2))
RETURNS DECIMAL(10,2) 
RETURN 
  CASE
    WHEN is_member('finance-access') THEN salary_value
    WHEN is_member('hr-access') AND salary_value <= 150000 THEN salary_value
    WHEN is_member('manager-access') THEN 
      CASE WHEN salary_value > 100000 THEN 100000.00 ELSE salary_value END
    ELSE NULL
  END;

ALTER TABLE production_data.silver.employee_data
ALTER COLUMN salary SET MASK production_data.silver.salary_mask;

-- =============================================================================
-- 6. Service Principal Management
-- =============================================================================

-- Service principals are created via REST API or Databricks CLI
-- databricks service-principals create --display-name "etl-pipeline-sp"

-- Grant permissions to service principal (use the SP's application ID)
GRANT USE CATALOG ON CATALOG production_data 
TO `12345678-abcd-1234-5678-123456789012`;

GRANT USE SCHEMA ON SCHEMA production_data.bronze 
TO `12345678-abcd-1234-5678-123456789012`;

GRANT SELECT, MODIFY ON SCHEMA production_data.bronze 
TO `12345678-abcd-1234-5678-123456789012`;

-- Create service principal groups for easier management
-- In Databricks Admin Console: Create group "etl-service-principals"
-- Add service principals to this group, then grant permissions to the group

-- =============================================================================
-- 7. Data Lineage and Auditing
-- =============================================================================

-- Query table metadata and lineage
SELECT 
  table_catalog,
  table_schema,
  table_name,
  table_type,
  created,
  comment
FROM information_schema.tables 
WHERE table_catalog = 'production_data'
  AND table_schema = 'silver'
ORDER BY created DESC;

-- Column-level metadata
SELECT 
  table_catalog,
  table_schema,
  table_name,
  column_name,
  data_type,
  is_nullable,
  column_default
FROM information_schema.columns 
WHERE table_catalog = 'production_data'
  AND table_schema = 'silver'
ORDER BY table_name, ordinal_position;

-- Query audit logs (requires system tables access)
SELECT 
  event_time,
  user_identity.email as user_email,
  action_name,
  request_params.full_name_arg as object_name,
  source_ip_address
FROM system.access.audit
WHERE action_name IN ('getTable', 'createTable', 'deleteTable', 'select')
  AND event_time >= current_timestamp() - INTERVAL 7 DAYS
  AND request_params.full_name_arg LIKE 'production_data%'
ORDER BY event_time DESC
LIMIT 100;

-- =============================================================================
-- 8. Data Classification & Tagging
-- =============================================================================

-- Tag tables with sensitivity and compliance classifications
ALTER TABLE production_data.silver.customers 
SET TAGS ('data_classification' = 'PII', 'retention_policy' = '7_years', 'compliance' = 'GDPR');

ALTER TABLE production_data.gold.financial_reports
SET TAGS ('data_classification' = 'CONFIDENTIAL', 'compliance' = 'SOX', 'department' = 'finance');

-- Query tables by classification tags
SELECT 
  t.table_catalog,
  t.table_schema,
  t.table_name,
  tt.tag_name,
  tt.tag_value
FROM information_schema.tables t
JOIN information_schema.table_tags tt ON 
  t.table_catalog = tt.catalog_name AND
  t.table_schema = tt.schema_name AND  
  t.table_name = tt.table_name
WHERE tt.tag_name = 'data_classification'
  AND tt.tag_value IN ('PII', 'CONFIDENTIAL')
ORDER BY t.table_name;

-- =============================================================================
-- 9. Delta Sharing (Data Sharing with External Organizations)
-- =============================================================================

-- Create a share for external data consumers
CREATE SHARE IF NOT EXISTS customer_analytics_share
COMMENT 'Customer analytics data for external partners';

-- Add tables to the share
ALTER SHARE customer_analytics_share 
ADD TABLE production_data.gold.customer_metrics
COMMENT 'Aggregated customer metrics - no PII';

-- Create recipient (done via API/CLI, shown for reference)
-- databricks recipients create customer-partner-recipient --authentication-type token

-- Grant share access to recipient
-- GRANT SELECT ON SHARE customer_analytics_share TO RECIPIENT `customer-partner-recipient`;

-- Monitor share usage
-- SELECT * FROM system.information_schema.share_usage_stats
-- WHERE share_name = 'customer_analytics_share';

-- =============================================================================
-- 10. Security Monitoring Queries
-- =============================================================================

-- High-activity users (potential security concern)
WITH user_activity AS (
  SELECT 
    user_identity.email as user_email,
    count(*) as action_count,
    count(distinct request_params.full_name_arg) as unique_objects
  FROM system.access.audit
  WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
    AND action_name IN ('select', 'createTable', 'deleteTable', 'alterTable')
  GROUP BY user_identity.email
)
SELECT 
  user_email,
  action_count,
  unique_objects
FROM user_activity
WHERE action_count > 100  -- Threshold for investigation
ORDER BY action_count DESC;

-- Failed access attempts
SELECT 
  event_time,
  user_identity.email,
  action_name,
  request_params.full_name_arg as attempted_resource,
  response.status_code,
  response.error_message
FROM system.access.audit
WHERE response.status_code != 200
  AND event_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY event_time DESC;

-- Data access outside business hours
SELECT 
  event_time,
  user_identity.email,
  action_name,
  request_params.full_name_arg as resource_accessed
FROM system.access.audit
WHERE action_name = 'select'
  AND (hour(event_time) < 8 OR hour(event_time) > 18)  -- Outside 8 AM - 6 PM
  AND dayofweek(event_time) IN (2,3,4,5,6)  -- Weekdays only
  AND event_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY event_time DESC;

-- =============================================================================
-- 11. Best Practices Checklist
-- =============================================================================

-- ✅ Governance Structure:
-- • Use consistent naming conventions (catalog.schema.table)
-- • Implement medallion architecture (raw/bronze/silver/gold)
-- • Document table purposes with meaningful comments
-- • Use appropriate data types and constraints

-- ✅ Access Control:
-- • Follow principle of least privilege
-- • Use groups instead of individual user grants
-- • Implement row-level and column-level security where needed
-- • Regular access reviews and cleanup

-- ✅ Security:
-- • Enable audit logging at account level
-- • Monitor for unusual access patterns
-- • Use service principals for automated processes
-- • Classify and tag sensitive data

-- ✅ Compliance:
-- • Document data lineage and transformations
-- • Implement retention policies
-- • Use appropriate masking for PII
-- • Regular security assessments

-- Pro Tips:
-- 1. Start with Unity Catalog - it's the foundation of Databricks security
-- 2. Use infrastructure as code (Terraform) for reproducible UC setup
-- 3. Test access controls in development before applying to production
-- 4. Monitor audit logs regularly for compliance and security
-- 5. Train teams on UC concepts and security best practices


