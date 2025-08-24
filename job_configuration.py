# Databricks Job Configuration: Production-Ready Patterns

# --- 1. Job Error Handling & Retry Logic ---
# Build resilient jobs that can recover from transient failures.

import time
import traceback
from typing import Callable, Any
from pyspark.sql import DataFrame

def retry_with_backoff(
    func: Callable,
    max_retries: int = 3,
    initial_delay: int = 60,
    backoff_multiplier: int = 2,
    exceptions: tuple = (Exception,)
) -> Any:
    """
    Retry function with exponential backoff.
    Essential for handling transient cloud service issues.
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except exceptions as e:
            if attempt == max_retries:
                print(f"❌ Final attempt failed: {str(e)}")
                raise
            
            delay = initial_delay * (backoff_multiplier ** attempt)
            print(f"⚠️  Attempt {attempt + 1} failed: {str(e)}")
            print(f"🔄 Retrying in {delay} seconds...")
            time.sleep(delay)

# Example: Retry table writes with backoff
def write_to_delta_with_retry(df: DataFrame, table_name: str):
    def write_operation():
        df.write \
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .saveAsTable(table_name)
        print(f"✅ Successfully wrote to {table_name}")
    
    retry_with_backoff(
        write_operation,
        max_retries=3,
        initial_delay=60,
        exceptions=(Exception,)
    )

# --- 2. Job Status Tracking & Notifications ---
# Track job progress and send alerts on failures.

import json
import requests
from datetime import datetime

class JobTracker:
    def __init__(self, job_name: str, slack_webhook: str = None):
        self.job_name = job_name
        self.slack_webhook = slack_webhook
        self.start_time = datetime.now()
        self.errors = []
        
    def log_progress(self, stage: str, message: str = ""):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] 📊 {self.job_name} - {stage}: {message}")
        
    def log_error(self, stage: str, error: Exception):
        error_msg = f"{stage}: {str(error)}"
        self.errors.append(error_msg)
        print(f"❌ ERROR in {stage}: {error_msg}")
        
    def send_notification(self, status: str, details: str = ""):
        if not self.slack_webhook:
            return
            
        duration = datetime.now() - self.start_time
        color = "good" if status == "SUCCESS" else "danger"
        
        payload = {
            "attachments": [{
                "color": color,
                "title": f"Databricks Job: {self.job_name}",
                "fields": [
                    {"title": "Status", "value": status, "short": True},
                    {"title": "Duration", "value": str(duration).split('.')[0], "short": True},
                    {"title": "Details", "value": details, "short": False}
                ],
                "footer": "Databricks Job Monitor",
                "ts": int(self.start_time.timestamp())
            }]
        }
        
        try:
            requests.post(self.slack_webhook, json=payload, timeout=10)
        except Exception as e:
            print(f"Failed to send notification: {e}")

# Usage example
tracker = JobTracker("Daily ETL Pipeline", slack_webhook="your-webhook-url")

try:
    tracker.log_progress("STARTED", "Beginning data processing")
    
    # Your ETL steps
    tracker.log_progress("EXTRACT", "Reading source data")
    df = spark.table("source_table")
    
    tracker.log_progress("TRANSFORM", f"Processing {df.count():,} records")
    processed_df = df.filter(F.col("status") == "active")
    
    tracker.log_progress("LOAD", "Writing to target table")
    write_to_delta_with_retry(processed_df, "target_table")
    
    tracker.send_notification("SUCCESS", "ETL pipeline completed successfully")
    
except Exception as e:
    tracker.log_error("PIPELINE", e)
    tracker.send_notification("FAILURE", f"Pipeline failed: {str(e)}")
    raise

# --- 3. Environment-Specific Configuration ---
# Manage different configurations for dev/staging/prod environments.

import os

class JobConfig:
    def __init__(self, environment: str = None):
        self.env = environment or os.getenv("DATABRICKS_ENV", "dev")
        self.config = self._load_config()
        
    def _load_config(self):
        configs = {
            "dev": {
                "source_catalog": "dev_catalog",
                "target_catalog": "dev_catalog", 
                "cluster_size": "small",
                "notification_level": "errors_only",
                "data_retention_days": 30
            },
            "staging": {
                "source_catalog": "staging_catalog",
                "target_catalog": "staging_catalog",
                "cluster_size": "medium", 
                "notification_level": "all",
                "data_retention_days": 90
            },
            "prod": {
                "source_catalog": "prod_catalog",
                "target_catalog": "prod_catalog",
                "cluster_size": "large",
                "notification_level": "all", 
                "data_retention_days": 365
            }
        }
        return configs.get(self.env, configs["dev"])
    
    def get(self, key: str, default=None):
        return self.config.get(key, default)

# Usage
config = JobConfig()
source_table = f"{config.get('source_catalog')}.bronze.raw_events"
target_table = f"{config.get('target_catalog')}.silver.processed_events"

# --- 4. Data Quality Validation ---
# Built-in data quality checks to catch issues early.

class DataQualityValidator:
    def __init__(self, df: DataFrame, table_name: str):
        self.df = df
        self.table_name = table_name
        self.issues = []
        
    def check_not_empty(self):
        count = self.df.count()
        if count == 0:
            self.issues.append(f"Table {self.table_name} is empty")
        return count
    
    def check_no_nulls_in_key_columns(self, key_columns: list):
        for col in key_columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                self.issues.append(f"Column {col} has {null_count} null values")
    
    def check_date_range(self, date_col: str, expected_date: str = None):
        if not expected_date:
            expected_date = datetime.now().strftime("%Y-%m-%d")
            
        actual_dates = self.df.select(date_col).distinct().collect()
        if len(actual_dates) != 1 or actual_dates[0][0].strftime("%Y-%m-%d") != expected_date:
            self.issues.append(f"Unexpected date range in {date_col}")
    
    def validate(self):
        if self.issues:
            error_msg = "Data quality issues: " + "; ".join(self.issues)
            raise ValueError(error_msg)
        print(f"✅ Data quality checks passed for {self.table_name}")

# Usage in ETL job
def validate_data_quality(df: DataFrame, table_name: str):
    validator = DataQualityValidator(df, table_name)
    record_count = validator.check_not_empty()
    validator.check_no_nulls_in_key_columns(["id", "timestamp"])
    validator.check_date_range("process_date")
    validator.validate()
    return record_count

# --- 5. Job Parameters & Widget Configuration ---
# Create parameterized jobs for flexibility.

# Databricks widgets for job parameters
dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("batch_size", "10000", "Batch Size")

# Retrieve parameters with validation
def get_job_parameters():
    start_date = dbutils.widgets.get("start_date")
    end_date = dbutils.widgets.get("end_date") 
    environment = dbutils.widgets.get("environment")
    batch_size = int(dbutils.widgets.get("batch_size"))
    
    # Validate parameters
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required parameters")
    
    # Validate date format
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Dates must be in YYYY-MM-DD format")
    
    return {
        "start_date": start_date,
        "end_date": end_date, 
        "environment": environment,
        "batch_size": batch_size
    }

# --- 6. Complete Job Template ---
# Production-ready job structure with all best practices.

def main():
    """Main job execution function with full error handling"""
    
    # Initialize tracking and configuration
    tracker = JobTracker("Production ETL Job")
    
    try:
        # Get job parameters
        params = get_job_parameters()
        config = JobConfig(params["environment"])
        
        tracker.log_progress("INIT", f"Job started with params: {params}")
        
        # Extract
        tracker.log_progress("EXTRACT", "Loading source data")
        source_df = spark.sql(f"""
            SELECT * FROM {config.get('source_catalog')}.bronze.events
            WHERE date BETWEEN '{params['start_date']}' AND '{params['end_date']}'
        """)
        
        # Validate source data
        record_count = validate_data_quality(source_df, "source_events")
        tracker.log_progress("VALIDATE", f"Validated {record_count:,} source records")
        
        # Transform
        tracker.log_progress("TRANSFORM", "Applying business logic")
        transformed_df = source_df.filter(F.col("status").isin(["active", "pending"]))
        
        # Load with retry logic
        tracker.log_progress("LOAD", "Writing to target table")
        target_table = f"{config.get('target_catalog')}.silver.processed_events"
        write_to_delta_with_retry(transformed_df, target_table)
        
        # Final validation
        final_count = validate_data_quality(transformed_df, "processed_events")
        tracker.log_progress("COMPLETE", f"Successfully processed {final_count:,} records")
        
        tracker.send_notification("SUCCESS", f"Processed {final_count:,} records")
        
    except Exception as e:
        error_details = traceback.format_exc()
        tracker.log_error("JOB", e)
        tracker.send_notification("FAILURE", error_details)
        
        # Re-raise for job failure
        raise

# Execute main function
if __name__ == "__main__":
    main()

# --- 7. Job Scheduling Best Practices ---
# Configure jobs via Databricks Jobs UI or API with these settings:

job_configuration = {
    "name": "production-etl-pipeline",
    "timeout_seconds": 3600,  # 1 hour timeout
    "max_concurrent_runs": 1,
    "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",  # Daily at 2 AM
        "timezone_id": "America/New_York"
    },
    "email_notifications": {
        "on_failure": ["data-team@company.com"],
        "on_success": [],
        "no_alert_for_skipped_runs": False
    },
    "webhook_notifications": {
        "on_failure": [{"id": "slack-webhook"}]
    },
    "retry_on_timeout": True,
    "max_retries": 2,
    "min_retry_interval_millis": 300000  # 5 minutes
}

# Pro Tips:
# 1. Always use job clusters for production (not interactive clusters)
# 2. Set appropriate timeouts based on data volume
# 3. Monitor job run history and set up alerting
# 4. Use job parameters for date ranges and environment configs
# 5. Implement circuit breakers for dependent service calls

# More job patterns at jakublasak.com