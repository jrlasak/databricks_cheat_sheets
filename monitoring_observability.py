# Databricks Monitoring & Observability: Production Monitoring Guide

import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# --- 1. Structured Logging Setup ---
# Configure comprehensive logging for Databricks jobs.

class DatabricksLogger:
    """Structured logger for Databricks with multiple output destinations"""
    
    def __init__(self, job_name: str, log_level: str = "INFO"):
        self.job_name = job_name
        self.logger = logging.getLogger(job_name)
        
        # Configure log format
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Context variables
        self.context = {
            "job_name": job_name,
            "start_time": datetime.now().isoformat(),
            "cluster_id": spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown"),
            "workspace_id": spark.conf.get("spark.databricks.clusterUsageTags.orgId", "unknown")
        }
    
    def log_structured(self, level: str, message: str, **kwargs):
        """Log structured message with context"""
        log_data = {
            **self.context,
            "timestamp": datetime.now().isoformat(),
            "message": message,
            **kwargs
        }
        
        self.logger.log(
            getattr(logging, level.upper()),
            json.dumps(log_data, indent=None)
        )
    
    def log_dataframe_stats(self, df: DataFrame, stage: str):
        """Log DataFrame statistics for monitoring"""
        stats = {
            "record_count": df.count(),
            "column_count": len(df.columns),
            "partitions": df.rdd.getNumPartitions(),
            "schema_hash": str(hash(str(df.schema)))
        }
        
        self.log_structured("INFO", f"DataFrame stats for {stage}", **stats)
        return stats

# Usage example
logger = DatabricksLogger("daily_etl_pipeline")

# --- 2. Performance Metrics Collection ---
# Track job performance metrics automatically.

class PerformanceMonitor:
    """Monitor and collect performance metrics during job execution"""
    
    def __init__(self, logger: DatabricksLogger):
        self.logger = logger
        self.metrics = {}
        self.stage_timers = {}
    
    def start_stage(self, stage_name: str):
        """Start timing a processing stage"""
        self.stage_timers[stage_name] = time.time()
        self.logger.log_structured("INFO", f"Starting stage: {stage_name}")
    
    def end_stage(self, stage_name: str, record_count: int = None):
        """End timing a stage and log performance"""
        if stage_name not in self.stage_timers:
            return
        
        duration = time.time() - self.stage_timers[stage_name]
        self.metrics[f"{stage_name}_duration_seconds"] = duration
        
        metrics_data = {"duration_seconds": duration}
        if record_count:
            metrics_data.update({
                "record_count": record_count,
                "records_per_second": record_count / duration if duration > 0 else 0
            })
        
        self.logger.log_structured(
            "INFO", 
            f"Completed stage: {stage_name}",
            **metrics_data
        )
        
        del self.stage_timers[stage_name]
    
    def log_spark_metrics(self):
        """Collect and log Spark execution metrics"""
        try:
            # Get Spark context metrics
            sc = spark.sparkContext
            
            metrics = {
                "active_stages": len(sc.statusTracker().getActiveStageIds()),
                "active_jobs": len(sc.statusTracker().getActiveJobIds()),
                "executor_infos_count": len(sc.statusTracker().getExecutorInfos()),
            }
            
            # Get application info
            app_id = sc.applicationId
            metrics["application_id"] = app_id
            
            self.logger.log_structured("INFO", "Spark metrics", **metrics)
            
        except Exception as e:
            self.logger.log_structured("WARNING", f"Failed to collect Spark metrics: {e}")

# Usage example
monitor = PerformanceMonitor(logger)

# --- 3. Data Quality Monitoring ---
# Monitor data quality throughout the pipeline.

class DataQualityMonitor:
    """Monitor data quality with alerts and metrics"""
    
    def __init__(self, logger: DatabricksLogger):
        self.logger = logger
        self.quality_metrics = {}
    
    def check_data_freshness(self, df: DataFrame, date_column: str, 
                           max_age_hours: int = 24) -> Dict[str, Any]:
        """Check if data is fresh within expected timeframe"""
        
        max_date = df.select(F.max(date_column)).collect()[0][0]
        if max_date is None:
            result = {"status": "FAILED", "reason": "No data found", "max_date": None}
        else:
            hours_old = (datetime.now() - max_date).total_seconds() / 3600
            if hours_old > max_age_hours:
                result = {
                    "status": "FAILED", 
                    "reason": f"Data is {hours_old:.1f} hours old",
                    "max_date": max_date.isoformat(),
                    "hours_old": hours_old
                }
            else:
                result = {
                    "status": "PASSED",
                    "max_date": max_date.isoformat(),
                    "hours_old": hours_old
                }
        
        self.logger.log_structured("INFO", "Data freshness check", **result)
        return result
    
    def check_data_completeness(self, df: DataFrame, required_columns: List[str]) -> Dict[str, Any]:
        """Check for missing values in critical columns"""
        
        results = {"status": "PASSED", "null_counts": {}}
        total_records = df.count()
        
        for col in required_columns:
            if col not in df.columns:
                results["status"] = "FAILED"
                results[f"{col}_missing_column"] = True
                continue
            
            null_count = df.filter(F.col(col).isNull()).count()
            null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
            
            results["null_counts"][col] = {
                "null_count": null_count,
                "null_percentage": null_percentage
            }
            
            # Fail if more than 5% nulls in required columns
            if null_percentage > 5:
                results["status"] = "FAILED"
        
        self.logger.log_structured("INFO", "Data completeness check", **results)
        return results
    
    def check_data_volume(self, df: DataFrame, expected_min: int = None, 
                         expected_max: int = None) -> Dict[str, Any]:
        """Check if data volume is within expected ranges"""
        
        record_count = df.count()
        result = {"status": "PASSED", "record_count": record_count}
        
        if expected_min and record_count < expected_min:
            result["status"] = "FAILED"
            result["reason"] = f"Record count {record_count} below minimum {expected_min}"
        
        if expected_max and record_count > expected_max:
            result["status"] = "FAILED"  
            result["reason"] = f"Record count {record_count} above maximum {expected_max}"
        
        self.logger.log_structured("INFO", "Data volume check", **result)
        return result
    
    def run_all_checks(self, df: DataFrame, config: Dict[str, Any]) -> bool:
        """Run all configured data quality checks"""
        
        all_passed = True
        
        # Freshness check
        if config.get("freshness"):
            result = self.check_data_freshness(
                df, 
                config["freshness"]["date_column"],
                config["freshness"].get("max_age_hours", 24)
            )
            if result["status"] != "PASSED":
                all_passed = False
        
        # Completeness check  
        if config.get("completeness"):
            result = self.check_data_completeness(
                df,
                config["completeness"]["required_columns"]
            )
            if result["status"] != "PASSED":
                all_passed = False
        
        # Volume check
        if config.get("volume"):
            result = self.check_data_volume(
                df,
                config["volume"].get("min_records"),
                config["volume"].get("max_records")
            )
            if result["status"] != "PASSED":
                all_passed = False
        
        overall_result = "PASSED" if all_passed else "FAILED"
        self.logger.log_structured("INFO", f"Data quality assessment: {overall_result}")
        
        return all_passed

# --- 4. Alert and Notification System ---
# Send alerts for job failures and data quality issues.

import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class AlertManager:
    """Send alerts via multiple channels"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.slack_webhook = config.get("slack_webhook")
        self.email_config = config.get("email", {})
    
    def send_slack_alert(self, title: str, message: str, color: str = "danger"):
        """Send alert to Slack channel"""
        if not self.slack_webhook:
            return
        
        payload = {
            "attachments": [{
                "color": color,
                "title": title,
                "text": message,
                "footer": "Databricks Monitoring",
                "ts": int(time.time())
            }]
        }
        
        try:
            response = requests.post(self.slack_webhook, json=payload, timeout=10)
            if response.status_code == 200:
                print("✅ Slack alert sent successfully")
            else:
                print(f"❌ Failed to send Slack alert: {response.status_code}")
        except Exception as e:
            print(f"❌ Error sending Slack alert: {e}")
    
    def send_email_alert(self, subject: str, body: str):
        """Send email alert"""
        if not self.email_config:
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['from_email']
            msg['To'] = ', '.join(self.email_config['to_emails'])
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'html'))
            
            with smtplib.SMTP(self.email_config['smtp_server'], 587) as server:
                server.starttls()
                server.login(self.email_config['username'], self.email_config['password'])
                server.send_message(msg)
            
            print("✅ Email alert sent successfully")
            
        except Exception as e:
            print(f"❌ Error sending email: {e}")

# --- 5. Health Check Dashboard Data ---
# Generate metrics for monitoring dashboards.

def create_health_check_metrics(job_name: str, status: str, duration: float, 
                               record_count: int, data_quality_passed: bool):
    """Create standardized health check metrics for dashboard"""
    
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "job_name": job_name,
        "status": status,  # SUCCESS, FAILED, WARNING
        "duration_seconds": duration,
        "record_count": record_count,
        "data_quality_passed": data_quality_passed,
        "cluster_id": spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown"),
        "workspace_id": spark.conf.get("spark.databricks.clusterUsageTags.orgId", "unknown")
    }
    
    # Write to monitoring table for dashboard
    metrics_df = spark.createDataFrame([metrics])
    
    metrics_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("monitoring.job_health_metrics")
    
    return metrics

# --- 6. Resource Usage Monitoring ---
# Monitor cluster resource utilization.

def monitor_cluster_resources():
    """Monitor cluster CPU, memory, and disk usage"""
    
    try:
        # Get cluster metrics (requires cluster access)
        cluster_metrics = {
            "timestamp": datetime.now().isoformat(),
            "num_executors": len(spark.sparkContext.statusTracker().getExecutorInfos()),
            "driver_memory": spark.conf.get("spark.driver.memory", "unknown"),
            "executor_memory": spark.conf.get("spark.executor.memory", "unknown"),
            "total_cores": spark.conf.get("spark.executor.cores", "unknown")
        }
        
        # Log resource usage
        logger.log_structured("INFO", "Cluster resource metrics", **cluster_metrics)
        
        # Check for resource warnings
        warnings = []
        executor_count = cluster_metrics["num_executors"]
        
        if executor_count < 2:
            warnings.append("Low executor count - may impact performance")
        
        if warnings:
            logger.log_structured("WARNING", "Resource warnings detected", warnings=warnings)
        
        return cluster_metrics
        
    except Exception as e:
        logger.log_structured("ERROR", f"Failed to collect resource metrics: {e}")
        return None

# --- 7. Complete Monitoring Workflow ---
# Integrated monitoring for production jobs.

def monitored_etl_job():
    """Example ETL job with comprehensive monitoring"""
    
    # Initialize monitoring components
    logger = DatabricksLogger("production_etl_job")
    monitor = PerformanceMonitor(logger)
    quality_monitor = DataQualityMonitor(logger)
    
    alert_config = {
        "slack_webhook": dbutils.secrets.get("alerts", "slack_webhook"),
        "email": {
            "smtp_server": "smtp.company.com",
            "from_email": "databricks@company.com",
            "to_emails": ["data-team@company.com"],
            "username": dbutils.secrets.get("alerts", "email_user"),
            "password": dbutils.secrets.get("alerts", "email_pass")
        }
    }
    alert_manager = AlertManager(alert_config)
    
    job_start_time = time.time()
    job_status = "SUCCESS"
    total_records = 0
    
    try:
        logger.log_structured("INFO", "Starting ETL job")
        
        # Stage 1: Extract
        monitor.start_stage("extract")
        source_df = spark.table("source.raw_events")
        extract_count = logger.log_dataframe_stats(source_df, "extract")["record_count"]
        monitor.end_stage("extract", extract_count)
        
        # Stage 2: Data Quality Checks
        monitor.start_stage("quality_checks")
        quality_config = {
            "freshness": {
                "date_column": "event_timestamp",
                "max_age_hours": 2
            },
            "completeness": {
                "required_columns": ["user_id", "event_type", "event_timestamp"]
            },
            "volume": {
                "min_records": 1000,
                "max_records": 10000000
            }
        }
        
        quality_passed = quality_monitor.run_all_checks(source_df, quality_config)
        monitor.end_stage("quality_checks")
        
        if not quality_passed:
            alert_manager.send_slack_alert(
                "Data Quality Alert",
                f"Data quality checks failed for {logger.job_name}",
                color="warning"
            )
        
        # Stage 3: Transform
        monitor.start_stage("transform")
        transformed_df = source_df.filter(F.col("event_type").isin(["login", "purchase"]))
        transform_count = logger.log_dataframe_stats(transformed_df, "transform")["record_count"]
        monitor.end_stage("transform", transform_count)
        
        # Stage 4: Load
        monitor.start_stage("load")
        transformed_df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("processed.user_events")
        monitor.end_stage("load", transform_count)
        
        total_records = transform_count
        
        # Log final metrics
        job_duration = time.time() - job_start_time
        monitor.log_spark_metrics()
        monitor_cluster_resources()
        
        logger.log_structured("INFO", "ETL job completed successfully", 
                             duration_seconds=job_duration,
                             total_records_processed=total_records)
        
    except Exception as e:
        job_status = "FAILED"
        job_duration = time.time() - job_start_time
        
        error_message = f"ETL job failed: {str(e)}"
        logger.log_structured("ERROR", error_message, 
                             duration_seconds=job_duration,
                             exception_type=type(e).__name__)
        
        # Send failure alert
        alert_manager.send_slack_alert(
            "Job Failure Alert", 
            f"ETL job {logger.job_name} failed: {error_message}"
        )
        
        raise
    
    finally:
        # Create health check metrics for dashboard
        create_health_check_metrics(
            job_name=logger.job_name,
            status=job_status,
            duration=time.time() - job_start_time,
            record_count=total_records,
            data_quality_passed=quality_passed if 'quality_passed' in locals() else False
        )

# --- 8. Monitoring Dashboard Queries ---
# SQL queries for monitoring dashboards.

monitoring_queries = """
-- Job Success Rate (Last 7 Days)
SELECT 
  DATE(timestamp) as job_date,
  job_name,
  COUNT(*) as total_runs,
  COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_runs,
  COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*) as success_rate
FROM monitoring.job_health_metrics 
WHERE timestamp >= current_date() - INTERVAL 7 DAYS
GROUP BY DATE(timestamp), job_name
ORDER BY job_date DESC, job_name;

-- Average Job Duration Trend
SELECT 
  DATE(timestamp) as job_date,
  job_name,
  AVG(duration_seconds) as avg_duration_seconds,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_seconds) as median_duration_seconds,
  MAX(duration_seconds) as max_duration_seconds
FROM monitoring.job_health_metrics 
WHERE timestamp >= current_date() - INTERVAL 30 DAYS
  AND status = 'SUCCESS'
GROUP BY DATE(timestamp), job_name
ORDER BY job_date DESC, job_name;

-- Data Quality Trends
SELECT 
  DATE(timestamp) as job_date,
  job_name,
  COUNT(*) as total_runs,
  COUNT(CASE WHEN data_quality_passed = true THEN 1 END) as quality_passed_runs,
  COUNT(CASE WHEN data_quality_passed = true THEN 1 END) * 100.0 / COUNT(*) as quality_pass_rate
FROM monitoring.job_health_metrics 
WHERE timestamp >= current_date() - INTERVAL 14 DAYS
GROUP BY DATE(timestamp), job_name
ORDER BY job_date DESC, quality_pass_rate ASC;

-- Failed Jobs Summary
SELECT 
  job_name,
  COUNT(*) as failure_count,
  MAX(timestamp) as last_failure,
  AVG(duration_seconds) as avg_duration_before_failure
FROM monitoring.job_health_metrics 
WHERE status = 'FAILED'
  AND timestamp >= current_date() - INTERVAL 7 DAYS
GROUP BY job_name
ORDER BY failure_count DESC;
"""

# --- 9. Best Practices Summary ---
"""
🎯 Monitoring Best Practices:

✅ Logging:
- Use structured logging with consistent format
- Include context (job name, cluster ID, timestamps)
- Log at appropriate levels (DEBUG, INFO, WARNING, ERROR)
- Centralize logs for easy analysis

✅ Metrics:
- Track performance metrics (duration, throughput)
- Monitor resource utilization
- Collect data quality metrics
- Use standardized metric formats

✅ Alerting:
- Set up multi-channel alerts (Slack, email)
- Define clear escalation procedures
- Avoid alert fatigue with proper thresholds
- Include actionable information in alerts

✅ Data Quality:
- Implement automated quality checks
- Monitor data freshness and completeness
- Track data volume anomalies
- Set appropriate quality thresholds

✅ Dashboards:
- Create real-time monitoring dashboards
- Show trends over time, not just current state
- Include success rates and SLA metrics
- Make dashboards accessible to stakeholders

Complete monitoring guide at jakublasak.com
"""

# Execute monitoring example
if __name__ == "__main__":
    monitored_etl_job()