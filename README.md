# Databricks Cheat Sheets 🚀

A comprehensive collection of production-ready Databricks code snippets and best practices for data engineers, data scientists, and platform teams.

## 📚 Available Cheat Sheets

### Data Engineering Fundamentals
- **[Autoloader](autoloader.py)** - Production-ready streaming ingestion pipeline with schema evolution and error handling
- **[Data Cleaning](data_cleaning.py)** - Comprehensive data cleaning patterns with PySpark for large-scale datasets
- **[Data Exploration](data_exploration.py)** - Big data-friendly exploration techniques to understand your datasets efficiently

### Delta Lake & Storage
- **[Delta Optimization](delta_optimization.py)** - OPTIMIZE, VACUUM, and Z-ORDER strategies for peak Delta Lake performance
- **[Change Data Feed](change_data_feed.sql)** - Automated downstream Change Data Feed propagation for incremental ETL

### Performance & Scalability
- **[Performance Tuning](performance_tuning.py)** - Spark configuration optimization, partitioning strategies, and caching best practices
- **[Cluster Sizing](cluster_sizing.sql)** - Estimation playbook for right-sizing clusters for unknown ETL workloads

### Production Operations
- **[Job Configuration](job_configuration.py)** - Production-ready job patterns with error handling, retry logic, and notifications
- **[Idempotent ETL](indempotent_etl.sql)** - Fault-tolerant pipeline patterns that prevent duplicate data in batch and streaming jobs

### Security & Governance
- **[Security & Access Control](security_access_control.sql)** - Unity Catalog setup, row-level security, column masking, and service principal management

### Machine Learning
- **[MLflow Integration](mlflow_integration.py)** - Complete ML lifecycle management from experiment tracking to model deployment

### Monitoring & Reliability
- **[Monitoring & Observability](monitoring_observability.py)** - Comprehensive monitoring setup with logging, alerting, and data quality checks

## 🎯 Quick Start

Each cheat sheet is designed to be:
- **Production-ready** - Tested patterns used in real enterprise environments
- **Copy-paste friendly** - Minimal modification needed for your use case  
- **Well-documented** - Clear comments explaining the why behind each approach
- **Performance-focused** - Optimized for scale and cost efficiency

## 💡 Usage Tips

1. **Environment Variables** - Update catalog/schema names and file paths for your environment
2. **Security** - Replace example credentials with your Databricks secrets
3. **Testing** - Test thoroughly in development before deploying to production
4. **Customization** - Adapt parameters (cluster size, retention periods, etc.) to your needs

## 🔗 Additional Resources

For more detailed guides and advanced patterns, visit:
- **[jakublasak.com](https://jakublasak.com)** - In-depth tutorials and case studies
- **[LinkedIn](https://linkedin.com/in/jrlasak/)** - Latest updates and tips

## 🤝 Contributing

Found a bug or have a suggestion? Feel free to open an issue or submit a pull request!

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---
*Happy data engineering! 🎉*