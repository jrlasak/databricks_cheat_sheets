# Unity Catalog-friendly storage access patterns
# Prefer External Locations and Volumes over legacy DBFS mounts when using UC.

# 1) Legacy mounts (not allowed with UC for table I/O)
# dbutils.fs.mount(
#   source="s3a://your-bucket/raw/",
#   mount_point="/mnt/raw",
#   extra_configs={"fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider"}
# )

# 2) Recommended: External Locations + UC Tables
# - Create a Storage Credential and External Location in SQL (see unity_catalog_admin.sql)
# - Create external tables:
#   CREATE TABLE my_catalog.raw.events USING DELTA LOCATION 's3://your-bucket/raw/events';

# 3) Volumes for file-like workloads (managed by UC)
# - CREATE VOLUME my_catalog.raw.files;
# - Access path: /Volumes/my_catalog/raw/files
# - Example file ops:
# display(dbutils.fs.ls("/Volumes/my_catalog/raw/files"))
# dbutils.fs.put("/Volumes/my_catalog/raw/files/example.txt", "hello", True)
