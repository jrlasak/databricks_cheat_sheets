# Databricks Utilities: Widgets, Secrets, FS, and Jobs Utils
# Run in a Databricks notebook. dbutils is provided by the runtime.

# 1) Parameterize notebooks with widgets
try:
    dbutils.widgets.text("run_date", "", "Run Date (YYYY-MM-DD)")
    dbutils.widgets.dropdown("env", "dev", ["dev", "stg", "prod"], "Environment")
    dbutils.widgets.multiselect("countries", "US", ["US", "DE", "FR", "GB"], "Countries")
    run_date = dbutils.widgets.get("run_date")
    env = dbutils.widgets.get("env")
    countries = dbutils.widgets.get("countries").split(",") if dbutils.widgets.get("countries") else []
except NameError:
    # Allows local linting without Databricks runtime
    run_date, env, countries = None, "dev", []

# 2) Secrets management (create scopes in workspace UI/CLI first)
# secret_value = dbutils.secrets.get(scope="my_scope", key="my_key")
# Example: fetching a JDBC password
# jdbc_password = dbutils.secrets.get(scope="kv-scope", key="jdbc-pw")

# 3) Filesystem helpers
# List
# display(dbutils.fs.ls("/mnt/raw"))
# Read head of a file
# print(dbutils.fs.head("/mnt/raw/sample.json", 200))
# Copy and remove
# dbutils.fs.cp("/mnt/raw/sample.json", "/mnt/processed/sample.json")
# dbutils.fs.rm("/mnt/raw/old/", recurse=True)

# 4) Task values for multi-task Jobs dependency passing
# dbutils.jobs.taskValues.set(key="row_count", value=123)
# row_count = dbutils.jobs.taskValues.get(taskKey="upstream_task", key="row_count", debugValue=0)

# 5) Notebook workflows
# result = dbutils.notebook.run("/Repos/org/project/etl_step", timeout_seconds=3600, arguments={
#   "run_date": run_date,
#   "env": env
# })
# print("Child result:", result)
