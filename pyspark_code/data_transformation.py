# Databricks PySpark: Data Transformation Cheat Sheet
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1) Column creation & renaming
# Create a new column with a literal value
df = df.withColumn("source_system", F.lit("SystemA"))
# Derive a new column from existing ones
df = df.withColumn("revenue", F.col("price") * F.col("quantity"))
# Rename a column
df = df.withColumnRenamed("cust_id", "customer_id")
# Create a column using a SQL expression
df = df.withColumn("is_discounted", F.expr("price < original_price"))

# 2) Conditional logic
# Create a categorical flag with when/otherwise
df = df.withColumn("customer_segment",
    F.when(F.col("total_spent") > 5000, "Premium")
     .when(F.col("total_spent") > 1000, "Standard")
     .otherwise("Basic"))

# 3) String transformations
# Concatenate multiple string columns
df = df.withColumn("full_address", F.concat_ws(", ", "street", "city", "zip_code"))
# Extract a pattern using regex
df = df.withColumn("product_code", F.regexp_extract("product_id", r"([A-Z]+)", 1))
# Split a string column into an array
df = df.withColumn("tags_array", F.split(F.col("tags_string"), ","))

# 4) Date & time transformations
# Extract year from a date column
df = df.withColumn("order_year", F.year("order_date"))
# Calculate the difference in days between two dates
df = df.withColumn("days_to_ship", F.datediff(F.col("ship_date"), F.col("order_date")))
# Format a timestamp for reporting
df = df.withColumn("order_hour", F.date_format("order_timestamp", "HH"))

# 5) Structuring & flattening
# Create a nested struct from columns
df = df.withColumn("customer_details", F.struct("customer_id", "email"))
# Flatten a struct back into top-level columns
df = df.select("order_id", "customer_details.*")
# Explode an array into multiple rows
df = df.withColumn("product_item", F.explode("products_array"))

# 6) Aggregations & rollups
# Basic aggregation to create a summary DataFrame
daily_sales = df.groupBy("order_date").agg(F.sum("revenue").alias("total_revenue"))
# Pivot a column to transform rows into columns
quarterly_sales = df.groupBy("product_category").pivot("order_quarter").sum("revenue")

# 7) Window (analytical) functions
# Rank records within a group (e.g., top N products per category)
window_spec = Window.partitionBy("category").orderBy(F.desc("sales"))
df = df.withColumn("sales_rank", F.rank().over(window_spec))
# Calculate a running total
df = df.withColumn("running_total", F.sum("sales").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)))

# 8) Joining & merging
# Enrich a DataFrame by joining with another
df_enriched = df.join(df_customers, "customer_id", "left")
# Combine two DataFrames with the same structure
df_all_years = df_2023.unionByName(df_2024)

# See jakublasak.com for more data engineering tips
