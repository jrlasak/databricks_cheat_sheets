# Data Cleaning in Databricks PySpark
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 1) Inspect & schema hygiene
df.printSchema()  # Inspect schema
df.limit(5).show(truncate=False)  # Peek rows

# 2) Categorical sanity checks and null statistics
# Frequency table - useful for detecting data skew
df.groupBy("country").count().orderBy(F.desc("count")).show(20, False)
# Nulls per column
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

# 3) Handle missing values
df = df.na.fill({"country": "Unknown", "age": 0})  # Fill selected columns
df = df.fillna("N/A")  # Fill all string columns
df = df.dropna(how="any", subset=["id"])  # Drop rows with null id

# 4) Duplicates & de-duplication
df = df.dropDuplicates()  # Remove full-row duplicates
df = df.dropDuplicates(["id"])  # Keep first per id

# 5) Type casting & safe conversions
df = df.withColumn("age", F.col("age").cast(T.IntegerType()))  # Cast type
df = df.withColumn("dt", F.to_date("date_str", "yyyy-MM-dd"))  # Parse date

# 6) Value standardization & replacement
df = df.replace({"US": "USA", "U.S.": "USA"}, subset=["country"])  # Normalize
df = df.withColumn("gender", F.lower(F.col("gender")))  # Lowercase

# 7) String cleanup
df = df.withColumn("email", F.trim(F.col("email")))  # Trim spaces
df = df.withColumn("phone", F.regexp_replace("phone", r"\D", ""))  # Keep digits
df = df.withColumn("name", F.regexp_replace("name", r"\s+", " "))  # Collapse many spaces

# 8) Outlier and invalid filtering
df = df.filter((F.col("age") >= 0) & (F.col("age") <= 120))  # Drop outliers
df = df.filter(~F.col("price").isNull())  # Ensure price exists

# 9) Column ops: drop, rename
df = df.drop("temp_flag")  # Drop column
df = df.withColumnRenamed("userId", "user_id")  # Rename column

# See jakublasak.com for more data engineering tips
