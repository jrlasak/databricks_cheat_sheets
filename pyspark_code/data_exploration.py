# Databricks PySpark: Data Exploration (big data friendly)

# 1) Structure, size, sample
# Reveal column names and dtypes
df.printSchema()                                
# Uncovers: schema mismatches, unexpected types, nested fields to explode

# Total row count
df.selectExpr("count(*) AS rows").show()       
# Uncovers: incomplete loads, unexpected duplicates after joins, partition read issues

# Take a tiny unbiased peek
df.sample(False, 0.001, 42).limit(20).show()
# Uncovers: obvious parsing errors, outliers without overloading the driver

# 2) Quick data health
# Count nulls per column
df.select([F.count(F.when(F.col(c).isNull(), 1)).alias(f"{c}_nulls")
           for c in df.columns]).show()         
# Uncovers: missing data hotspots, bad joins creating nulls, columns to impute/drop

# Estimate cardinality (NDV) per column
df.select([F.approx_count_distinct(c).alias(f"{c}_ndv")
           for c in df.columns]).show()
# Uncovers: high-cardinality keys, low-variance fields, potential identifiers

# 3) Numeric distribution
# Compute approximate min/median/max at scale
numeric_cols = [c for c, t in df.dtypes if t in ("int", "bigint", "double", "float", "decimal")]
qs = df.approxQuantile(numeric_cols, [0.0, 0.5, 1.0], 0.01)   
print(dict(zip(numeric_cols, qs)))                             
# Uncovers: skewed distributions, impossible values needing rules

# 4) List top frequent category values
categorical_cols = [c for c, t in df.dtypes if t in ("string", "boolean")]  
for c in categorical_cols[:5]:
    df.groupBy(c).count().orderBy(F.desc("count")).limit(20).show()  
# Uncovers: data skew, dominant categories, unexpected singletons

# See jakublasak.com for more data engineering tips