# 🧹 PySpark Data Cleaning: Production-Ready Patterns
# Comprehensive data cleaning patterns for Databricks environments

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from typing import List, Dict, Optional

# =============================================================================
# 1. Data Inspection & Profiling
# =============================================================================

def profile_dataframe(df: DataFrame, sample_size: float = 0.01) -> None:
    """
    Comprehensive data profiling for large datasets
    """
    print("📊 DataFrame Profile")
    print("=" * 50)
    
    # Basic info
    print(f"🔢 Shape: {df.count():,} rows × {len(df.columns)} columns")
    
    # Schema overview
    print("\n📋 Schema:")
    df.printSchema()
    
    # Sample data
    print(f"\n🔍 Sample Data (showing {int(sample_size * 100)}%):")
    df.sample(False, sample_size, 42).limit(5).show(truncate=False)

def analyze_data_quality(df: DataFrame) -> DataFrame:
    """
    Generate comprehensive data quality report
    """
    quality_metrics = []
    
    for column in df.columns:
        col_stats = (
            df.agg(
                F.count(column).alias("non_null_count"),
                F.sum(F.col(column).isNull().cast("int")).alias("null_count"),
                F.approx_count_distinct(column).alias("unique_count")
            ).collect()[0]
        )
        
        total_count = col_stats["non_null_count"] + col_stats["null_count"]
        null_percentage = (col_stats["null_count"] / total_count * 100) if total_count > 0 else 0
        
        quality_metrics.append({
            "column": column,
            "total_count": total_count,
            "null_count": col_stats["null_count"],
            "null_percentage": round(null_percentage, 2),
            "unique_count": col_stats["unique_count"]
        })
    
    return spark.createDataFrame(quality_metrics)

# Example usage
# df = spark.table("main.bronze.raw_data")
# profile_dataframe(df)
# quality_report = analyze_data_quality(df)
# quality_report.show()

# =============================================================================
# 2. Missing Value Handling
# =============================================================================

def handle_missing_values(df: DataFrame, strategy: str = "smart") -> DataFrame:
    """
    Handle missing values with different strategies
    """
    if strategy == "drop_all":
        # Drop rows with any null values
        return df.dropna(how="any")
    
    elif strategy == "drop_critical":
        # Drop rows missing critical columns (customize list)
        critical_columns = ["id", "customer_id", "transaction_date"]
        return df.dropna(how="any", subset=[col for col in critical_columns if col in df.columns])
    
    elif strategy == "fill_defaults":
        # Fill with sensible defaults by data type
        fill_values = {}
        for col_name, data_type in df.dtypes:
            if data_type in ["string"]:
                fill_values[col_name] = "Unknown"
            elif data_type in ["int", "bigint", "double", "float"]:
                fill_values[col_name] = 0
            elif data_type in ["boolean"]:
                fill_values[col_name] = False
                
        return df.fillna(fill_values)
    
    elif strategy == "smart":
        # Smart filling based on data patterns
        df_clean = df
        
        # Fill categorical columns with mode
        string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
        for col in string_cols:
            if col not in ["id", "email", "phone"]:  # Skip unique identifiers
                mode_value = (
                    df.groupBy(col)
                    .count()
                    .orderBy(F.desc("count"))
                    .first()
                )
                if mode_value:
                    df_clean = df_clean.fillna({col: mode_value[0] or "Unknown"})
        
        # Fill numeric columns with median
        numeric_cols = [col for col, dtype in df.dtypes if dtype in ["int", "bigint", "double", "float"]]
        for col in numeric_cols:
            median_value = df.approxQuantile(col, [0.5], 0.01)[0]
            if median_value is not None:
                df_clean = df_clean.fillna({col: median_value})
        
        return df_clean
    
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

# =============================================================================
# 3. Data Type Standardization
# =============================================================================

def standardize_data_types(df: DataFrame) -> DataFrame:
    """
    Standardize common data type issues
    """
    df_typed = df
    
    # Standardize string columns
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    for col in string_cols:
        df_typed = df_typed.withColumn(col, 
            F.when(F.col(col).isNotNull(), F.trim(F.col(col)))
            .otherwise(F.col(col))
        )
    
    # Convert date strings to proper dates (common patterns)
    date_patterns = [
        ("yyyy-MM-dd", "date"),
        ("MM/dd/yyyy", "date"),
        ("dd-MM-yyyy", "date")
    ]
    
    for col in string_cols:
        if "date" in col.lower() or "time" in col.lower():
            for pattern, target_type in date_patterns:
                try:
                    df_typed = df_typed.withColumn(f"{col}_parsed",
                        F.to_date(F.col(col), pattern)
                    )
                    break
                except:
                    continue
    
    return df_typed

# =============================================================================
# 4. Duplicate Detection & Removal
# =============================================================================

def handle_duplicates(df: DataFrame, 
                     subset: Optional[List[str]] = None,
                     strategy: str = "drop") -> DataFrame:
    """
    Comprehensive duplicate handling
    """
    if strategy == "drop":
        return df.dropDuplicates(subset)
    
    elif strategy == "flag":
        # Add a flag column for duplicates
        window = Window.partitionBy(subset or df.columns)
        return df.withColumn("is_duplicate", F.row_number().over(window) > 1)
    
    elif strategy == "keep_latest":
        # Keep the most recent record (requires a timestamp column)
        if "updated_at" in df.columns:
            timestamp_col = "updated_at"
        elif "created_at" in df.columns:
            timestamp_col = "created_at"
        else:
            raise ValueError("No timestamp column found for 'keep_latest' strategy")
            
        window = Window.partitionBy(subset or df.columns).orderBy(F.desc(timestamp_col))
        return df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")
    
    return df

# =============================================================================
# 5. String Cleaning & Standardization
# =============================================================================

def clean_string_columns(df: DataFrame) -> DataFrame:
    """
    Standardize string data across common patterns
    """
    df_clean = df
    
    string_cols = [col for col, dtype in df.dtypes if dtype == "string"]
    
    for col in string_cols:
        if col.lower() in ["email"]:
            # Email standardization
            df_clean = df_clean.withColumn(col,
                F.lower(F.trim(F.regexp_replace(col, r"\s+", "")))
            )
        
        elif col.lower() in ["phone", "phone_number"]:
            # Phone number standardization (keep only digits)
            df_clean = df_clean.withColumn(col,
                F.regexp_replace(col, r"[^\d]", "")
            )
        
        elif col.lower() in ["name", "first_name", "last_name", "company"]:
            # Name standardization
            df_clean = df_clean.withColumn(col,
                F.initcap(F.trim(F.regexp_replace(col, r"\s+", " ")))
            )
        
        elif col.lower() in ["country", "state", "city"]:
            # Geographic standardization
            df_clean = df_clean.withColumn(col,
                F.upper(F.trim(col))
            )
        
        else:
            # General string cleaning
            df_clean = df_clean.withColumn(col,
                F.trim(F.regexp_replace(col, r"\s+", " "))
            )
    
    return df_clean

# =============================================================================
# 6. Outlier Detection & Handling
# =============================================================================

def detect_outliers(df: DataFrame, columns: List[str], method: str = "iqr") -> DataFrame:
    """
    Detect outliers using statistical methods
    """
    if method == "iqr":
        for col in columns:
            # Calculate IQR
            quantiles = df.approxQuantile(col, [0.25, 0.75], 0.01)
            q1, q3 = quantiles[0], quantiles[1]
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Flag outliers
            df = df.withColumn(f"{col}_outlier",
                (F.col(col) < lower_bound) | (F.col(col) > upper_bound)
            )
    
    elif method == "zscore":
        for col in columns:
            # Calculate mean and standard deviation
            stats = df.agg(F.mean(col).alias("mean"), F.stddev(col).alias("stddev")).collect()[0]
            mean_val, std_val = stats["mean"], stats["stddev"]
            
            # Flag outliers (|z-score| > 3)
            df = df.withColumn(f"{col}_outlier",
                F.abs((F.col(col) - mean_val) / std_val) > 3
            )
    
    return df

# =============================================================================
# 7. Complete Data Cleaning Pipeline
# =============================================================================

def comprehensive_data_cleaning(df: DataFrame,
                              critical_columns: List[str],
                              duplicate_subset: Optional[List[str]] = None) -> DataFrame:
    """
    Complete data cleaning pipeline
    """
    print("🧹 Starting comprehensive data cleaning...")
    
    # 1. Initial profiling
    initial_count = df.count()
    print(f"📊 Initial record count: {initial_count:,}")
    
    # 2. Handle critical missing values
    df_clean = df.dropna(how="any", subset=critical_columns)
    after_critical = df_clean.count()
    print(f"🔍 After removing records missing critical columns: {after_critical:,} ({initial_count - after_critical:,} removed)")
    
    # 3. Remove duplicates
    df_clean = handle_duplicates(df_clean, subset=duplicate_subset, strategy="drop")
    after_dedup = df_clean.count()
    print(f"🔄 After deduplication: {after_dedup:,} ({after_critical - after_dedup:,} removed)")
    
    # 4. Handle remaining missing values
    df_clean = handle_missing_values(df_clean, strategy="smart")
    
    # 5. Standardize data types and strings
    df_clean = standardize_data_types(df_clean)
    df_clean = clean_string_columns(df_clean)
    
    # 6. Add data quality metadata
    df_clean = (df_clean
                .withColumn("_cleaned_timestamp", F.current_timestamp())
                .withColumn("_cleaning_version", F.lit("1.0"))
    )
    
    final_count = df_clean.count()
    print(f"✅ Final record count: {final_count:,}")
    print(f"📈 Data retention rate: {(final_count / initial_count * 100):.2f}%")
    
    return df_clean

# =============================================================================
# 8. Usage Examples
# =============================================================================

# Example 1: Basic cleaning
# df_raw = spark.table("main.bronze.customer_data")
# df_clean = comprehensive_data_cleaning(
#     df=df_raw,
#     critical_columns=["customer_id", "email"],
#     duplicate_subset=["customer_id"]
# )

# Example 2: Custom cleaning for specific use case
# df_events = spark.table("main.bronze.user_events")
# df_events_clean = (df_events
#     .filter(F.col("event_timestamp").isNotNull())
#     .filter(F.col("user_id").isNotNull())
#     .dropDuplicates(["user_id", "event_timestamp", "event_type"])
#     .withColumn("event_date", F.to_date("event_timestamp"))
# )

print("🧹 Data cleaning patterns loaded!")
print("💡 Pro Tips:")
print("   • Always profile your data before cleaning")
print("   • Be cautious with aggressive cleaning - preserve data when possible")
print("   • Document your cleaning logic for reproducibility")
print("   • Test cleaning pipelines on samples before full runs")
