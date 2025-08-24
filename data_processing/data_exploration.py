# 🔍 PySpark Data Exploration: Big Data Friendly Patterns
# Scalable data exploration techniques for large Databricks datasets

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import NumericType, StringType
from typing import List, Dict, Any
import matplotlib.pyplot as plt
import seaborn as sns

# =============================================================================
# 1. Dataset Overview & Structure Analysis
# =============================================================================

def explore_dataset_structure(df: DataFrame) -> Dict[str, Any]:
    """
    Get comprehensive dataset overview optimized for big data
    """
    print("📊 Dataset Structure Analysis")
    print("=" * 50)
    
    # Basic metrics
    row_count = df.count()
    col_count = len(df.columns)
    
    print(f"🔢 Dimensions: {row_count:,} rows × {col_count} columns")
    print(f"💾 Estimated size: ~{(row_count * col_count * 8 / 1024**3):.2f} GB")
    
    # Schema breakdown
    schema_info = {}
    for col, dtype in df.dtypes:
        schema_info[dtype] = schema_info.get(dtype, 0) + 1
    
    print(f"\n📋 Schema Breakdown:")
    for dtype, count in schema_info.items():
        print(f"   • {dtype}: {count} columns")
    
    # Sample data with smart sampling
    sample_rate = min(0.001, 1000 / row_count) if row_count > 0 else 0.001
    print(f"\n🔍 Sample Data (sample rate: {sample_rate:.4f}):")
    df.sample(False, sample_rate, 42).limit(10).show(truncate=False)
    
    return {
        "row_count": row_count,
        "column_count": col_count,
        "schema_info": schema_info,
        "sample_rate_used": sample_rate
    }

def analyze_data_freshness(df: DataFrame, timestamp_cols: List[str] = None) -> None:
    """
    Analyze data freshness and temporal patterns
    """
    if not timestamp_cols:
        # Auto-detect timestamp columns
        timestamp_cols = [col for col, dtype in df.dtypes 
                         if 'timestamp' in dtype.lower() or 'date' in col.lower()]
    
    for col in timestamp_cols:
        if col in df.columns:
            print(f"\n📅 Temporal Analysis for {col}:")
            df.agg(
                F.min(col).alias("earliest"),
                F.max(col).alias("latest"),
                F.count(col).alias("non_null_count")
            ).show()
            
            # Recent data volume (last 30 days)
            recent_cutoff = F.date_sub(F.current_date(), 30)
            recent_count = df.filter(F.col(col) >= recent_cutoff).count()
            total_count = df.count()
            
            print(f"📈 Recent data (last 30 days): {recent_count:,} records ({recent_count/total_count*100:.1f}%)")

# =============================================================================
# 2. Data Quality Assessment
# =============================================================================

def assess_data_quality(df: DataFrame) -> DataFrame:
    """
    Comprehensive data quality assessment at scale
    """
    print("🔍 Data Quality Assessment")
    print("=" * 40)
    
    quality_metrics = []
    
    for col in df.columns:
        # Basic stats
        col_stats = df.agg(
            F.count(col).alias("non_null"),
            F.sum(F.col(col).isNull().cast("int")).alias("null_count"),
            F.approx_count_distinct(col).alias("unique_count")
        ).collect()[0]
        
        total = col_stats["non_null"] + col_stats["null_count"]
        null_pct = (col_stats["null_count"] / total * 100) if total > 0 else 0
        
        # Data type specific analysis
        col_type = dict(df.dtypes)[col]
        
        if col_type in ['string']:
            # String-specific quality checks
            empty_count = df.filter((F.col(col) == "") | (F.col(col) == " ")).count()
            avg_length = df.agg(F.avg(F.length(col)).alias("avg_len")).collect()[0]["avg_len"] or 0
            
            quality_metrics.append({
                "column": col,
                "type": col_type,
                "total_count": total,
                "null_count": col_stats["null_count"],
                "null_percentage": round(null_pct, 2),
                "unique_count": col_stats["unique_count"],
                "empty_strings": empty_count,
                "avg_length": round(avg_length, 2),
                "quality_score": round(100 - null_pct - (empty_count/total*100), 1)
            })
            
        elif col_type in ['int', 'bigint', 'double', 'float', 'decimal']:
            # Numeric-specific quality checks
            zero_count = df.filter(F.col(col) == 0).count()
            negative_count = df.filter(F.col(col) < 0).count()
            
            quality_metrics.append({
                "column": col,
                "type": col_type,
                "total_count": total,
                "null_count": col_stats["null_count"],
                "null_percentage": round(null_pct, 2),
                "unique_count": col_stats["unique_count"],
                "zero_count": zero_count,
                "negative_count": negative_count,
                "quality_score": round(100 - null_pct, 1)
            })
        
        else:
            # Generic quality metrics
            quality_metrics.append({
                "column": col,
                "type": col_type,
                "total_count": total,
                "null_count": col_stats["null_count"],
                "null_percentage": round(null_pct, 2),
                "unique_count": col_stats["unique_count"],
                "quality_score": round(100 - null_pct, 1)
            })
    
    quality_df = spark.createDataFrame(quality_metrics)
    quality_df.orderBy(F.desc("quality_score")).show(50, False)
    
    return quality_df

# =============================================================================
# 3. Statistical Profiling for Numeric Columns
# =============================================================================

def profile_numeric_columns(df: DataFrame) -> None:
    """
    Statistical profiling for numeric data at scale
    """
    numeric_cols = [col for col, dtype in df.dtypes 
                   if dtype in ('int', 'bigint', 'double', 'float', 'decimal')]
    
    if not numeric_cols:
        print("ℹ️  No numeric columns found")
        return
    
    print(f"📊 Numeric Column Profiling ({len(numeric_cols)} columns)")
    print("=" * 50)
    
    # Efficient quantile calculation
    quantiles = [0.0, 0.25, 0.5, 0.75, 0.95, 1.0]
    
    for col in numeric_cols:
        print(f"\n🔢 {col}:")
        
        # Basic statistics
        stats = df.agg(
            F.count(col).alias("count"),
            F.mean(col).alias("mean"),
            F.stddev(col).alias("stddev"),
            F.min(col).alias("min"),
            F.max(col).alias("max")
        ).collect()[0]
        
        # Calculate quantiles (approximate for large datasets)
        quantile_values = df.approxQuantile(col, quantiles, 0.01)
        
        print(f"   Count: {stats['count']:,}")
        print(f"   Mean: {stats['mean']:.2f}")
        print(f"   Std: {stats['stddev']:.2f}" if stats['stddev'] else "   Std: N/A")
        print(f"   Min: {stats['min']} | Q25: {quantile_values[1]} | Median: {quantile_values[2]}")
        print(f"   Q75: {quantile_values[3]} | P95: {quantile_values[4]} | Max: {stats['max']}")
        
        # Detect potential outliers
        if len(quantile_values) >= 4 and quantile_values[1] and quantile_values[3]:
            iqr = quantile_values[3] - quantile_values[1]
            lower_fence = quantile_values[1] - 1.5 * iqr
            upper_fence = quantile_values[3] + 1.5 * iqr
            outlier_count = df.filter((F.col(col) < lower_fence) | (F.col(col) > upper_fence)).count()
            print(f"   Potential outliers: {outlier_count:,} ({outlier_count/stats['count']*100:.1f}%)")

# =============================================================================
# 4. Categorical Data Analysis
# =============================================================================

def profile_categorical_columns(df: DataFrame, max_categories: int = 20) -> None:
    """
    Analyze categorical data patterns and distributions
    """
    string_cols = [col for col, dtype in df.dtypes if dtype == 'string']
    
    if not string_cols:
        print("ℹ️  No string columns found")
        return
    
    print(f"📊 Categorical Column Profiling ({len(string_cols)} columns)")
    print("=" * 50)
    
    for col in string_cols:
        print(f"\n📝 {col}:")
        
        # Cardinality and frequency analysis
        cardinality = df.select(col).distinct().count()
        total_count = df.count()
        
        print(f"   Unique values: {cardinality:,}")
        print(f"   Cardinality ratio: {cardinality/total_count:.4f}")
        
        # Show top categories
        top_values = (df.groupBy(col)
                     .count()
                     .orderBy(F.desc("count"))
                     .limit(max_categories))
        
        print(f"   Top {min(max_categories, cardinality)} values:")
        for row in top_values.collect():
            value = row[col] if row[col] is not None else "NULL"
            pct = (row['count'] / total_count * 100)
            print(f"     '{value}': {row['count']:,} ({pct:.1f}%)")
        
        # Identify potential data quality issues
        empty_count = df.filter((F.col(col) == "") | (F.col(col).isNull())).count()
        if empty_count > 0:
            print(f"   ⚠️  Empty/null values: {empty_count:,} ({empty_count/total_count*100:.1f}%)")
        
        # Flag high cardinality columns that might be identifiers
        if cardinality / total_count > 0.9:
            print(f"   🏷️  High cardinality - likely an identifier column")

# =============================================================================
# 5. Correlation and Relationship Analysis
# =============================================================================

def analyze_correlations(df: DataFrame, threshold: float = 0.3) -> None:
    """
    Analyze correlations between numeric columns
    """
    numeric_cols = [col for col, dtype in df.dtypes 
                   if dtype in ('int', 'bigint', 'double', 'float', 'decimal')]
    
    if len(numeric_cols) < 2:
        print("ℹ️  Need at least 2 numeric columns for correlation analysis")
        return
    
    print(f"🔗 Correlation Analysis ({len(numeric_cols)} numeric columns)")
    print("=" * 50)
    
    # Calculate correlation matrix (for reasonably sized datasets)
    if len(numeric_cols) <= 20:  # Avoid memory issues with too many columns
        from pyspark.ml.stat import Correlation
        from pyspark.ml.feature import VectorAssembler
        
        # Prepare data for correlation
        assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
        df_vector = assembler.transform(df).select("features")
        
        # Calculate Pearson correlation
        corr_matrix = Correlation.corr(df_vector, "features").head()[0]
        corr_array = corr_matrix.toArray()
        
        # Find strong correlations
        strong_correlations = []
        for i in range(len(numeric_cols)):
            for j in range(i+1, len(numeric_cols)):
                corr_val = corr_array[i][j]
                if abs(corr_val) >= threshold:
                    strong_correlations.append({
                        'col1': numeric_cols[i],
                        'col2': numeric_cols[j],
                        'correlation': round(corr_val, 3)
                    })
        
        if strong_correlations:
            print(f"🎯 Strong correlations (|r| >= {threshold}):")
            for corr in sorted(strong_correlations, key=lambda x: abs(x['correlation']), reverse=True):
                print(f"   {corr['col1']} ↔ {corr['col2']}: {corr['correlation']}")
        else:
            print(f"ℹ️  No strong correlations found (threshold: {threshold})")
    else:
        print(f"⚠️  Too many numeric columns ({len(numeric_cols)}) for correlation matrix")

# =============================================================================
# 6. Complete Data Exploration Pipeline
# =============================================================================

def comprehensive_data_exploration(df: DataFrame, 
                                 timestamp_cols: List[str] = None) -> Dict[str, Any]:
    """
    Complete data exploration pipeline
    """
    print("🔍 COMPREHENSIVE DATA EXPLORATION")
    print("=" * 60)
    
    exploration_results = {}
    
    # 1. Dataset structure
    exploration_results['structure'] = explore_dataset_structure(df)
    
    # 2. Data quality assessment
    print(f"\n" + "="*60)
    exploration_results['quality'] = assess_data_quality(df)
    
    # 3. Numeric profiling
    print(f"\n" + "="*60)
    profile_numeric_columns(df)
    
    # 4. Categorical profiling
    print(f"\n" + "="*60)
    profile_categorical_columns(df)
    
    # 5. Temporal analysis
    if timestamp_cols:
        print(f"\n" + "="*60)
        analyze_data_freshness(df, timestamp_cols)
    
    # 6. Correlation analysis
    print(f"\n" + "="*60)
    analyze_correlations(df)
    
    print(f"\n✅ Data exploration complete!")
    return exploration_results

# =============================================================================
# 7. Usage Examples
# =============================================================================

# Example 1: Quick exploration
# df = spark.table("main.bronze.customer_data")
# results = comprehensive_data_exploration(df, timestamp_cols=["created_at", "updated_at"])

# Example 2: Focused analysis
# df = spark.table("main.silver.transactions")
# explore_dataset_structure(df)
# profile_numeric_columns(df)
# analyze_correlations(df, threshold=0.5)

# Example 3: Data quality focus
# df = spark.table("main.bronze.raw_events")
# quality_report = assess_data_quality(df)
# quality_report.filter(F.col("quality_score") < 80).show()

print("🔍 Data exploration patterns loaded!")
print("💡 Pro Tips:")
print("   • Use sampling for initial exploration of very large datasets")
print("   • Focus on data quality metrics before complex analysis")
print("   • Look for unexpected patterns that might indicate data issues")
print("   • Document your findings for future reference")