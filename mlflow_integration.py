# Databricks MLflow: ML Lifecycle Management

import mlflow
import mlflow.sklearn
import mlflow.pyspark.ml
from mlflow.tracking import MlflowClient
from mlflow.models import infer_signature
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor as SparkRF

# --- 1. Experiment Setup and Organization ---
# Organize ML experiments with proper naming and structure.

# Set experiment (creates if doesn't exist)
mlflow.set_experiment("/Shared/ml_experiments/house_price_prediction")

# Alternative: Use experiment by ID
experiment_id = "1234567890"
mlflow.set_experiment(experiment_id=experiment_id)

# Create experiment with metadata
client = MlflowClient()
experiment_id = client.create_experiment(
    name="customer_churn_prediction",
    artifact_location="s3://mlflow-artifacts/customer_churn",
    tags={
        "team": "data-science",
        "project": "customer-retention",
        "environment": "production"
    }
)

# --- 2. Basic Experiment Tracking ---
# Track parameters, metrics, and artifacts for reproducibility.

def train_sklearn_model():
    """Example training function with MLflow tracking"""
    
    # Start MLflow run
    with mlflow.start_run(run_name="rf_baseline_v1") as run:
        
        # Log run metadata  
        mlflow.set_tags({
            "model_type": "random_forest",
            "data_version": "2024-01-15",
            "author": "data-scientist@company.com"
        })
        
        # Load and split data
        df = pd.read_csv("/databricks-datasets/housing/housing.csv")
        X = df.drop(['price'], axis=1)
        y = df['price']
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Model parameters
        n_estimators = 100
        max_depth = 10
        random_state = 42
        
        # Log parameters
        mlflow.log_params({
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "random_state": random_state,
            "train_size": len(X_train),
            "test_size": len(X_test)
        })
        
        # Train model
        model = RandomForestRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth, 
            random_state=random_state
        )
        model.fit(X_train, y_train)
        
        # Make predictions
        y_pred = model.predict(X_test)
        
        # Calculate and log metrics
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_test, y_pred)
        
        mlflow.log_metrics({
            "mse": mse,
            "rmse": rmse,
            "r2_score": r2,
            "mean_prediction": np.mean(y_pred)
        })
        
        # Log model with signature
        signature = infer_signature(X_train, y_pred)
        mlflow.sklearn.log_model(
            model, 
            "model",
            signature=signature,
            input_example=X_train.iloc[:5]
        )
        
        # Log feature importance plot
        import matplotlib.pyplot as plt
        feature_importance = pd.DataFrame({
            'feature': X_train.columns,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        plt.figure(figsize=(10, 6))
        plt.barh(feature_importance['feature'][:10], 
                feature_importance['importance'][:10])
        plt.title('Top 10 Feature Importances')
        plt.xlabel('Importance')
        plt.tight_layout()
        plt.savefig("feature_importance.png")
        mlflow.log_artifact("feature_importance.png")
        plt.close()
        
        print(f"Run ID: {run.info.run_id}")
        print(f"Experiment ID: {run.info.experiment_id}")
        
        return model, run.info.run_id

# --- 3. PySpark ML Pipeline Tracking ---
# Track Spark ML pipelines with MLflow.

def train_spark_ml_pipeline():
    """PySpark ML pipeline with MLflow tracking"""
    
    with mlflow.start_run(run_name="spark_rf_pipeline") as run:
        
        # Load Spark DataFrame
        df = spark.read.format("delta").table("ml_datasets.housing_data")
        
        # Feature engineering
        feature_cols = ["bedrooms", "bathrooms", "sqft_living", "floors"]
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        # Transform features
        train_assembled = assembler.transform(train_df)
        test_assembled = assembler.transform(test_df)
        
        # Train model
        rf = SparkRF(
            featuresCol="features",
            labelCol="price",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        model = rf.fit(train_assembled)
        
        # Make predictions
        predictions = model.transform(test_assembled)
        
        # Evaluate
        from pyspark.ml.evaluation import RegressionEvaluator
        evaluator = RegressionEvaluator(
            labelCol="price",
            predictionCol="prediction", 
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        
        # Log parameters and metrics
        mlflow.log_params({
            "num_trees": 100,
            "max_depth": 10,
            "feature_columns": feature_cols
        })
        mlflow.log_metric("rmse", rmse)
        
        # Log model
        mlflow.spark.log_model(
            model,
            "spark_model",
            signature=infer_signature(
                train_assembled.select("features").toPandas(),
                predictions.select("prediction").toPandas()
            )
        )
        
        return model, run.info.run_id

# --- 4. Model Registry Management ---
# Manage model lifecycle from experimentation to production.

def register_best_model(experiment_id: str, metric_name: str = "rmse"):
    """Find best model and register it"""
    
    # Find best run
    runs = mlflow.search_runs(
        experiment_ids=[experiment_id],
        order_by=[f"metrics.{metric_name} ASC"],
        max_results=1
    )
    
    if runs.empty:
        print("No runs found")
        return
    
    best_run = runs.iloc[0]
    run_id = best_run.run_id
    
    print(f"Best run: {run_id} with {metric_name}: {best_run[f'metrics.{metric_name}']}")
    
    # Register model
    model_name = "house_price_predictor"
    model_uri = f"runs:/{run_id}/model"
    
    model_version = mlflow.register_model(
        model_uri=model_uri,
        name=model_name,
        tags={
            "validation_status": "pending",
            "model_type": "random_forest"
        }
    )
    
    print(f"Registered model {model_name} version {model_version.version}")
    return model_version

# --- 5. Model Deployment Patterns ---
# Deploy models for batch and real-time inference.

# Batch scoring with registered model
def batch_score_model(model_name: str, version: str, input_table: str, output_table: str):
    """Batch scoring using registered model"""
    
    # Load model
    model_uri = f"models:/{model_name}/{version}"
    model = mlflow.pyfunc.load_model(model_uri)
    
    # Load input data
    input_df = spark.table(input_table)
    input_pandas = input_df.toPandas()
    
    # Score
    predictions = model.predict(input_pandas)
    
    # Combine with input data
    output_df = input_pandas.copy()
    output_df['prediction'] = predictions
    output_df['model_version'] = version
    output_df['scoring_timestamp'] = pd.Timestamp.now()
    
    # Write to output table
    output_spark_df = spark.createDataFrame(output_df)
    output_spark_df.write.mode("overwrite").saveAsTable(output_table)
    
    print(f"Batch scoring complete. Results saved to {output_table}")

# Real-time model serving (Databricks Model Serving)
def create_model_endpoint(model_name: str, model_version: str):
    """Create model serving endpoint"""
    
    endpoint_config = {
        "name": f"{model_name}-endpoint",
        "config": {
            "served_models": [{
                "name": f"{model_name}-{model_version}",
                "model_name": model_name,
                "model_version": model_version,
                "workload_size": "Small",
                "scale_to_zero_enabled": True
            }]
        }
    }
    
    # Note: Use Databricks REST API or databricks-cli to create endpoint
    print(f"Endpoint configuration: {endpoint_config}")

# --- 6. Model Validation and Testing ---
# Validate models before promoting to production.

def validate_model_performance(model_name: str, candidate_version: str, 
                             production_version: str = None):
    """Compare candidate model against production model"""
    
    # Load models
    candidate_model = mlflow.pyfunc.load_model(
        f"models:/{model_name}/{candidate_version}"
    )
    
    if production_version:
        production_model = mlflow.pyfunc.load_model(
            f"models:/{model_name}/{production_version}"
        )
    
    # Load test data
    test_data = spark.table("ml_datasets.housing_test").toPandas()
    X_test = test_data.drop(['price'], axis=1)
    y_test = test_data['price']
    
    # Evaluate candidate model
    candidate_pred = candidate_model.predict(X_test)
    candidate_rmse = np.sqrt(mean_squared_error(y_test, candidate_pred))
    candidate_r2 = r2_score(y_test, candidate_pred)
    
    results = {
        "candidate_rmse": candidate_rmse,
        "candidate_r2": candidate_r2
    }
    
    if production_version:
        production_pred = production_model.predict(X_test)
        production_rmse = np.sqrt(mean_squared_error(y_test, production_pred))
        production_r2 = r2_score(y_test, production_pred)
        
        results.update({
            "production_rmse": production_rmse,
            "production_r2": production_r2,
            "rmse_improvement": (production_rmse - candidate_rmse) / production_rmse * 100,
            "r2_improvement": (candidate_r2 - production_r2) / production_r2 * 100
        })
        
        # Decision logic
        if candidate_rmse < production_rmse * 0.95:  # 5% improvement threshold
            results["recommendation"] = "PROMOTE"
        else:
            results["recommendation"] = "REJECT"
    
    return results

# --- 7. Model Monitoring and Drift Detection ---
# Monitor model performance in production.

def log_prediction_metrics(model_name: str, model_version: str, 
                          predictions: pd.DataFrame, actuals: pd.DataFrame = None):
    """Log prediction metrics for monitoring"""
    
    with mlflow.start_run(run_name=f"monitoring_{model_name}_{model_version}"):
        mlflow.set_tags({
            "monitoring": "true",
            "model_name": model_name,
            "model_version": model_version
        })
        
        # Log prediction statistics
        pred_stats = {
            "prediction_count": len(predictions),
            "prediction_mean": predictions['prediction'].mean(),
            "prediction_std": predictions['prediction'].std(),
            "prediction_min": predictions['prediction'].min(),
            "prediction_max": predictions['prediction'].max()
        }
        
        mlflow.log_metrics(pred_stats)
        
        # If actuals are available, log performance metrics
        if actuals is not None:
            rmse = np.sqrt(mean_squared_error(actuals['actual'], predictions['prediction']))
            r2 = r2_score(actuals['actual'], predictions['prediction'])
            
            mlflow.log_metrics({
                "production_rmse": rmse,
                "production_r2": r2
            })
        
        # Log prediction distribution
        predictions['prediction'].hist(bins=50)
        plt.title('Prediction Distribution')
        plt.xlabel('Prediction Value')
        plt.ylabel('Frequency')
        plt.savefig("prediction_distribution.png")
        mlflow.log_artifact("prediction_distribution.png")
        plt.close()

# --- 8. Complete ML Workflow Example ---
# End-to-end ML workflow with MLflow.

def complete_ml_workflow():
    """Complete ML workflow from training to deployment"""
    
    # 1. Train multiple models
    print("🚀 Training models...")
    model, run_id = train_sklearn_model()
    
    # 2. Find and register best model
    print("📊 Finding best model...")
    experiment = mlflow.get_experiment_by_name("/Shared/ml_experiments/house_price_prediction")
    model_version = register_best_model(experiment.experiment_id)
    
    # 3. Validate model
    print("✅ Validating model...")
    validation_results = validate_model_performance(
        "house_price_predictor", 
        model_version.version
    )
    print(f"Validation results: {validation_results}")
    
    # 4. Promote to production if validation passes
    if validation_results.get("recommendation") == "PROMOTE":
        client.transition_model_version_stage(
            name="house_price_predictor",
            version=model_version.version,
            stage="Production"
        )
        print(f"✅ Model version {model_version.version} promoted to Production")
    
    # 5. Batch scoring
    print("🎯 Running batch scoring...")
    batch_score_model(
        "house_price_predictor",
        model_version.version,
        "ml_datasets.housing_new_data",
        "ml_predictions.house_prices_batch"
    )

# --- 9. MLflow UI and Search ---
# Programmatically search and analyze experiments.

def analyze_experiments():
    """Analyze experiment results"""
    
    # Search runs across experiments
    runs = mlflow.search_runs(
        experiment_ids=None,  # Search all experiments
        filter_string="tags.model_type = 'random_forest' and metrics.rmse < 50000",
        order_by=["metrics.rmse ASC"],
        max_results=10
    )
    
    print("Top 10 Random Forest models:")
    print(runs[['experiment_id', 'run_id', 'metrics.rmse', 'metrics.r2_score']])
    
    # Best practices query
    hyperparameter_analysis = runs.groupby(['params.max_depth', 'params.n_estimators']).agg({
        'metrics.rmse': ['mean', 'std', 'count']
    }).round(2)
    
    print("Hyperparameter analysis:")
    print(hyperparameter_analysis)

# Execute complete workflow
if __name__ == "__main__":
    complete_ml_workflow()

# --- 10. MLflow Best Practices ---
"""
🎯 Experiment Organization:
- Use consistent naming conventions
- Group related experiments 
- Add meaningful tags and descriptions

🎯 Model Registry:
- Version models systematically
- Use staging environments (None -> Staging -> Production)
- Document model changes and performance

🎯 Reproducibility: 
- Log all parameters, including data versions
- Use model signatures for input validation
- Store example inputs with models

🎯 Monitoring:
- Track production model performance
- Set up automated model validation
- Monitor for data and concept drift

🎯 Collaboration:
- Share experiments with team members
- Use clear run naming conventions
- Document model assumptions and limitations

Complete MLflow guide at jakublasak.com
"""