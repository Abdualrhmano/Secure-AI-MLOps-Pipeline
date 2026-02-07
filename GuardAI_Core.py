import logging
import mlflow
import mlflow.sklearn
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, regexp_replace, count, when
from pyspark.sql.types import IntegerType, DoubleType
from sklearn.tree import DecisionTreeRegressor
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

# Initialize Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DiabetesMLPipeline:
    def __init__(self, config):
        self.config = config
        self.spark = self._get_or_create_spark()
        
    def _get_or_create_spark(self):
        """Creates a Spark session adaptable to different environments."""
        try:
            return SparkSession.builder.getOrCreate()
        except Exception as e:
            logger.error(f"Failed to initialize Spark Session: {e}")
            raise

    def secure_ingestion(self, path):
        """Step 1: Secure Data Ingestion & Anonymization"""
        logger.info("Starting Secure Data Ingestion...")
        df = self.spark.read.format("delta").load(path)
        
        # PII Protection: Hashing Patient IDs (ISC2 Standard)
        if "Patient_ID" in df.columns:
            df = df.withColumn("Patient_ID", sha2(col("Patient_ID").cast("string"), 256))
        
        # Sanitization: Clean string columns from potential injection characters
        for column in df.columns:
            if df.schema[column].dataType == "string":
                df = df.withColumn(column, regexp_replace(col(column), r"[<>/'\"&]", ""))
        
        # Schema Enforcement
        return df.withColumn("S1", col("S1").cast(IntegerType())) \
                 .withColumn("S2", col("S2").cast(DoubleType())) \
                 .na.fill(0)

    def validate_quality(self, df):
        """Step 2: Data Quality Audit"""
        logger.info("Executing Data Quality Audit...")
        initial_count = df.count()
        df = df.dropDuplicates()
        logger.info(f"Audit Complete: Removed {initial_count - df.count()} duplicates.")
        return df

    def train_and_register(self, df):
        """Step 3: Model Training & MLOps Tracking"""
        logger.info("Training Model with MLflow Tracking...")
        mlflow.set_experiment(self.config['experiment_name'])
        
        # Feature Selection
        features = ["AGE","SEX","BMI","BP","S1","S2","S3","S4","S5","S6"]
        pdf = df.select(features + ["target"]).toPandas()
        X, y = pdf[features], pdf["target"]

        # Define Schema Signature for Model Governance
        input_schema = Schema([ColSpec("integer", f) if f in ["AGE", "SEX", "S1", "S6"] 
                               else ColSpec("double", f) for f in features])
        signature = ModelSignature(inputs=input_schema, outputs=Schema([ColSpec("double")]))

        with mlflow.start_run() as run:
            mlflow.autolog(log_models=False)
            model = DecisionTreeRegressor(max_depth=self.config['max_depth'])
            model.fit(X, y)
            
            # Log Model with Signature
            mlflow.sklearn.log_model(
                sk_model=model, 
                artifact_path="model", 
                signature=signature,
                registered_model_name=self.config['model_name']
            )
            logger.info(f"Model registered. Run ID: {run.info.run_id}")
            return run.info.run_id

# ==========================================
# MAIN EXECUTION PIPELINE
# ==========================================
if __name__ == "__main__":
    # Centralized Configuration
    pipeline_config = {
        'input_path': "Tables/new_table",
        'output_path': "Tables/final_predictions",
        'experiment_name': "Secure_Diabetes_Analytics",
        'model_name': "Diabetes_Pro_Model",
        'max_depth': 5
    }

    pipeline = DiabetesMLPipeline(pipeline_config)
    
    try:
        # Run Pipeline
        raw_data = pipeline.secure_ingestion(pipeline_config['input_path'])
        clean_data = pipeline.validate_quality(raw_data)
        run_id = pipeline.train_and_register(clean_data)
        
        logger.info("✨ Full Enterprise Pipeline executed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Critical Pipeline Failure: {e}")


