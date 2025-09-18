# scripts/elt_job.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, current_timestamp
import argparse

# Function to get or create Spark Session
def get_or_create_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .getOrCreate()
    return spark

# Main ELT function
def run_elt_job(input_path, output_db):
    spark = get_or_create_spark_session("IoT_Data_ELT_Job")
    
    # 1. READ RAW DATA (Extraction)
    # Use schema inference and permissive mode to handle anomalies
    df_raw = spark.read.option("header", "true").csv(input_path)

    # 2. TRANSFORM DATA (Transformation)
    # Define a rescue column to capture bad records (similar to Databricks)
    df_transformed = df_raw.withColumn("_corrupt_record", lit(None).cast("string"))

    # Split into clean and rejected dataframes
    df_clean = df_transformed.filter(col("reading_value").isNotNull()).drop("_corrupt_record")
    df_rejected = df_transformed.filter(col("reading_value").isNull())

    # 3. WRITE TO DATABASE (Loading)
    # Load clean data into a 'clean_readings' table
    df_clean.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{output_db}:5432/airflow") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "clean_readings") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("append") \
        .save()

    # Load rejected records into a 'rejected_records' table
    df_rejected.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{output_db}:5432/airflow") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "rejected_records") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("append") \
        .save()
        
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", help="Path to the input data directory.")
    parser.add_argument("--output_db", help="Name of the database service.")
    args = parser.parse_args()

    run_elt_job(args.input_path, args.output_db)