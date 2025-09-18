# scripts/elt_job.py

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, current_timestamp, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import argparse
import logging

# Function to get or create Spark Session
def get_or_create_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .getOrCreate()
    return spark

def create_dynamic_table_schema(df, table_name, db_url, db_properties, spark):
    """
    Dynamically create database table based on DataFrame schema
    This makes the system robust to schema changes (like Databricks)
    """
    logger = logging.getLogger(__name__)
    
    # Get DataFrame schema
    schema = df.schema
    
    # Build CREATE TABLE statement dynamically
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    
    column_definitions = []
    for field in schema.fields:
        field_name = field.name
        field_type = field.dataType
        
        # Map Spark types to PostgreSQL types
        if field_type == StringType():
            pg_type = "VARCHAR(255)"
        elif field_type == DoubleType():
            pg_type = "DOUBLE PRECISION"
        elif field_type == TimestampType():
            pg_type = "TIMESTAMP"
        elif field_type == IntegerType():
            pg_type = "INTEGER"
        else:
            pg_type = "TEXT"  # Fallback for unknown types
        
        column_definitions.append(f"{field_name} {pg_type}")
    
    create_table_sql += ", ".join(column_definitions) + ")"
    
    logger.info(f"Creating table {table_name} with dynamic schema:")
    logger.info(create_table_sql)
    
    # Execute CREATE TABLE statement
    try:
        # Use Spark SQL to create table
        df.createOrReplaceTempView("temp_df")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING jdbc OPTIONS (url='{db_url}', dbtable='{table_name}', user='{db_properties['user']}', password='{db_properties['password']}') AS SELECT * FROM temp_df WHERE 1=0")
        logger.info(f"✅ Table {table_name} created successfully")
    except Exception as e:
        logger.warning(f"⚠️  Table creation failed (may already exist): {e}")
        # Continue with the process as table might already exist

# Main ELT function
def run_elt_job(input_path, output_db):
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    spark = get_or_create_spark_session("IoT_Data_ELT_Job")
    
    try:
        logger.info(f"Starting ELT job for input path: {input_path}")
        
        # 1. READ RAW DATA (Extraction)
        logger.info("Step 1: Reading raw data with schema inference...")
        
        # Use schema inference for robust data handling (like Databricks)
        # This automatically adapts to new columns and data types
        df_raw = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .option("multiline", "true") \
            .option("escape", "\"") \
            .csv(input_path)
        
        # Log the inferred schema
        logger.info("Inferred schema:")
        df_raw.printSchema()
        
        logger.info(f"Raw data loaded: {df_raw.count()} records")
        
        # 2. TRANSFORM DATA (Transformation)
        logger.info("Step 2: Transforming data with dynamic column mapping...")
        
        # Get all column names for dynamic processing
        all_columns = df_raw.columns
        logger.info(f"Available columns: {all_columns}")
        
        # Dynamic column mapping (handles different column names)
        # This makes the system robust to schema changes
        column_mapping = {
            'room_id/id': 'device_id',
            'room_id': 'device_id', 
            'noted_date': 'reading_time',
            'temp': 'temperature',
            'out/in': 'location_type',
            'out_in': 'location_type'
        }
        
        # Apply column mapping dynamically
        df_mapped = df_raw
        for old_col, new_col in column_mapping.items():
            if old_col in all_columns:
                df_mapped = df_mapped.withColumnRenamed(old_col, new_col)
                logger.info(f"Mapped column: {old_col} -> {new_col}")
        
        # Clean and transform the data with flexible column handling
        df_transformed = df_mapped \
            .withColumn("reading_time", 
                when(col("reading_time").isNotNull(), 
                     to_timestamp(col("reading_time"), "dd-MM-yyyy HH:mm"))
                .otherwise(col("reading_time"))) \
            .withColumn("temperature", 
                when(col("temperature").isNotNull(), 
                     col("temperature").cast("double"))
                .otherwise(col("temperature"))) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("original_id", col("id"))
        
        # Check if _corrupt_record column exists before selecting it
        if "_corrupt_record" in df_transformed.columns:
            df_transformed = df_transformed.select("original_id", "device_id", "reading_time", "temperature", "location_type", "processing_timestamp", "_corrupt_record")
        else:
            df_transformed = df_transformed.select("original_id", "device_id", "reading_time", "temperature", "location_type", "processing_timestamp")
        
        # Split into clean and rejected dataframes
        if "_corrupt_record" in df_transformed.columns:
            df_clean = df_transformed.filter(
                col("_corrupt_record").isNull() & 
                col("temperature").isNotNull() & 
                ~isnan(col("temperature")) &
                col("reading_time").isNotNull()
            ).drop("_corrupt_record")
            
            df_rejected = df_transformed.filter(
                col("_corrupt_record").isNotNull() | 
                col("temperature").isNull() | 
                isnan(col("temperature")) |
                col("reading_time").isNull()
            )
        else:
            # No corrupt records column, filter based on data quality only
            df_clean = df_transformed.filter(
                col("temperature").isNotNull() & 
                ~isnan(col("temperature")) &
                col("reading_time").isNotNull()
            )
            
            df_rejected = df_transformed.filter(
                col("temperature").isNull() | 
                isnan(col("temperature")) |
                col("reading_time").isNull()
            )
        
        # Add rejection reason
        if "_corrupt_record" in df_transformed.columns:
            df_rejected = df_rejected.withColumn("rejection_reason", 
                when(col("_corrupt_record").isNotNull(), "Corrupt record")
                .when(col("temperature").isNull(), "Missing temperature")
                .when(isnan(col("temperature")), "Invalid temperature value")
                .when(col("reading_time").isNull(), "Invalid timestamp")
                .otherwise("Unknown error")
            )
        else:
            df_rejected = df_rejected.withColumn("rejection_reason", 
                when(col("temperature").isNull(), "Missing temperature")
                .when(isnan(col("temperature")), "Invalid temperature value")
                .when(col("reading_time").isNull(), "Invalid timestamp")
                .otherwise("Unknown error")
            )
        
        clean_count = df_clean.count()
        rejected_count = df_rejected.count()
        
        logger.info(f"Data transformation completed:")
        logger.info(f"  - Clean records: {clean_count}")
        logger.info(f"  - Rejected records: {rejected_count}")
        
        # 3. WRITE TO DATABASE (Loading)
        logger.info("Step 3: Loading data to database with dynamic schema...")
        
        # Database connection details
        db_url = f"jdbc:postgresql://{output_db}:5432/iot_data"
        db_properties = {
            "driver": "org.postgresql.Driver",
            "user": "postgres",
            "password": "postgres123"
        }
        
        # Load clean data into 'iot_temperature_readings' table
        if clean_count > 0:
            # Create table dynamically based on inferred schema
            create_dynamic_table_schema(df_clean, "iot_temperature_readings", db_url, db_properties, spark)
            
            # Write data with schema evolution support
            df_clean.write \
                .format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", "iot_temperature_readings") \
                .option("driver", db_properties["driver"]) \
                .option("user", db_properties["user"]) \
                .option("password", db_properties["password"]) \
                .option("truncate", "false") \
                .mode("append") \
                .save()
            logger.info(f"✅ {clean_count} clean records loaded to iot_temperature_readings table")
        
        # Load rejected records into 'rejected_temperature_readings' table
        if rejected_count > 0:
            df_rejected.write \
                .format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", "rejected_temperature_readings") \
                .option("driver", db_properties["driver"]) \
                .option("user", db_properties["user"]) \
                .option("password", db_properties["password"]) \
                .mode("append") \
                .save()
            logger.info(f"⚠️  {rejected_count} rejected records loaded to rejected_temperature_readings table")
        
        # 4. GENERATE METADATA/SUMMARY
        logger.info("Step 4: Generating processing summary...")
        
        summary = {
            "total_records_processed": clean_count + rejected_count,
            "successful_records": clean_count,
            "rejected_records": rejected_count,
            "success_rate": (clean_count / (clean_count + rejected_count) * 100) if (clean_count + rejected_count) > 0 else 0,
            "processing_timestamp": current_timestamp(),
            "input_file": input_path
        }
        
        logger.info("📊 Processing Summary:")
        logger.info(f"  - Total records: {summary['total_records_processed']}")
        logger.info(f"  - Successful: {summary['successful_records']}")
        logger.info(f"  - Rejected: {summary['rejected_records']}")
        logger.info(f"  - Success rate: {summary['success_rate']:.2f}%")
        
        logger.info("✅ ELT job completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ ELT job failed: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", help="Path to the input data directory.")
    parser.add_argument("--output_db", help="Name of the database service.")
    args = parser.parse_args()

    run_elt_job(args.input_path, args.output_db)