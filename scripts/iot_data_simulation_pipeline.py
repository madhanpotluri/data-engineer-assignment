#!/usr/bin/env python3
"""
Enhanced Medallion Pipeline with Checkpoint Support
Processes batch files from checkpoint processor
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with PostgreSQL support"""
    return SparkSession.builder \
        .appName("IoT-Medallion-Pipeline-Checkpoint") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .getOrCreate()

def create_dynamic_table_schema(spark, df, table_name, db_url, db_properties):
    """Create PostgreSQL table based on DataFrame schema with dynamic column addition"""
    try:
        # Get DataFrame schema
        schema = df.schema
        
        # Map Spark types to PostgreSQL types
        type_mapping = {
            "StringType": "TEXT",
            "IntegerType": "INTEGER", 
            "LongType": "BIGINT",
            "DoubleType": "DOUBLE PRECISION",
            "FloatType": "REAL",
            "BooleanType": "BOOLEAN",
            "TimestampType": "TIMESTAMP",
            "DateType": "DATE"
        }
        
        # Check if table exists and get current columns
        try:
            existing_df = spark.read \
                .format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", f"(SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}') as cols") \
                .option("user", db_properties["user"]) \
                .option("password", db_properties["password"]) \
                .load()
            
            existing_columns = [row[0] for row in existing_df.collect()]
        except:
            existing_columns = []
        
        # Build ALTER TABLE statements for new columns
        new_columns = []
        for field in schema.fields:
            if field.name not in existing_columns and field.name != "created_timestamp":
                spark_type = str(field.dataType)
                pg_type = type_mapping.get(spark_type, "TEXT")
                new_columns.append(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{field.name}" {pg_type}')
        
        # If table doesn't exist, create it
        if not existing_columns:
            columns = []
            for field in schema.fields:
                spark_type = str(field.dataType)
                pg_type = type_mapping.get(spark_type, "TEXT")
                columns.append(f'"{field.name}" {pg_type}')
            
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns)},
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # Execute via direct JDBC connection
            import psycopg2
            conn = psycopg2.connect(
                host="postgres",
                port=5432,
                database="iot_data", 
                user="postgres",
                password="postgres123"
            )
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Table created: {table_name}")
        else:
            # Add new columns if any
            if new_columns:
                import psycopg2
                conn = psycopg2.connect(
                    host="postgres",
                    port=5432,
                    database="iot_data",
                    user="postgres", 
                    password="postgres123"
                )
                cursor = conn.cursor()
                for alter_sql in new_columns:
                    try:
                        cursor.execute(alter_sql)
                        logger.info(f"✅ Added column: {alter_sql}")
                    except Exception as col_e:
                        logger.warning(f"⚠️  Column may already exist: {col_e}")
                conn.commit()
                cursor.close()
                conn.close()
                
                logger.info(f"✅ Table schema updated: {table_name}")
            else:
                logger.info(f"✅ Table schema verified: {table_name}")
        
    except Exception as e:
        logger.warning(f"⚠️  Table creation/update failed: {e}")

def rawdata_layer_processing(spark, batch_file_path, db_url, db_properties):
    """RawData Layer: Store data as-is with minimal processing"""
    logger.info("📥 Processing RawData Layer...")
    
    try:
        # Read batch file
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .option("escape", "\"") \
            .csv(batch_file_path)
        
        # Add layer metadata
        df_rawdata = df.withColumn("rawdata_layer_version", lit("1.0")) \
                      .withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("batch_file", lit(os.path.basename(batch_file_path)))
        
        # Create table schema
        create_dynamic_table_schema(spark, df_rawdata, "rawdata_iot_sensors", db_url, db_properties)
        
        # Write to PostgreSQL
        df_rawdata.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", "rawdata_iot_sensors") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .mode("append") \
            .save()
        
        logger.info(f"✅ RawData Layer completed: {df_rawdata.count()} records")
        return df_rawdata
        
    except Exception as e:
        logger.error(f"❌ RawData Layer failed: {e}")
        raise e

def cleaneddata_layer_processing(spark, df_rawdata, db_url, db_properties):
    """CleanedData Layer: Clean and validate data"""
    logger.info("🧹 Processing CleanedData Layer...")
    
    try:
        # Data cleaning and validation
        df_cleaned = df_rawdata.filter(
            col("temp").isNotNull() & 
            (col("temp") >= -50) & 
            (col("temp") <= 100) &
            col("room_id/id").isNotNull() &
            col("noted_date").isNotNull()
        )
        
        # Add cleaning metadata
        df_cleaneddata = df_cleaned.withColumn("cleaneddata_layer_version", lit("1.0")) \
                                  .withColumn("cleaning_timestamp", current_timestamp()) \
                                  .withColumn("data_quality_score", 
                                            when(col("temp").between(15, 35), 1.0)
                                            .when(col("temp").between(10, 40), 0.8)
                                            .otherwise(0.5))
        
        # Handle corrupt records if they exist
        if "_corrupt_record" in df_rawdata.columns:
            df_rejected = df_rawdata.filter(col("_corrupt_record").isNotNull()) \
                                  .withColumn("rejection_reason", lit("corrupt_record"))
            rejected_count = df_rejected.count()
        else:
            rejected_count = df_rawdata.count() - df_cleaneddata.count()
        
        # Create table schema
        create_dynamic_table_schema(spark, df_cleaneddata, "cleaneddata_iot_sensors", db_url, db_properties)
        
        # Write to PostgreSQL
        df_cleaneddata.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", "cleaneddata_iot_sensors") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .mode("append") \
            .save()
        
        logger.info(f"✅ CleanedData Layer completed: {df_cleaneddata.count()} records, {rejected_count} rejected")
        return df_cleaneddata, rejected_count
        
    except Exception as e:
        logger.error(f"❌ CleanedData Layer failed: {e}")
        raise e

def analyticsdata_layer_processing(spark, df_cleaneddata, db_url, db_properties):
    """AnalyticsData Layer: Business analytics and aggregations"""
    logger.info("📊 Processing AnalyticsData Layer...")
    
    try:
        # Device metrics
        df_device_metrics = df_cleaneddata.groupBy("room_id/id") \
            .agg(
                count("*").alias("reading_count"),
                avg("temp").alias("avg_temperature"),
                min("temp").alias("min_temperature"),
                max("temp").alias("max_temperature"),
                stddev("temp").alias("temp_stddev")
            ) \
            .withColumn("analyticsdata_layer_version", lit("1.0")) \
            .withColumn("created_timestamp", current_timestamp())
        
        # Location metrics
        df_location_metrics = df_cleaneddata.groupBy("out/in") \
            .agg(
                count("*").alias("reading_count"),
                avg("temp").alias("avg_temperature"),
                min("temp").alias("min_temperature"),
                max("temp").alias("max_temperature")
            ) \
            .withColumn("analyticsdata_layer_version", lit("1.0")) \
            .withColumn("created_timestamp", current_timestamp())
        
        # Time-based metrics
        df_time_metrics = df_cleaneddata.withColumn("hour", hour(to_timestamp(col("noted_date"), "yyyy-MM-dd HH:mm:ss"))) \
            .groupBy("hour") \
            .agg(
                count("*").alias("reading_count"),
                avg("temp").alias("avg_temperature")
            ) \
            .withColumn("analyticsdata_layer_version", lit("1.0")) \
            .withColumn("created_timestamp", current_timestamp())
        
        # Quality summary
        total_records = df_cleaneddata.count()
        quality_score = df_cleaneddata.agg(avg("data_quality_score")).collect()[0][0]
        
        df_quality_summary = spark.createDataFrame([{
            "total_records": total_records,
            "avg_quality_score": quality_score,
            "high_quality_records": df_cleaneddata.filter(col("data_quality_score") >= 0.8).count(),
            "medium_quality_records": df_cleaneddata.filter((col("data_quality_score") >= 0.5) & (col("data_quality_score") < 0.8)).count(),
            "low_quality_records": df_cleaneddata.filter(col("data_quality_score") < 0.5).count(),
            "analyticsdata_layer_version": "1.0",
            "created_timestamp": datetime.now()
        }])
        
        # Create tables and write data
        tables_data = [
            (df_device_metrics, "analyticsdata_device_metrics"),
            (df_location_metrics, "analyticsdata_location_metrics"),
            (df_time_metrics, "analyticsdata_time_metrics"),
            (df_quality_summary, "analyticsdata_quality_summary")
        ]
        
        for df, table_name in tables_data:
            create_dynamic_table_schema(spark, df, table_name, db_url, db_properties)
            df.write \
                .format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", table_name) \
                .option("user", db_properties["user"]) \
                .option("password", db_properties["password"]) \
                .mode("append") \
                .save()
        
        logger.info("✅ AnalyticsData Layer completed")
        return df_device_metrics, df_location_metrics, df_time_metrics, df_quality_summary
        
    except Exception as e:
        logger.error(f"❌ AnalyticsData Layer failed: {e}")
        raise e

def generate_pipeline_metadata(spark, batch_file_path, db_url, db_properties, 
                             rawdata_count, cleaneddata_count, rejected_count, 
                             processing_start_time, processing_end_time):
    """Generate comprehensive pipeline metadata for audit and reprocessing"""
    try:
        processing_duration = (processing_end_time - processing_start_time).total_seconds()
        success_rate = (cleaneddata_count / rawdata_count * 100) if rawdata_count > 0 else 0
        rejection_rate = (rejected_count / rawdata_count * 100) if rawdata_count > 0 else 0
        
        # Calculate data quality score
        quality_score = 0.8 if success_rate >= 90 else 0.6 if success_rate >= 70 else 0.4
        
        # Determine pipeline status
        pipeline_status = "SUCCESS" if rejected_count == 0 else "PARTIAL_SUCCESS"
        
        # Create metadata DataFrame
        metadata_data = [{
            "pipeline_run_id": f"run_{int(processing_start_time.timestamp())}",
            "input_file_name": os.path.basename(batch_file_path),
            "input_file_path": batch_file_path,
            "processing_start_time": processing_start_time,
            "processing_end_time": processing_end_time,
            "processing_duration_seconds": processing_duration,
            "rawdata_records_ingested": rawdata_count,
            "cleaneddata_records_processed": cleaneddata_count,
            "records_rejected": rejected_count,
            "success_rate_percent": success_rate,
            "rejection_rate_percent": rejection_rate,
            "pipeline_status": pipeline_status,
            "data_quality_score": quality_score,
            "created_timestamp": datetime.now()
        }]
        
        df_metadata = spark.createDataFrame(metadata_data)
        
        # Create metadata table
        create_dynamic_table_schema(spark, df_metadata, "pipeline_metadata", db_url, db_properties)
        
        # Write metadata
        df_metadata.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", "pipeline_metadata") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .mode("append") \
            .save()
        
        logger.info("✅ Pipeline metadata generated and stored")
        
    except Exception as e:
        logger.error(f"❌ Metadata generation failed: {e}")

def run_checkpoint_pipeline(batch_file_path, output_db="postgres"):
    """Run the three-layer architecture pipeline with checkpoint support"""
    logger.info("🚀 Starting Checkpoint-based Medallion Pipeline...")
    
    spark = None
    try:
        # Initialize Spark
        spark = create_spark_session()
        logger.info("✅ Spark session created")
        
        # Database configuration
        db_url = "jdbc:postgresql://postgres:5432/iot_data"
        db_properties = {
            "driver": "org.postgresql.Driver",
            "user": "postgres",
            "password": "postgres123"
        }
        
        # Track processing start time
        processing_start_time = datetime.now()
        
        # RawData Layer: Raw data ingestion
        df_rawdata = rawdata_layer_processing(spark, batch_file_path, db_url, db_properties)
        rawdata_count = df_rawdata.count()
        
        # CleanedData Layer: Cleaned data
        df_cleaneddata, rejected_count = cleaneddata_layer_processing(spark, df_rawdata, db_url, db_properties)
        cleaneddata_count = df_cleaneddata.count()
        
        # AnalyticsData Layer: Business analytics
        df_device_analytics, df_location_analytics, df_time_analytics, df_quality_summary = analyticsdata_layer_processing(spark, df_cleaneddata, db_url, db_properties)
        
        # Track processing end time
        processing_end_time = datetime.now()
        
        # Generate comprehensive pipeline metadata
        generate_pipeline_metadata(spark, batch_file_path, db_url, db_properties, 
                                 rawdata_count, cleaneddata_count, rejected_count, 
                                 processing_start_time, processing_end_time)
        
        logger.info("🎉 Checkpoint-based Medallion Pipeline completed successfully!")
        logger.info("📊 Data layers created:")
        logger.info("  📥 RawData: rawdata_iot_sensors (raw as-is)")
        logger.info("  🧹 CleanedData: cleaneddata_iot_sensors (cleaned & validated)")
        logger.info("  📊 AnalyticsData: analyticsdata_device_metrics, analyticsdata_location_metrics, analyticsdata_time_metrics, analyticsdata_quality_summary")
        logger.info("  📋 Pipeline Metadata: pipeline_metadata (audit & reprocessing info)")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        raise e
    finally:
        if spark:
            spark.stop()

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Checkpoint-based Medallion Pipeline")
    parser.add_argument("--batch-file", required=True, help="Path to batch CSV file")
    parser.add_argument("--output-db", default="postgres", help="Output database")
    
    args = parser.parse_args()
    
    run_checkpoint_pipeline(args.batch_file, args.output_db)

if __name__ == "__main__":
    main()
