#!/usr/bin/env python3
"""
Medallion Architecture Implementation for IoT Data Pipeline
Bronze (Raw) → Silver (Cleaned) → Gold (Analytics)
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, current_timestamp, when, isnan, isnull, avg, count, max, min, sum as spark_sum, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import argparse
import logging

def get_or_create_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .getOrCreate()
    return spark

def rawdata_layer_processing(spark, input_path, db_url, db_properties):
    """
    📥 RAWDATA LAYER: Raw data ingestion (as-is)
    - Store original data without any processing
    - Add metadata for tracking
    - Preserve all original columns and values
    """
    logger = logging.getLogger(__name__)
    logger.info("📥 RAWDATA LAYER: Ingesting raw data as-is...")
    
    # Read raw data with schema inference
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .csv(input_path)
    
    # Add metadata columns for tracking
    df_rawdata = df_raw \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("data_source", lit("iot_sensors")) \
        .withColumn("file_name", lit(os.path.basename(input_path))) \
        .withColumn("raw_record_id", col("id")) \
        .withColumn("rawdata_layer_version", lit("1.0"))
    
    # Store in rawdata table (raw data as-is)
    df_rawdata.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", "rawdata_iot_sensors") \
        .option("driver", db_properties["driver"]) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .mode("overwrite") \
        .save()
    
    rawdata_count = df_rawdata.count()
    logger.info(f"✅ Rawdata layer: {rawdata_count} raw records stored")
    return df_rawdata

def cleaneddata_layer_processing(spark, df_rawdata, db_url, db_properties):
    """
    🧹 CLEANEDDATA LAYER: Cleaned and validated data
    - Apply data quality rules
    - Standardize schemas
    - Clean and validate data
    - Business-ready format
    """
    logger = logging.getLogger(__name__)
    logger.info("🧹 CLEANEDDATA LAYER: Processing cleaned data...")
    
    # Data cleaning and validation
    # Check if _corrupt_record column exists before filtering
    if "_corrupt_record" in df_rawdata.columns:
        df_cleaneddata = df_rawdata.filter(col("_corrupt_record").isNull())
    else:
        df_cleaneddata = df_rawdata
    
    df_cleaneddata = df_cleaneddata \
        .withColumn("device_id", col("room_id/id")) \
        .withColumn("reading_time", 
            to_timestamp(col("noted_date"), "dd-MM-yyyy HH:mm")) \
        .withColumn("temperature", col("temp").cast("double")) \
        .withColumn("location_type", col("out/in")) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("data_quality_score", 
            when(col("temperature").isNotNull() & 
                 col("reading_time").isNotNull() & 
                 col("device_id").isNotNull(), 1.0)
            .otherwise(0.0)) \
        .withColumn("cleaneddata_layer_version", lit("1.0")) \
        .withColumn("is_valid", 
            when(col("temperature").isNotNull() & 
                 col("reading_time").isNotNull() & 
                 col("device_id").isNotNull() & 
                 ~isnan(col("temperature")), True)
            .otherwise(False)) \
        .select("raw_record_id", "device_id", "reading_time", "temperature", 
                "location_type", "processing_timestamp", "data_quality_score",
                "is_valid", "ingestion_timestamp", "data_source", 
                "file_name", "cleaneddata_layer_version")
    
    # Store in cleaneddata table
    df_cleaneddata.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", "cleaneddata_iot_sensors") \
        .option("driver", db_properties["driver"]) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .mode("overwrite") \
        .save()
    
    cleaneddata_count = df_cleaneddata.count()
    valid_count = df_cleaneddata.filter(col("is_valid") == True).count()
    logger.info(f"✅ Cleaneddata layer: {cleaneddata_count} records processed, {valid_count} valid")
    return df_cleaneddata

def analyticsdata_layer_processing(spark, df_cleaneddata, db_url, db_properties):
    """
    📊 ANALYTICSDATA LAYER: Business analytics and reporting
    - Aggregated metrics
    - Business KPIs
    - Analytics-ready data
    - Reporting tables
    """
    logger = logging.getLogger(__name__)
    logger.info("📊 ANALYTICSDATA LAYER: Creating business analytics...")
    
    # Filter only valid records for analytics
    df_valid = df_cleaneddata.filter(col("is_valid") == True)
    
    # 1. Device-level analytics
    df_device_analytics = df_valid \
        .groupBy("device_id") \
        .agg(
            count("temperature").alias("total_readings"),
            avg("temperature").alias("avg_temperature"),
            max("temperature").alias("max_temperature"),
            min("temperature").alias("min_temperature"),
            avg("data_quality_score").alias("avg_quality_score"),
            count(when(col("location_type") == "out", 1)).alias("outdoor_readings"),
            count(when(col("location_type") == "in", 1)).alias("indoor_readings")
        ) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("analyticsdata_layer_version", lit("1.0"))
    
    # 2. Location-based analytics
    df_location_analytics = df_valid \
        .groupBy("location_type") \
        .agg(
            count("temperature").alias("total_readings"),
            avg("temperature").alias("avg_temperature"),
            max("temperature").alias("max_temperature"),
            min("temperature").alias("min_temperature"),
            countDistinct("device_id").alias("unique_devices")
        ) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("analyticsdata_layer_version", lit("1.0"))
    
    # 3. Time-based analytics (hourly aggregates)
    df_time_analytics = df_valid \
        .withColumn("hour", col("reading_time").cast("string").substr(1, 13)) \
        .groupBy("hour", "location_type") \
        .agg(
            count("temperature").alias("readings_count"),
            avg("temperature").alias("avg_temperature"),
            max("temperature").alias("max_temperature"),
            min("temperature").alias("min_temperature")
        ) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("analyticsdata_layer_version", lit("1.0"))
    
    # 4. Data quality summary
    df_quality_summary = df_cleaneddata \
        .agg(
            count("*").alias("total_records"),
            count(when(col("is_valid") == True, 1)).alias("valid_records"),
            count(when(col("is_valid") == False, 1)).alias("invalid_records"),
            avg("data_quality_score").alias("overall_quality_score"),
            countDistinct("device_id").alias("unique_devices"),
            countDistinct("file_name").alias("source_files")
        ) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("analyticsdata_layer_version", lit("1.0"))
    
    # Store analyticsdata layer tables
    tables_to_create = [
        (df_device_analytics, "analyticsdata_device_metrics"),
        (df_location_analytics, "analyticsdata_location_metrics"),
        (df_time_analytics, "analyticsdata_time_metrics"),
        (df_quality_summary, "analyticsdata_quality_summary")
    ]
    
    for df, table_name in tables_to_create:
        df.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("driver", db_properties["driver"]) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .mode("overwrite") \
            .save()
        logger.info(f"✅ Created {table_name}")
    
    logger.info("✅ Analyticsdata layer: Business analytics tables created")
    return df_device_analytics, df_location_analytics, df_time_analytics, df_quality_summary

def run_three_layer_pipeline(input_path, output_db):
    """
    Complete Three Layer Architecture Pipeline
    RawData → CleanedData → AnalyticsData
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    spark = get_or_create_spark_session("IoT_Three_Layer_Pipeline")
    
    try:
        logger.info("🏗️ Starting Three Layer Architecture Pipeline...")
        
        # Database connection
        db_url = f"jdbc:postgresql://{output_db}:5432/iot_data"
        db_properties = {
            "driver": "org.postgresql.Driver",
            "user": "postgres",
            "password": "postgres123"
        }
        
        # RawData Layer: Raw data ingestion
        df_rawdata = rawdata_layer_processing(spark, input_path, db_url, db_properties)
        
        # CleanedData Layer: Cleaned data
        df_cleaneddata = cleaneddata_layer_processing(spark, df_rawdata, db_url, db_properties)
        
        # AnalyticsData Layer: Business analytics
        df_device_analytics, df_location_analytics, df_time_analytics, df_quality_summary = analyticsdata_layer_processing(spark, df_cleaneddata, db_url, db_properties)
        
        logger.info("🎉 Three Layer Architecture Pipeline completed successfully!")
        logger.info("📊 Data layers created:")
        logger.info("  📥 RawData: rawdata_iot_sensors (raw as-is)")
        logger.info("  🧹 CleanedData: cleaneddata_iot_sensors (cleaned & validated)")
        logger.info("  📊 AnalyticsData: analyticsdata_device_metrics, analyticsdata_location_metrics, analyticsdata_time_metrics, analyticsdata_quality_summary")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", help="Path to the input data directory.")
    parser.add_argument("--output_db", help="Name of the database service.")
    args = parser.parse_args()

    run_three_layer_pipeline(args.input_path, args.output_db)
