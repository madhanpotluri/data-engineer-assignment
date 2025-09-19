#!/usr/bin/env python3
"""
Simple Batch Processor for IoT Data
Processes batch files and stores data in PostgreSQL without Spark
"""

import pandas as pd
import psycopg2
import os
import sys
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="iot_data",
            user="postgres",
            password="postgres123"
        )
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise e

def create_tables():
    """Create necessary tables if they don't exist"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # RawData table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rawdata_iot_sensors (
                id SERIAL PRIMARY KEY,
                "room_id/id" TEXT,
                noted_date TIMESTAMP,
                temp DOUBLE PRECISION,
                "out/in" TEXT,
                room TEXT,
                source_file TEXT,
                processing_timestamp TIMESTAMP,
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # CleanedData table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cleaneddata_iot_sensors (
                id SERIAL PRIMARY KEY,
                "room_id/id" TEXT,
                noted_date TIMESTAMP,
                temp DOUBLE PRECISION,
                "out/in" TEXT,
                room TEXT,
                source_file TEXT,
                processing_timestamp TIMESTAMP,
                cleaneddata_layer_version TEXT,
                cleaning_timestamp TIMESTAMP,
                data_quality_score DOUBLE PRECISION,
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Analytics tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analyticsdata_device_metrics (
                id SERIAL PRIMARY KEY,
                "room_id/id" TEXT,
                reading_count INTEGER,
                avg_temperature DOUBLE PRECISION,
                min_temperature DOUBLE PRECISION,
                max_temperature DOUBLE PRECISION,
                temp_stddev DOUBLE PRECISION,
                analyticsdata_layer_version TEXT,
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analyticsdata_location_metrics (
                id SERIAL PRIMARY KEY,
                "out/in" TEXT,
                reading_count INTEGER,
                avg_temperature DOUBLE PRECISION,
                min_temperature DOUBLE PRECISION,
                max_temperature DOUBLE PRECISION,
                analyticsdata_layer_version TEXT,
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analyticsdata_time_metrics (
                id SERIAL PRIMARY KEY,
                hour INTEGER,
                reading_count INTEGER,
                avg_temperature DOUBLE PRECISION,
                analyticsdata_layer_version TEXT,
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analyticsdata_quality_summary (
                id SERIAL PRIMARY KEY,
                total_records INTEGER,
                avg_quality_score DOUBLE PRECISION,
                high_quality_records INTEGER,
                medium_quality_records INTEGER,
                low_quality_records INTEGER,
                analyticsdata_layer_version TEXT,
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Pipeline metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metadata (
                id SERIAL PRIMARY KEY,
                pipeline_run_id TEXT,
                input_file_name TEXT,
                input_file_path TEXT,
                processing_start_time TIMESTAMP,
                processing_end_time TIMESTAMP,
                processing_duration_seconds DOUBLE PRECISION,
                rawdata_records_ingested INTEGER,
                cleaneddata_records_processed INTEGER,
                records_rejected INTEGER,
                success_rate_percent DOUBLE PRECISION,
                rejection_rate_percent DOUBLE PRECISION,
                pipeline_status TEXT,
                data_quality_score DOUBLE PRECISION,
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logger.info("✅ Database tables created/verified")
        
    except Exception as e:
        logger.error(f"❌ Table creation failed: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def process_rawdata_layer(batch_file_path):
    """Process RawData layer - store data as-is"""
    logger.info("📥 Processing RawData Layer...")
    
    # Read batch file
    df = pd.read_csv(batch_file_path)
    
    # Convert date format to PostgreSQL compatible format
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    
    # Add processing metadata
    df['processing_timestamp'] = datetime.now()
    df['source_file'] = os.path.basename(batch_file_path)
    
    # Store in database using direct SQL
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO rawdata_iot_sensors 
                ("room_id/id", noted_date, temp, "out/in", room, source_file, processing_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['room_id/id'],
                row['noted_date'],
                row['temp'],
                row['out/in'],
                row['room'],
                row['source_file'],
                row['processing_timestamp']
            ))
        conn.commit()
        logger.info(f"✅ RawData Layer completed: {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"❌ RawData Layer failed: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def process_cleaneddata_layer(df_rawdata):
    """Process CleanedData layer - clean and validate data"""
    logger.info("🧹 Processing CleanedData Layer...")
    
    # Data cleaning and validation
    df_cleaned = df_rawdata[
        (df_rawdata['temp'].notna()) & 
        (df_rawdata['temp'] >= -50) & 
        (df_rawdata['temp'] <= 100) &
        (df_rawdata['room_id/id'].notna()) &
        (df_rawdata['noted_date'].notna())
    ].copy()
    
    # Add cleaning metadata
    df_cleaned['cleaneddata_layer_version'] = '1.0'
    df_cleaned['cleaning_timestamp'] = datetime.now()
    df_cleaned['data_quality_score'] = df_cleaned['temp'].apply(
        lambda x: 1.0 if 15 <= x <= 35 else 0.8 if 10 <= x <= 40 else 0.5
    )
    
    # Calculate rejected records
    rejected_count = len(df_rawdata) - len(df_cleaned)
    
    # Store in database using direct SQL
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for _, row in df_cleaned.iterrows():
            cursor.execute("""
                INSERT INTO cleaneddata_iot_sensors 
                ("room_id/id", noted_date, temp, "out/in", room, source_file, processing_timestamp,
                 cleaneddata_layer_version, cleaning_timestamp, data_quality_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['room_id/id'],
                row['noted_date'],
                row['temp'],
                row['out/in'],
                row['room'],
                row['source_file'],
                row['processing_timestamp'],
                row['cleaneddata_layer_version'],
                row['cleaning_timestamp'],
                row['data_quality_score']
            ))
        conn.commit()
        logger.info(f"✅ CleanedData Layer completed: {len(df_cleaned)} records, {rejected_count} rejected")
        return df_cleaned, rejected_count
    except Exception as e:
        logger.error(f"❌ CleanedData Layer failed: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def process_analyticsdata_layer(df_cleaned):
    """Process AnalyticsData layer - business analytics"""
    logger.info("📊 Processing AnalyticsData Layer...")
    
    conn = get_db_connection()
    try:
        # Device metrics
        device_metrics = df_cleaned.groupby('room_id/id').agg({
            'temp': ['count', 'mean', 'min', 'max', 'std']
        }).round(2)
        device_metrics.columns = ['reading_count', 'avg_temperature', 'min_temperature', 'max_temperature', 'temp_stddev']
        device_metrics = device_metrics.reset_index()
        device_metrics['analyticsdata_layer_version'] = '1.0'
        device_metrics['created_timestamp'] = datetime.now()
        # Insert device metrics
        cursor = conn.cursor()
        for _, row in device_metrics.iterrows():
            cursor.execute("""
                INSERT INTO analyticsdata_device_metrics 
                ("room_id/id", reading_count, avg_temperature, min_temperature, max_temperature, temp_stddev, analyticsdata_layer_version, created_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['room_id/id'],
                int(row['reading_count']),
                float(row['avg_temperature']),
                float(row['min_temperature']),
                float(row['max_temperature']),
                float(row['temp_stddev']) if pd.notna(row['temp_stddev']) else 0.0,
                row['analyticsdata_layer_version'],
                row['created_timestamp']
            ))
        
        # Location metrics
        location_metrics = df_cleaned.groupby('out/in').agg({
            'temp': ['count', 'mean', 'min', 'max']
        }).round(2)
        location_metrics.columns = ['reading_count', 'avg_temperature', 'min_temperature', 'max_temperature']
        location_metrics = location_metrics.reset_index()
        location_metrics['analyticsdata_layer_version'] = '1.0'
        location_metrics['created_timestamp'] = datetime.now()
        
        for _, row in location_metrics.iterrows():
            cursor.execute("""
                INSERT INTO analyticsdata_location_metrics 
                ("out/in", reading_count, avg_temperature, min_temperature, max_temperature, analyticsdata_layer_version, created_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['out/in'],
                int(row['reading_count']),
                float(row['avg_temperature']),
                float(row['min_temperature']),
                float(row['max_temperature']),
                row['analyticsdata_layer_version'],
                row['created_timestamp']
            ))
        
        # Time metrics
        df_cleaned['hour'] = pd.to_datetime(df_cleaned['noted_date']).dt.hour
        time_metrics = df_cleaned.groupby('hour').agg({
            'temp': ['count', 'mean']
        }).round(2)
        time_metrics.columns = ['reading_count', 'avg_temperature']
        time_metrics = time_metrics.reset_index()
        time_metrics['analyticsdata_layer_version'] = '1.0'
        time_metrics['created_timestamp'] = datetime.now()
        
        for _, row in time_metrics.iterrows():
            cursor.execute("""
                INSERT INTO analyticsdata_time_metrics 
                (hour, reading_count, avg_temperature, analyticsdata_layer_version, created_timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                int(row['hour']),
                int(row['reading_count']),
                float(row['avg_temperature']),
                row['analyticsdata_layer_version'],
                row['created_timestamp']
            ))
        
        # Quality summary
        total_records = len(df_cleaned)
        avg_quality_score = df_cleaned['data_quality_score'].mean()
        high_quality = len(df_cleaned[df_cleaned['data_quality_score'] >= 0.8])
        medium_quality = len(df_cleaned[(df_cleaned['data_quality_score'] >= 0.5) & (df_cleaned['data_quality_score'] < 0.8)])
        low_quality = len(df_cleaned[df_cleaned['data_quality_score'] < 0.5])
        
        cursor.execute("""
            INSERT INTO analyticsdata_quality_summary 
            (total_records, avg_quality_score, high_quality_records, medium_quality_records, low_quality_records, analyticsdata_layer_version, created_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            int(total_records),
            float(round(avg_quality_score, 3)),
            int(high_quality),
            int(medium_quality),
            int(low_quality),
            '1.0',
            datetime.now()
        ))
        
        logger.info("✅ AnalyticsData Layer completed")
        
    except Exception as e:
        logger.error(f"❌ AnalyticsData Layer failed: {e}")
        raise e
    finally:
        conn.close()

def generate_pipeline_metadata(batch_file_path, rawdata_count, cleaneddata_count, rejected_count, 
                             processing_start_time, processing_end_time):
    """Generate pipeline metadata"""
    logger.info("📋 Generating pipeline metadata...")
    
    processing_duration = (processing_end_time - processing_start_time).total_seconds()
    success_rate = (cleaneddata_count / rawdata_count * 100) if rawdata_count > 0 else 0
    rejection_rate = (rejected_count / rawdata_count * 100) if rawdata_count > 0 else 0
    quality_score = 0.8 if success_rate >= 90 else 0.6 if success_rate >= 70 else 0.4
    pipeline_status = "SUCCESS" if rejected_count == 0 else "PARTIAL_SUCCESS"
    
    metadata = pd.DataFrame([{
        'pipeline_run_id': f"run_{int(processing_start_time.timestamp())}",
        'input_file_name': os.path.basename(batch_file_path),
        'input_file_path': batch_file_path,
        'processing_start_time': processing_start_time,
        'processing_end_time': processing_end_time,
        'processing_duration_seconds': float(round(processing_duration, 2)),
        'rawdata_records_ingested': int(rawdata_count),
        'cleaneddata_records_processed': int(cleaneddata_count),
        'records_rejected': int(rejected_count),
        'success_rate_percent': float(round(success_rate, 2)),
        'rejection_rate_percent': float(round(rejection_rate, 2)),
        'pipeline_status': pipeline_status,
        'data_quality_score': float(quality_score),
        'created_timestamp': datetime.now()
    }])
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO pipeline_metadata 
            (pipeline_run_id, input_file_name, input_file_path, processing_start_time, processing_end_time,
             processing_duration_seconds, rawdata_records_ingested, cleaneddata_records_processed, records_rejected,
             success_rate_percent, rejection_rate_percent, pipeline_status, data_quality_score, created_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(metadata['pipeline_run_id'].iloc[0]),
            str(metadata['input_file_name'].iloc[0]),
            str(metadata['input_file_path'].iloc[0]),
            metadata['processing_start_time'].iloc[0],
            metadata['processing_end_time'].iloc[0],
            float(metadata['processing_duration_seconds'].iloc[0]),
            int(metadata['rawdata_records_ingested'].iloc[0]),
            int(metadata['cleaneddata_records_processed'].iloc[0]),
            int(metadata['records_rejected'].iloc[0]),
            float(metadata['success_rate_percent'].iloc[0]),
            float(metadata['rejection_rate_percent'].iloc[0]),
            str(metadata['pipeline_status'].iloc[0]),
            float(metadata['data_quality_score'].iloc[0]),
            metadata['created_timestamp'].iloc[0]
        ))
        conn.commit()
        logger.info("✅ Pipeline metadata generated and stored")
    except Exception as e:
        logger.error(f"❌ Metadata generation failed: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def process_batch_file(batch_file_path):
    """Process a batch file through the three-layer architecture"""
    logger.info("🚀 Starting Simple Batch Processing Pipeline...")
    
    processing_start_time = datetime.now()
    
    try:
        # Create tables
        create_tables()
        
        # RawData Layer
        df_rawdata = process_rawdata_layer(batch_file_path)
        rawdata_count = len(df_rawdata)
        
        # CleanedData Layer
        df_cleaned, rejected_count = process_cleaneddata_layer(df_rawdata)
        cleaneddata_count = len(df_cleaned)
        
        # AnalyticsData Layer
        process_analyticsdata_layer(df_cleaned)
        
        # Generate metadata
        processing_end_time = datetime.now()
        generate_pipeline_metadata(batch_file_path, rawdata_count, cleaneddata_count, rejected_count,
                                 processing_start_time, processing_end_time)
        
        logger.info("🎉 Simple Batch Processing Pipeline completed successfully!")
        logger.info("📊 Data layers created:")
        logger.info("  📥 RawData: rawdata_iot_sensors (raw as-is)")
        logger.info("  🧹 CleanedData: cleaneddata_iot_sensors (cleaned & validated)")
        logger.info("  📊 AnalyticsData: analyticsdata_device_metrics, analyticsdata_location_metrics, analyticsdata_time_metrics, analyticsdata_quality_summary")
        logger.info("  📋 Pipeline Metadata: pipeline_metadata (audit & reprocessing info)")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise e

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Simple Batch Processor for IoT Data")
    parser.add_argument("--batch-file", required=True, help="Path to batch CSV file")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.batch_file):
        logger.error(f"❌ Batch file not found: {args.batch_file}")
        sys.exit(1)
    
    process_batch_file(args.batch_file)

if __name__ == "__main__":
    main()
