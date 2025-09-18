#!/usr/bin/env python3
"""
Direct Airflow DAG for Three-Layer Architecture (Medallion) Pipeline
Uses direct Docker commands instead of Docker Compose
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import subprocess
import os

# Default arguments
default_args = {
    'owner': 'iot-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'iot-three-layered-pipeline',
    default_args=default_args,
    description='IoT Data Pipeline with Three-Layer Architecture',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    tags=['iot', 'data-pipeline', 'three-layer', 'analytics']
)

def run_iot_pipeline():
    """Run the IoT data pipeline using direct Docker command"""
    try:
        print("🚀 Starting IoT Data Pipeline...")
        
        # Run the IoT data pipeline using direct Docker command
        result = subprocess.run([
            'docker', 'exec', '-i', 'spark-master', 
            'python3', '/scripts/medallion_pipeline.py',
            '--input_path', '/data/landing_zone/IOT-temp.csv',
            '--output_db', 'postgres'
        ], capture_output=True, text=True, check=True)
        
        print("✅ IoT Data Pipeline completed successfully!")
        print("Output:", result.stdout)
        return result.stdout
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Pipeline failed: {e}")
        print("Error:", e.stderr)
        raise e

def check_data_quality():
    """Check data quality across all layers"""
    try:
        print("📊 Checking data quality...")
        
        result = subprocess.run([
            'docker', 'exec', '-i', 'postgres', 
            'psql', '-U', 'postgres', '-d', 'iot_data', '-c',
            """
            SELECT 
                'RawData' as layer, COUNT(*) as records FROM rawdata_iot_sensors
            UNION ALL
            SELECT 
                'CleanedData' as layer, COUNT(*) as records FROM cleaneddata_iot_sensors
            UNION ALL
            SELECT 
                'AnalyticsData' as layer, COUNT(*) as records FROM analyticsdata_device_metrics;
            """
        ], capture_output=True, text=True, check=True)
        
        print("📊 Data Quality Check Results:")
        print(result.stdout)
        return result.stdout
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Quality check failed: {e}")
        print("Error:", e.stderr)
        raise e

def generate_analytics_summary():
    """Generate analytics summary"""
    try:
        print("📈 Generating analytics summary...")
        
        result = subprocess.run([
            'docker', 'exec', '-i', 'postgres', 
            'psql', '-U', 'postgres', '-d', 'iot_data', '-c',
            """
            SELECT 
                'Device Analytics' as metric_type,
                COUNT(*) as total_devices,
                AVG(total_readings) as avg_readings_per_device
            FROM analyticsdata_device_metrics;
            """
        ], capture_output=True, text=True, check=True)
        
        print("📈 Analytics Summary:")
        print(result.stdout)
        return result.stdout
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Analytics summary failed: {e}")
        print("Error:", e.stderr)
        raise e

# Define tasks
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# IoT Data Pipeline Task
iot_pipeline_task = PythonOperator(
    task_id='run_iot_pipeline',
    python_callable=run_iot_pipeline,
    dag=dag
)

# Data Quality Check Task
quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,
    dag=dag
)

# Analytics Summary Task
analytics_summary_task = PythonOperator(
    task_id='analytics_summary',
    python_callable=generate_analytics_summary,
    dag=dag
)

# End task
end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Define task dependencies
start_task >> iot_pipeline_task >> quality_check_task >> analytics_summary_task >> end_task
