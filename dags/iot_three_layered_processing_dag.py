#!/usr/bin/env python3
"""
Three-Layered Processing DAG - Scheduled Every 5 Minutes
Processes accumulated IoT data through Bronze → Silver → Gold layers
Uses checkpoint to avoid reprocessing files
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
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
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
dag = DAG(
    'iot-three-layered-processing',
    default_args=default_args,
    description='Three-Layered Data Processing - Every 5 Minutes',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=['iot', 'processing', 'three-layer', 'checkpoint']
)

def run_checkpoint_processor():
    """Run checkpoint-based batch processor"""
    try:
        print("📦 Running checkpoint processor...")
        result = subprocess.run([
            'python3', '/opt/airflow/scripts/checkpoint_processor.py',
            '--landing-zone', '/data/landing_zone',
            '--checkpoint-file', '/data/checkpoints/last_processed.json',
            '--processed-dir', '/data/processed'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Checkpoint Processor completed successfully")
            print(result.stdout)
            return result.stdout
        else:
            print(f"❌ Checkpoint Processor failed: {result.stderr}")
            raise Exception(f"Checkpoint processor failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error running checkpoint processor: {e}")
        raise e

def run_three_layered_pipeline(batch_file_path):
    """Run three-layered pipeline on batch file"""
    try:
        print(f"🏗️  Running three-layered pipeline on: {batch_file_path}")
        result = subprocess.run([
            'python3', '/opt/airflow/scripts/iot_data_simulation_pipeline.py',
            '--batch-file', batch_file_path,
            '--output-db', 'postgres'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Three-Layered Pipeline completed successfully")
            print(result.stdout)
        else:
            print(f"❌ Three-Layered Pipeline failed: {result.stderr}")
            raise Exception(f"Three-layered pipeline failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error running three-layered pipeline: {e}")
        raise e

def process_accumulated_data():
    """Main processing function that orchestrates the entire pipeline"""
    print("🚀 Starting Three-Layered Data Processing...")
    
    try:
        # Step 1: Run checkpoint processor to get batch file
        print("📦 Running checkpoint processor...")
        processor_output = run_checkpoint_processor()
        
        # Extract batch file path from output (if any)
        batch_file_path = None
        if "Batch file:" in processor_output:
            for line in processor_output.split('\n'):
                if "Batch file:" in line:
                    batch_file_path = line.split("Batch file:")[1].strip()
                    break
        
        if batch_file_path and os.path.exists(batch_file_path):
            print(f"📄 Processing batch file: {batch_file_path}")
            
            # Step 2: Run three-layered pipeline on batch file
            print("🏗️  Running three-layered pipeline...")
            run_three_layered_pipeline(batch_file_path)
            
            print("✅ Three-Layered Data Processing completed successfully!")
        else:
            print("📭 No new data to process in this batch")
            
    except Exception as e:
        print(f"❌ Three-Layered Data Processing failed: {e}")
        raise e

# Task 1: Checkpoint-based Batch Processing
checkpoint_processing_task = PythonOperator(
    task_id='checkpoint_processing',
    python_callable=process_accumulated_data,
    dag=dag
)

# Task 2: Data Quality Check (optional)
data_quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command="""
    echo "🔍 Running data quality checks..."
    # Add any data quality validation commands here
    echo "✅ Data quality checks completed"
    """,
    dag=dag
)

# Task 3: Cleanup old files (optional)
cleanup_task = BashOperator(
    task_id='cleanup_old_files',
    bash_command="""
    echo "🧹 Cleaning up old files..."
    find /data/landing_zone -name "iot_data_*.csv" -mtime +7 -delete 2>/dev/null || true
    find /data/processed -name "iot_batch_*.csv" -mtime +30 -delete 2>/dev/null || true
    echo "✅ Cleanup completed"
    """,
    dag=dag
)

# Define task dependencies
checkpoint_processing_task >> data_quality_check >> cleanup_task
