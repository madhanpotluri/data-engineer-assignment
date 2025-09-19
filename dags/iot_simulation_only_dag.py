#!/usr/bin/env python3
"""
IoT Data Simulation DAG - Manual Trigger Only
Generates IoT data every 5 seconds into landing zone
Can be started/stopped manually in Airflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess
import time

# Default arguments
default_args = {
    'owner': 'iot-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # No retries for simulation
    'retry_delay': timedelta(minutes=1),
}

# DAG definition - NO SCHEDULE (manual trigger only)
dag = DAG(
    'iot-simulation-only',
    default_args=default_args,
    description='IoT Data Simulation - Manual Start/Stop',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['iot', 'simulation', 'manual']
)

def run_iot_simulator():
    """Run IoT data simulator continuously until stopped"""
    try:
        print("🚀 Starting IoT Data Simulation...")
        print("📝 Generating data every 5 seconds into landing zone")
        print("⏹️  Stop this task in Airflow to stop simulation")
        
        # Run simulator continuously (no duration limit)
        result = subprocess.run([
            'python3', '/opt/airflow/scripts/iot_simulator.py',
            '--output-dir', '/data/landing_zone',
            '--interval', '5'
            # No --duration parameter = runs until stopped
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ IoT Simulator completed successfully")
            print(result.stdout)
        else:
            print(f"❌ IoT Simulator failed: {result.stderr}")
            raise Exception(f"Simulator failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error running IoT simulator: {e}")
        raise e

# Single task for IoT simulation
iot_simulation_task = PythonOperator(
    task_id='run_iot_simulation',
    python_callable=run_iot_simulator,
    dag=dag
)
