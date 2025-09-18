# dags/iot_data_pipeline.py

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Define the DAG's arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the Spark connection ID and app path
SPARK_CONN_ID = 'spark_default'
ELT_APP_PATH = '/opt/spark/scripts/elt_job.py'

with DAG(
    dag_id='iot_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['iot', 'elt', 'spark']
) as dag:
    
    # Task 1: Check for new files in the landing zone
    check_for_new_files = BashOperator(
        task_id='check_for_new_files',
        bash_command='ls /data/landing_zone | wc -l > /tmp/file_count'
    )

    # Task 2: Run the ELT job using Spark
    run_elt_job = SparkSubmitOperator(
        task_id='run_elt_job',
        conn_id=SPARK_CONN_ID,
        application=ELT_APP_PATH,
        name='IoT_Data_ELT_Job',
        application_args=[
            '--input_path', '/data/landing_zone',
            '--output_db', 'postgres'
        ],
        conf={
            'spark.driver.extraClassPath': '/opt/spark/jars/postgresql-42.5.0.jar',
            'spark.executor.extraClassPath': '/opt/spark/jars/postgresql-42.5.0.jar'
        },
        driver_memory='2g',
        executor_memory='3g',
        num_executors=2
    )

    # Task 3: Create the final curated table for reporting
    create_final_table = PostgresOperator(
        task_id='create_final_table',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS final_reporting_data AS
            SELECT
                device_id,
                DATE(reading_time) as reading_date,
                AVG(reading_value) as average_temp,
                COUNT(*) as readings_count
            FROM clean_readings
            GROUP BY device_id, reading_date;
        """
    )
    
    check_for_new_files >> run_elt_job >> create_final_table