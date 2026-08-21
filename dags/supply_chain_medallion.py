from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging
import os
import sys

# Add opt/airflow to path so our local modules can be imported
sys.path.append('/opt/airflow')

from pipelines.Project_Batch_Process import main as batch_process_main
from pipelines.Project_Silver_To_SQL import main as sql_load_main


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'supply_chain_medallion_pipeline',
    default_args=default_args,
    description='End-to-end analytical data engineering workflow',
    schedule_interval=None, # Triggered manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['medallion', 'supply_chain'],
) as dag:

    # TASK 1: Clean Raw Kaggle Data (Local execution via Bash)
    # We use BashOperator here because prep_raw_data uses argparse
    clean_raw_data = BashOperator(
        task_id='prep_raw_data',
        bash_command='python /opt/airflow/data_scaling/prep_raw_data.py -i /opt/airflow/DataCoSupplyChainDataset.csv -o /opt/airflow/model_ready.csv',
    )

    # TASK 2: Wait for SDV Simulation (Simulated)
    # In a real environment, this would be an S3KeySensor waiting for Colab to upload to Bronze.
    # For this test, we simulate Colab by just pushing the cleaned data straight to Bronze MinIO.
    simulate_sdv_upload = BashOperator(
        task_id='simulate_sdv_upload_to_bronze',
        bash_command='''
        pip install s3cmd && \
        s3cmd --host=http://minio:9000 --host-bucket=http://minio:9000 --no-ssl --access_key=admin --secret_key=password123 put /opt/airflow/model_ready.csv s3://data-lake/bronze/DataCo_Synthetic_Simulated.csv
        ''',
    )

    # TASK 3: Bronze to Silver (Polars Transformations)
    transform_bronze_to_silver = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=batch_process_main,
    )

    # TASK 4: Silver to SQL Server
    load_silver_to_sql = PythonOperator(
        task_id='load_silver_to_sql',
        python_callable=sql_load_main,
    )


    # TASK 0: Bootstrap Dimension Tables into MinIO
    bootstrap_dimensions = BashOperator(
        task_id='bootstrap_dimensions',
        bash_command='python /opt/airflow/scripts/bootstrap_dimensions.py',
    )

    # Define execution graph
    bootstrap_dimensions >> clean_raw_data >> simulate_sdv_upload >> transform_bronze_to_silver >> load_silver_to_sql
