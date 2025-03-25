from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Agregar el path del proyecto
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/../../sierracol-data-pipeline/data_sources"))

from data_sources.data_loader import load_json_to_gcs

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "load_data_to_bq",
    default_args=default_args,
    description="Carga datos desde Storage a BigQuery",
    schedule_interval="@daily",  # Corre diariamente
    catchup=False,
)

def run_eia():
    load_json_to_gcs("eia", "ventas", "2022-01-01", "2022-12-31")

def run_col():
    load_json_to_gcs("col", "produccion")

task_eia = PythonOperator(
    task_id="load_eia_data",
    python_callable=run_eia,
    dag=dag,
)

task_col = PythonOperator(
    task_id="load_col_data",
    python_callable=run_col,
    dag=dag,
)

task_eia >> task_col  # Primero corre EIA, luego Colombia
