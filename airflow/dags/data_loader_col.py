from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # Agrega la ruta del DAG
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/data_sources"))  # Agrega data_sources

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
    "load_data_col",
    default_args=default_args,
    description="Carga datos desde Storage a BigQuery",
    schedule_interval="@daily",  # Corre diariamente
    catchup=False,
)

def run_regalias():
    load_json_to_gcs("col", "Producci_n_y_Regal_as_por_Campo")

def run_fiscalizada():
    load_json_to_gcs("col", "Producci_n_Fiscalizada_de_Petr_leo")

task_regalias = PythonOperator(
    task_id="load_col_regalias",
    python_callable=run_regalias,
    dag=dag,
)

task_fiscalizada = PythonOperator(
    task_id="load_col_fiscalizada",
    python_callable=run_fiscalizada,
    dag=dag,
)

task_regalias >> task_fiscalizada  # Primero corre EIA, luego Colombia
