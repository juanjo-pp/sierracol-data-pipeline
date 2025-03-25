from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import sys
import os
from dateutil.relativedelta import relativedelta

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
    "load_data_eia",
    default_args=default_args,
    description="Carga datos desde Storage a BigQuery",
    schedule_interval="@daily",
    catchup=False,
)

hoy = datetime.today()
fecha_ini = (hoy - relativedelta(years=1, months=1)).strftime("%Y-%m")
fecha_fin = (hoy.replace(year=hoy.year - 1)).strftime("%Y-%m")

def ventas():
    load_json_to_gcs("eia", "ventas", fecha_ini, fecha_fin)

def prices_sales_volumes_stocks():
    load_json_to_gcs("eia", "prices_sales_volumes_stocks", fecha_ini, fecha_fin)

def crude_oil_production():
    load_json_to_gcs("eia", "crude_oil_production", fecha_ini, fecha_fin)

# Punto de inicio (opcional)
start = DummyOperator(task_id="start", dag=dag)

task_ventas = PythonOperator(
    task_id="load_ventas",
    python_callable=ventas,
    dag=dag,
)

task_prices_sales_volumes_stocks = PythonOperator(
    task_id="load_prices_sales_volumes_stocks",
    python_callable=prices_sales_volumes_stocks,
    dag=dag,
)

task_crude_oil_production = PythonOperator(
    task_id="load_crude_oil_production",
    python_callable=crude_oil_production,
    dag=dag,
)

# Punto de fin (opcional)
end = DummyOperator(task_id="end", dag=dag)

# Configurar ejecución en paralelo
start >> [task_ventas, task_prices_sales_volumes_stocks, task_crude_oil_production] >> end
