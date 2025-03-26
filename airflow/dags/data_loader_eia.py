from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import sys
import os
import requests
import json
from dateutil.relativedelta import relativedelta

from data_sources.data_loader import load_data_to_gcs

def notificar_slack(estado, **context):
    """
    Envía una notificación a Slack con el estado de ejecución del DAG.
    """
    mensaje = f":bell: *DAG {context['dag'].dag_id}* ha terminado con estado: *{estado}* 🚦\n"
    mensaje += f"Tarea: `{context['task_instance'].task_id}`\n"
    mensaje += f"Hora de ejecución: `{context['execution_date']}`"

    print(mensaje)

    url = "https://hooks.slack.com/services/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"
    headers = {"Content-Type": "application/json"}
    data = {"text": mensaje}

    response = requests.post(url, headers=headers, data=json.dumps(data))
    print(f"Slack Response: {response.status_code} - {response.text}")

# Callbacks para notificar éxito o fallo
def notificar_exito(context):
    notificar_slack("ÉXITO ✅", **context)

def notificar_fallo(context):
    notificar_slack("FALLÓ ❌", **context)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_success_callback": notificar_exito,
    "on_failure_callback": notificar_fallo,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "load_data_eia",
    default_args=default_args,
    description="Carga datos desde Storage a BigQuery",
    schedule_interval="@monthly",
    catchup=False,
)

# Definir fechas dinámicas para el año pasado (entre enero y diciembre)
hoy = datetime.today()
anio_pasado = hoy.year - 1
fecha_ini = f"{anio_pasado}-01"
fecha_fin = f"{anio_pasado}-12"

def ventas():
    load_data_to_gcs("eia", "ventas", fecha_ini, fecha_fin)

def prices_sales_volumes_stocks():
    load_data_to_gcs("eia", "prices_sales_volumes_stocks", fecha_ini, fecha_fin)

def crude_oil_production():
    load_data_to_gcs("eia", "crude_oil_production", fecha_ini, fecha_fin)

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

task_slack = PythonOperator(
    task_id="send_slack_notification",
    python_callable=lambda **context: notificar_slack("FINALIZADO 🎯", **context),
    provide_context=True,
    dag=dag,
    trigger_rule="all_done"  # Se ejecuta sin importar si las anteriores fallan o no
)

# Punto de fin (opcional)
end = DummyOperator(task_id="end", dag=dag)

# Configurar ejecución en paralelo
start >> [task_ventas, task_prices_sales_volumes_stocks, task_crude_oil_production] >> task_slack >> end
