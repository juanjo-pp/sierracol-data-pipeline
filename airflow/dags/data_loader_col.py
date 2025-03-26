from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.dummy_operator import DummyOperator
import requests
import json

# Importa la función de carga de datos
from data_sources.data_loader import load_data_to_gcs
from data_sources.bq_querys import ejecutar_rutinas


def notificar_slack(estado, **context):
    """
    Envía una notificación a Slack con el estado de ejecución del DAG.
    """
    mensaje = f":bell: *DAG {context['dag'].dag_id}* ha terminado con estado: *{estado}* 🚦\n"
    mensaje += f"Tarea: `{context['task_instance'].task_id}`\n"
    mensaje += f"Hora de ejecución: `{context['execution_date']}`"

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
    "on_success_callback": notificar_exito,  # Notificar si corre bien
    "on_failure_callback": notificar_fallo,  # Notificar si falla
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "load_data_col",
    default_args=default_args,
    description="Carga datos desde Storage a BigQuery",
    schedule_interval="@monthly",
    catchup=False,
)

def run_regalias():
    load_data_to_gcs("col", "Producci_n_y_Regal_as_por_Campo")


def run_fiscalizada():
    load_data_to_gcs("col", "Producci_n_Fiscalizada_de_Petr_leo")


def run_rutinas():
    ejecutar_rutinas()


start = DummyOperator(task_id="start", dag=dag)

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

task_rutinas = PythonOperator(
    task_id="run_rutinas",
    python_callable=run_rutinas,
    dag=dag,
)

# ✅ Nueva tarea para notificar a Slack al finalizar el DAG
task_slack = PythonOperator(
    task_id="send_slack_notification",
    python_callable=lambda **context: notificar_slack("FINALIZADO 🎯", **context),
    provide_context=True,  # Necesario para pasar `context`
    dag=dag,
    trigger_rule="all_done"  # Se ejecuta sin importar si las anteriores fallan o no
)

end = DummyOperator(task_id="end", dag=dag)

# ✅ Flujo corregido: Ahora `task_slack` se ejecuta siempre al final
start >> [task_regalias, task_fiscalizada] >> task_rutinas >> task_slack >> end
