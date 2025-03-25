from google.cloud import bigquery
import os

def delete_existing_data(client, dataset_id, table_id, period_normal, project_id):
    """
    Elimina los datos existentes en la tabla con el mismo period_normal antes de insertar nuevos.

    :param client: Cliente de BigQuery.
    :param dataset_id: Nombre del dataset en BigQuery.
    :param table_id: Nombre de la tabla en BigQuery.
    :param period_normal: Valor de la columna period_normal a eliminar.
    :param project_id: ID del proyecto en Google Cloud.
    """
    query = f"""
    DELETE FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE period_normal = '{period_normal}'
    """
    query_job = client.query(query)
    query_job.result()
    print(f"🗑️ Datos eliminados en {dataset_id}.{table_id} para period_normal = {period_normal}")

def load_file_to_bigquery(file_path, dataset_id, table_id, project_id):
    """
    Carga un archivo CSV o NDJSON en BigQuery después de eliminar duplicados.

    :param file_path: Ruta local del archivo a subir.
    :param dataset_id: Nombre del dataset en BigQuery.
    :param table_id: Nombre de la tabla en BigQuery.
    :param project_id: ID del proyecto en Google Cloud.
    """
    client = bigquery.Client(project=project_id)
    period_normal = file_path.split("_")[-1].split(".")[0]
    table_ref = client.dataset(dataset_id).table(table_id)

    # Configuración del formato del archivo
    if file_path.endswith(".csv"):
        source_format = bigquery.SourceFormat.CSV
        job_config = bigquery.LoadJobConfig(
            source_format=source_format,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        table_id = table_id + f'_{period_normal}'

    elif file_path.endswith(".ndjson"):


        try:
            client.get_table(table_ref)  # Si la tabla existe, intentamos eliminar los datos
            delete_existing_data(client, dataset_id, table_id, period_normal, project_id)
        except:
            print(f"La tabla {dataset_id}.{table_id} no existe. No se eliminó ninguna fila.")

        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config = bigquery.LoadJobConfig(
            source_format=source_format,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
    else:
        print(f"⚠️ Formato de archivo no compatible: {file_path}")
        return

    # Cargar el archivo a BigQuery
    with open(file_path, "rb") as file:
        load_job = client.load_table_from_file(file, f"{dataset_id}.{table_id}", job_config=job_config)

    load_job.result()  # Espera a que termine
    print(f"✅ Archivo {file_path} cargado en {dataset_id}.{table_id} en BigQuery.")
    os.remove(file_path)



if __name__ == "__main__":
    # Configuración
    PROJECT_ID = "juanpe-sierracol"
    DATASET_ID = "data_eia"

    # Lista de archivos a cargar
    files_to_upload = [
        "output/ndjson/prices_sales_volumes_stocks_2022-01.ndjson"
    ]

    # Cargar archivos en BigQuery después de eliminar duplicados
    for file_path in files_to_upload:
        table_name = "prices_sales_volumes_stocks"  # Usamos la misma tabla para todos los periodos
        load_file_to_bigquery(file_path, DATASET_ID, table_name, PROJECT_ID)
