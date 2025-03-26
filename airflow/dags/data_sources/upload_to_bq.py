from google.cloud import bigquery
import os


def delete_existing_data(client: bigquery.Client, dataset_id: str, table_id: str, period_normal: str, project_id: str):
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


def load_file_to_bigquery(file_path: str, dataset_id: str, table_id: str, project_id: str):
    """
    Carga un archivo NDJSON en BigQuery después de eliminar duplicados.

    :param file_path: Ruta local del archivo a subir.
    :param dataset_id: Nombre del dataset en BigQuery.
    :param table_id: Nombre de la tabla en BigQuery.
    :param project_id: ID del proyecto en Google Cloud.
    """
    client = bigquery.Client(project=project_id)
    period_normal = file_path.split("_")[-1].split(".")[0]
    table_ref = client.dataset(dataset_id).table(table_id)

    if file_path.endswith(".ndjson"):
        try:
            client.get_table(table_ref)  # Verifica si la tabla existe
            delete_existing_data(client, dataset_id, table_id, period_normal, project_id)
        except Exception:
            print(f"⚠️ La tabla {dataset_id}.{table_id} no existe. No se eliminó ninguna fila.")

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


def load_df_to_bigquery(json_data: dict, dataset_id: str, table_id: str, project_id: str):
    """
    Carga un DataFrame en BigQuery con la opción de truncar la tabla.

    :param json_data: Diccionario con el DataFrame a cargar.
    :param dataset_id: Nombre del dataset en BigQuery.
    :param table_id: Nombre de la tabla en BigQuery.
    :param project_id: ID del proyecto en Google Cloud.
    """
    client = bigquery.Client(project=project_id)
    key = list(json_data.keys())[0]
    period_normal = key.split("_")[-1].split(".")[0]
    table_ref = f"{project_id}.{dataset_id}.{table_id}{period_normal}"

    job = client.load_table_from_dataframe(
        json_data[key],
        table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()  # Espera a que termine
    print(f"✅ Datos subidos a {table_ref}")

