import os
import io
import pandas as pd
from google.cloud import storage

# Configuración de GCS y BigQuery
GCS_BUCKET_NAME = "sierracol-data-storage"
GCS_FOLDER_PATH = "col/"  # Asegúrate de que termine con "/"


def list_gcs_files(bucket_name: str, folder_path: str, prefix: str) -> list:
    """
    Lista los archivos en un bucket GCS dentro de una carpeta específica que
    comienzan con un prefijo dado y terminan en '.csv'.

    :param bucket_name: Nombre del bucket en GCS.
    :param folder_path: Ruta de la carpeta dentro del bucket.
    :param prefix: Prefijo que deben tener los archivos.
    :return: Lista de nombres de archivos en GCS.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)

    return [blob.name for blob in blobs if
            blob.name.startswith(f"{folder_path}{prefix}") and blob.name.endswith(".csv")]


def read_gcs_csv(bucket_name: str, file_path: str) -> pd.DataFrame:
    """
    Lee un archivo CSV almacenado en GCS y lo convierte en un DataFrame de Pandas.

    :param bucket_name: Nombre del bucket en GCS.
    :param file_path: Ruta completa del archivo en GCS.
    :return: DataFrame con los datos del CSV.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    content = blob.download_as_bytes()  # Descarga el archivo como bytes
    return pd.read_csv(io.BytesIO(content))  # Carga en Pandas


def preparation_before_upload(file_name: str, df: pd.DataFrame) -> tuple:
    """
    Realiza modificaciones al DataFrame antes de subirlo a BigQuery.

    - Extrae el nombre base del archivo.
    - Normaliza la columna 'period_normal' desde el nombre del archivo.
    - Renombra las columnas a minúsculas y reemplaza caracteres especiales.

    :param file_name: Nombre del archivo procesado.
    :param df: DataFrame con los datos originales.
    :return: Tuple con el nuevo nombre del archivo y el DataFrame modificado.
    """
    new_name = file_name[4:-13]  # Extraer parte relevante del nombre
    df["period_normal"] = new_name[-4:]  # Agregar columna de periodo

    # Normalizar nombres de columnas
    df.columns = df.columns.str.lower().str.replace(r"[ /.ñ]", "_", regex=True)

    return new_name, df


def process_gcs_to_bq(prefix: str) -> list:
    """
    Procesa archivos CSV desde GCS, los transforma y los agrupa en una lista.

    :param prefix: Prefijo de los archivos a procesar.
    :return: Lista de diccionarios con DataFrames organizados por nombre.
    """
    files = list_gcs_files(GCS_BUCKET_NAME, GCS_FOLDER_PATH, prefix)
    print(f"Archivos encontrados: {files}")

    processed_data = []

    for file in files:
        print(f"Procesando {file}...")
        df = read_gcs_csv(GCS_BUCKET_NAME, file)
        name, df = preparation_before_upload(file, df)
        processed_data.append({name: df})

    return processed_data
