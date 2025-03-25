from google.cloud import storage
import os

def create_gcs_folder_if_not_exists(bucket_name, destination_folder, project_id):
    """
    Simula la creación de una carpeta en GCS subiendo un archivo vacío si no hay archivos en la carpeta.
    """
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    # Verificar si la "carpeta" ya tiene archivos
    blobs = list(bucket.list_blobs(prefix=destination_folder + "/"))  # Agregar "/" al prefijo
    if not blobs:  # Si la carpeta está vacía
        blob = bucket.blob(destination_folder + "/.keep")  # Forzar el slash al final
        blob.upload_from_string("")  # Archivo vacío para marcar la carpeta
        print(f"Carpeta gs://{bucket_name}/{destination_folder}/ creada.")

def upload_files_to_gcs(file_paths, bucket_name, destination_folder, project_id, delete_local=False):
    """
    Sube una lista de archivos a Google Cloud Storage, reemplazando si ya existen.

    :param file_paths: Lista de rutas de archivos locales a subir.
    :param bucket_name: Nombre del bucket en GCS.
    :param destination_folder: Carpeta dentro del bucket donde se subirán los archivos.
    :param delete_local: Si es True, elimina los archivos locales después de subirlos.
    """
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    # Crear la carpeta en GCS si no existe
    create_gcs_folder_if_not_exists(bucket_name, destination_folder, project_id)

    for file_path in file_paths:
        full_path = os.path.abspath(file_path)  # Convertir ruta relativa a absoluta

        if not os.path.exists(full_path):
            print(f"ERROR: El archivo {full_path} no existe")
            continue

        # Nombre del archivo en GCS
        blob_name = os.path.join(destination_folder, os.path.basename(file_path)).replace("\\", "/")
        blob = bucket.blob(blob_name)

        # Subir y reemplazar si ya existe
        blob.upload_from_filename(full_path)
        print(f"Archivo subido a gs://{bucket_name}/{blob_name}")

        # Eliminar archivo local después de subirlo
        if delete_local:
            os.remove(full_path)
            print(f"Archivo local eliminado: {full_path}")
