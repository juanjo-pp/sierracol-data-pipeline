from google.cloud import secretmanager

def get_secret_value(project_id: str, secret_name: str) -> str:
    """
    Obtiene un secreto desde Google Secret Manager y lo decodifica si es Base64.

    :param project_id: ID del proyecto en GCP.
    :param secret_name: Nombre del secreto en Secret Manager.
    :return: Valor del secreto como string.
    """
    client = secretmanager.SecretManagerServiceClient()

    # Construir la ruta del secreto en Secret Manager
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Acceder al secreto
    response = client.access_secret_version(name=secret_path)
    secret_value = response.payload.data.decode("utf-8")

    return secret_value
