from google.cloud import secretmanager
import base64


def get_secret_value(project_id, secret_name):
    """
    Obtiene un secreto desde Google Secret Manager y lo decodifica si es Base64.

    :param project_id: ID del proyecto en GCP
    :param secret_name: Nombre del secreto en Secret Manager
    :return: Valor del secreto como string
    """
    client = secretmanager.SecretManagerServiceClient()

    # Construir el nombre del secreto
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Acceder al secreto
    response = client.access_secret_version(name=secret_path)
    secret_value = response.payload.data

    return secret_value.decode("utf-8")


"""if __name__ == "__main__":
    # Configuración (reemplázala con tus valores)
    PROJECT_ID = "juanpe-sierracol"
    SECRET_NAME = "token_eia"

    # Obtener el secreto
    secret = get_secret_value(PROJECT_ID, SECRET_NAME)
    print(f"🔑 Secreto obtenido: {secret}")"""
