import os
import requests
import json
import calendar
from datetime import datetime, timedelta
from data_sources.schemas.schemas_eia import schemas_end_points as sep
from data_sources.secret.get_secret import get_secret_value as gsv


def call_eia(end_point: str, fecha_ini: str, fecha_fin: str, project_id: str) -> list:
    """
    Llama a la API de EIA y obtiene datos en formato JSON.

    :param end_point: Nombre del endpoint de EIA.
    :param fecha_ini: Fecha de inicio en formato 'YYYY-MM'.
    :param fecha_fin: Fecha de fin en formato 'YYYY-MM'.
    :param project_id: ID del proyecto en GCP.
    :return: Lista de datos obtenidos de la API.
    """
    if fecha_ini == fecha_fin:
        return ["ERROR: LAS FECHAS NO PUEDEN SER IGUALES"]

    # Obtener URL y parámetros del endpoint
    eia_url = sep.get(end_point, {}).get('url')
    params = sep.get(end_point, {}).get('params', {})

    # Ajustar fechas según la frecuencia de datos
    if params.get('frequency') == 'daily':
        fecha_ini = f"{fecha_ini}-01"
        fecha_fin = f"{fecha_fin}-{calendar.monthrange(*map(int, fecha_fin.split('-')))[1]}"
    elif params.get('frequency') == 'monthly':
        fecha_fin = (datetime.strptime(fecha_fin, "%Y-%m") + timedelta(days=31)).strftime("%Y-%m")

    # Agregar fechas y API Key a los parámetros de la consulta
    params.update({
        'start': fecha_ini,
        'end': fecha_fin,
        'api_key': gsv(project_id, 'token_eia')
    })

    list_json = []
    offset = 0
    max_data = 5000  # Límite de registros por petición

    while max_data == 5000:
        params['offset'] = offset  # Paginación en la API
        response = requests.get(eia_url, params=params)

        if response.status_code == 200:
            # Extraer datos de la respuesta JSON
            data = response.json().get("response", {}).get("data", [])

            # Convertir valores numéricos a FLOAT si es necesario
            for row in data:
                row["value"] = float(row["value"]) if row.get("value") not in [None, ""] else None

                for key in list(row.keys()):
                    new_key = key.replace("-", "_")
                    if new_key != key:
                        row[new_key] = row.pop(key)

            max_data = len(data)  # Número de registros devueltos
            offset += max_data  # Ajustar el offset para la próxima iteración
            list_json.extend(data)  # Acumular los datos
        else:
            print(f"❌ Error en la API de EIA: {response.status_code}")
            break

    return list_json


def get_eia_data(end_point: str, fecha_ini: str, fecha_fin: str, project_id: str) -> list:
    """
    Extrae datos de la API de EIA y los guarda en archivos NDJSON organizados por periodo.

    :param end_point: Nombre del endpoint de EIA.
    :param fecha_ini: Fecha de inicio en formato 'YYYY-MM'.
    :param fecha_fin: Fecha de fin en formato 'YYYY-MM'.
    :param project_id: ID del proyecto en GCP.
    :return: Lista de rutas de archivos generados.
    """
    data = call_eia(end_point, fecha_ini, fecha_fin, project_id)

    # Agregar columna period_normal con el formato YYYY-MM
    for record in data:
        record["period_normal"] = record["period"][:7]

    # Obtener todos los periodos únicos presentes en los datos extraídos
    unique_periods = set(record["period_normal"] for record in data)
    output_dir = "data_sources/output/ndjson"
    os.makedirs(output_dir, exist_ok=True)  # Crear directorio si no existe

    list_file_path = []

    for period in unique_periods:
        # Filtrar registros correspondientes a este periodo
        period_records = [rec for rec in data if rec["period_normal"] == period]
        file_path = os.path.join(output_dir, f"{end_point}_{period}.ndjson")
        list_file_path.append(file_path)

        # Guardar los datos en formato NDJSON
        with open(file_path, "w") as file:
            for record in period_records:
                file.write(json.dumps(record) + "\n")

        print(f"✅ Archivo creado: {file_path}")

    return list_file_path
