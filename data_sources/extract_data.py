import os
import requests
import json
import pandas as pd
from schemas_eia import schemas_end_points as sep
from secret.get_secret import get_secret_value as gsv
import calendar
from datetime import datetime, timedelta

def call_eia(end_point, fecha_ini, fecha_fin):

    if fecha_ini == fecha_fin:

        return("ERROR: LAS FECHAS NO PUEDEN SER IGUALES")

    eia_url = sep.get(end_point).get('url')
    params = sep.get(end_point).get('params')

    if params['frequency'] == 'daily':
        fecha_ini = fecha_ini + '-01'
        fecha_fin = f"{fecha_fin}-{calendar.monthrange(*map(int, fecha_fin.split('-')))[1]}"

    if params['frequency'] == 'monthly':
        fecha_fin = (datetime.strptime(fecha_fin, "%Y-%m") + timedelta(days=31)).strftime("%Y-%m")


    params['start'] = fecha_ini
    params['end'] = fecha_fin

    params['api_key'] = gsv('juanpe-sierracol', 'token_eia')

    i=0
    max_data = 5000
    list_json = []

    while max_data == 5000:
        params['offset'] = i
        response = requests.get(eia_url, params=params)
        if response.status_code == 200:
            data = response.json()
            json = data["response"]["data"]
            max_data = len(json)
            i = i + max_data
            list_json = list_json + json
        else:
            print(f"❌ Error en la API de EIA: {response.status_code}")
            max_data = 5000
    return list_json

def get_eia_data(end_point, fecha_ini, fecha_fin):
    """ Extrae datos de la API de EIA y lo guarda en CSV """

    data = call_eia(end_point, fecha_ini, fecha_fin)

    for record in data:
        record["period_normal"] = record["period"][:7]

    unique_periods = set(record["period_normal"] for record in data)

    output_dir = "output/ndjson"

    list_file_path = []

    for period in unique_periods:
        # Filtrar registros correspondientes a este periodo
        period_records = [rec for rec in data if rec["period_normal"] == period]

        # Definir la ruta del archivo
        file_path = os.path.join(output_dir, f"{end_point}_{period}.ndjson")

        list_file_path.append(file_path)

        # Guardar en formato NDJSON
        with open(file_path, "w") as file:
            for record in period_records:
                file.write(json.dumps(record) + "\n")

        print(f"Archivo creado: {file_path}")

    return list_file_path


