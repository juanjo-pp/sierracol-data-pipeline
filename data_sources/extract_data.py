import os
import requests
import pandas as pd
from schemas_eia import schemas_end_points as sep


# Crear la carpeta de salida si no existe
os.makedirs("data_sources/output", exist_ok=True)



# Configuración de la API de EIA
EIA_API_KEY = "Ss64oru9uS3ZaJfj91YYf7sv0Nvsuv6AMeGnB6mM"  # Reemplázala con tu clave
EIA_URL = f"https://api.eia.gov/v2/petroleum/crd/crpdn/data/"

# Configuración de Open Data (Banco Mundial)
WB_URL = "http://api.worldbank.org/v2/en/indicator/NY.GDP.PETR.RT.ZS?downloadformat=csv"

def call_eia(end_point, fecha_ini, fecha_fin):

    eia_url = sep.get(end_point).get('url')
    params = sep.get(end_point).get('params')

    if params['frequency'] == 'monthly':
        params['start'] = fecha_ini[:-3]
        params['end'] = fecha_fin[:-3]

    elif params['frequency'] == 'daily':
        params['start'] = fecha_ini
        params['end'] = fecha_fin
    params['api_key'] = EIA_API_KEY

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

def get_eia_data():
    """ Extrae datos de la API de EIA y lo guarda en CSV """
    end_point = 'crude_oil_production'
    fecha_ini = '2020-01-01'
    fecha_fin = '2022-12-31'
    data = call_eia(end_point, fecha_ini, fecha_fin)
    df = pd.DataFrame(data)
    output_path = "data_sources/output/eia_data.csv"
    df.to_csv(output_path, index=False)




def get_world_bank_data():
    """ Descarga los datos del Banco Mundial y los guarda en CSV """
    wb_data = pd.read_csv(WB_URL, skiprows=4)
    output_path = "data_sources/output/world_bank_data.csv"
    wb_data.to_csv(output_path, index=False)
    print(f"✅ Datos del Banco Mundial guardados en {output_path}")


if __name__ == "__main__":
    get_eia_data()
    get_world_bank_data()
