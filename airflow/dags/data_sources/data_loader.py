from data_sources.upload_to_bq import load_file_to_bigquery
from data_sources.extract_data_eia import get_eia_data
from data_sources.extract_data_col import get_col_data

def load_json_to_gcs(llamado, end_point, fecha_ini = None, fecha_fin = None):

    project_id = "juanpe-sierracol"

    if llamado == 'eia':

        list_path = get_eia_data(end_point, fecha_ini, fecha_fin, project_id)

    elif llamado == 'col':

        list_path = get_col_data(end_point)


    for file_path in list_path:
        table_name = end_point  # Usamos la misma tabla para todos los periodos
        load_file_to_bigquery(file_path, f"data_{llamado}", table_name, project_id)
