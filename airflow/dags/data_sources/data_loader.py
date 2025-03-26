from data_sources.upload_to_bq import load_file_to_bigquery, load_df_to_bigquery
from data_sources.extract_data_eia import get_eia_data
from data_sources.extract_data_col import process_gcs_to_bq


def load_data_to_gcs(llamado: str, end_point: str, fecha_ini: str = None, fecha_fin: str = None):
    """
    Orquesta la extracción y carga de datos en BigQuery.

    :param llamado: Identifica la fuente de datos ('eia' o 'col').
    :param end_point: Nombre del endpoint a procesar.
    :param fecha_ini: Fecha de inicio para la extracción (solo para 'eia').
    :param fecha_fin: Fecha de fin para la extracción (solo para 'eia').
    """
    project_id = "juanpe-sierracol"

    if llamado == 'eia':
        # Extraer datos de la EIA y cargar cada archivo a BigQuery
        list_path = get_eia_data(end_point, fecha_ini, fecha_fin, project_id)
        for file_path in list_path:
            table_name = end_point  # Se mantiene la misma tabla para todos los periodos
            load_file_to_bigquery(file_path, f"data_{llamado}", table_name, project_id)

    elif llamado == 'col':
        # Procesar archivos CSV desde GCS y cargar a BigQuery
        list_json = process_gcs_to_bq(end_point)
        for json_data in list_json:
            table_name = end_point  # Se mantiene la misma tabla para todos los periodos
            load_df_to_bigquery(json_data, f"data_{llamado}", table_name, project_id)
