from upload_to_gcs import upload_files_to_gcs
from extract_data_eia import get_eia_data

def load_json_to_gcs():
    end_point = 'ventas'
    fecha_ini = '2020-01'
    fecha_fin = '2020-12'

    bucket_name = "sierracol-data-storage"
    destination_folder = f"eia/{end_point}"  # Ahora sí se asegurará de que sea un prefijo válido
    project_id = "juanpe-sierracol"

    list_path = get_eia_data(end_point, fecha_ini, fecha_fin)
    upload_files_to_gcs(list_path
                        , bucket_name
                        , destination_folder
                        , project_id
                        , delete_local=True)

if __name__ == "__main__":
    load_json_to_gcs()
