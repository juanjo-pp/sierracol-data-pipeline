import os
import pandas as pd

def preparation_before_upload(folder_path: str, prefix):
    for filename in os.listdir(folder_path):
        if filename.startswith(prefix) and filename.endswith(".csv") and len(filename) > 13:
            old_path = os.path.join(folder_path, filename)
            new_filename = filename[:-13] + ".csv"
            new_path = os.path.join(folder_path, new_filename)
            os.rename(old_path, new_path)
            print(f'Renombrado: {filename} -> {new_filename}')

            df = pd.read_csv(new_path)

            # Agregar una nueva columna con un valor predeterminado
            df["period_normal"] = new_filename[-8:-4]

            df.columns = df.columns.str.lower().str.replace(r"[ /.ñ]", "_", regex=True)

            # Guardar el CSV editado
            df.to_csv(f"{new_path}", index=False)

def get_col_data(prefix):

    """
    Lista todos los archivos dentro de un directorio y devuelve sus rutas absolutas.
    """
    directory = "data_sources/csv"

    preparation_before_upload(directory, prefix)

    list_final =[
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, f)) and f.startswith(prefix)
    ]

    return list_final
