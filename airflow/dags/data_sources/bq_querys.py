from google.cloud import bigquery


def ejecutar_rutinas():
    """Ejecuta dos rutinas en BigQuery y muestra los resultados."""
    client = bigquery.Client()
    query = ("CALL `juanpe-sierracol.data_col.procedure_fiscalizada`();"
             "CALL `juanpe-sierracol.data_col.procedure_regalias`();")

    query_job = client.query(query)
    result = query_job.result()
    for row in result:
        print(dict(row))


if __name__ == "__main__":
    ejecutar_rutinas()
