from google.cloud import bigquery


def get_bq_table(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    return client.get_table(table_ref)
