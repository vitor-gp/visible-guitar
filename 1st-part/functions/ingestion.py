import json
import time
import logging

from google.cloud import bigquery
from google.cloud import pubsub_v1


def has_son(schema_id, client):
    """ Check the transformation chain to see if this schema
    should trigger another function
    """
    project_id = 'lively-option-279217'
    query = f"""
        SELECT query_child_id
        FROM `{project_id}.DATA_CATALOG.transformation_chain`
        WHERE schema_father_id = '{schema_id}'
    """
    query_job = client.query(query)
    if len(list(query_job.result())) == 0:
        return

    file_info = list(query_job.result())
    query_child_id = [file_info[x]['query_child_id'] for x in range(len(file_info))]
    # Call l3 creation
    call_transformation(project_id, query_child_id)


def call_transformation(project_id, query_id_list):
    """ Publish a message into a pubsub topic that starts the
    transformation function
    """

    topic_name = 'chain'

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    futures = dict()

    # Must return a function
    def get_callback(f, id):
        def callback(f):
            try:
                futures.pop(id)
            except:
                raise RuntimeError(f"Please handle {f.exception()} for {id}.")
        return callback

    for id in query_id_list:
        futures.update({id: None})
        # When you publish a message, the client returns a future.
        future = publisher.publish(
            topic_path, data=id.encode("utf-8")  # data must be a bytestring.
        )
        futures[id] = future
        # Publish failures shall be handled in the callback function.
        future.add_done_callback(get_callback(future, id))
    while futures:
        time.sleep(5)


def get_schema_info(client, gcs_path, gcs_filename):
    """ Query the catalog after transformation info
    """


    bucket_path = gcs_path.split('/')[1]

    logging.info('query bigquery 1')
    query = f"""
        SELECT *
        FROM `lively-option-279217.DATA_CATALOG.schema`
        WHERE bucket_name = '{bucket_path}'
    """
    logging.info('query bigquery 1')

    query_job = client.query(query)
    logging.info('query bigquery 2')
    schema_info = list(query_job.result())[0]
    dataset = schema_info['dataset_destination']
    table = schema_info['table_destination']
    schema_id = schema_info['id']
    schema = schema_info['schema_json']
    logging.info('query bigquery 3')
    return dataset, table, schema_id, schema


def _get_field_schema(**kwargs):
    """ Recursive function capable of parse nested schemas
    """
    fields = kwargs.get('fields', None)
    if fields:
        field_schema = bigquery.SchemaField(
            name=kwargs['name'],
            mode=kwargs.get('mode', 'NULLABLE'),
            field_type=kwargs.get('type', 'STRING'),
            description=kwargs.get('description', None),
            fields=[_get_field_schema(**field) for field in fields]
        )
        return field_schema
    field_schema = bigquery.SchemaField(
        name=kwargs['name'],
        mode=kwargs.get('mode', 'NULLABLE'),
        field_type=kwargs.get('type', 'STRING'),
        description=kwargs.get('description', None),
    )
    return field_schema


def bq_ingestion(data, context):
    gcs_path = data['name']
    client = bigquery.Client()

    # expects 'bucket/folder/filename_yyy_mm_dd.csv'
    file_format = gcs_path.split('.')[-1]
    gcs_filename = gcs_path.split('/')[-1]
    gcs_filename = gcs_filename.replace(f'.{file_format}', '')

    # terminating function without reporting to stack driver
    dataset, table_name, schema_id, schema = get_schema_info(
        client, gcs_path, gcs_filename
    )
    logging.info('query bigquery ok')
    table_name = f"{table_name}_{'_'.join(gcs_path.split('/')[3:-1])}"

    dataset_ref = client.dataset(dataset)
    job_config = bigquery.LoadJobConfig()
    # Transform the json schema into a usable job_config.schema
    schema = json.loads(schema)
    job_config.schema = [_get_field_schema(**field) for field in schema]
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.ignore_unknown_values = True

    gcs_file_path = f'gs://pd_data_lake/{gcs_path}'
    # Make the API request to load table based on the json schema
    load_job = client.load_table_from_uri(
        gcs_file_path,
        dataset_ref.table(table_name),
        job_config=job_config
    )
    # Waits for table load to complete
    load_job.result()
    # Verify if needs to update a query
    has_son(schema_id, client)
