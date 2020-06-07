import time
import base64

from google.cloud import bigquery
from google.cloud import pubsub_v1


def get_query_info(project_id, query_id, client):
    """ Query the catalog after transformation info
    """
    query = f"""
        SELECT *
        FROM `{project_id}.DATA_CATALOG.query`
        WHERE id = '{query_id}'
    """
    query_job = client.query(query)
    if len(list(query_job.result())) == 0:
        raise RuntimeError("Query not found")

    query_info = list(query_job.result())[0]

    dataset = query_info['dataset_destination']
    table = query_info['table_destination']
    sql_query = query_info['sql_script']
    return dataset, table, sql_query


def has_son(project_id, query_id, client):
    """ Check the transformation chain to see if this schema
    should trigger another function
    """

    query = f"""
        SELECT query_child_id
        FROM `{project_id}.DATA_CATALOG.transformation_chain`
        WHERE query_father_id = '{query_id}'
    """
    query_job = client.query(query)
    if len(list(query_job.result())) == 0:
        return

    file_info = list(query_job.result())
    query_child_id = [file_info[x]['query_child_id'] for x in range(len(file_info))]
    # Call l3 creation
    call_transformation(query_child_id)


def call_transformation(query_id_list):
    """ Publish a message into a pubsub topic that starts the
    transformation function
    """

    project_id = 'lively-option-279217'
    topic_name = 'chain'

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    futures = dict()

    # Must return a function
    def get_callback(f, id):
        def callback(f):
            try:
                print(f.result())
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


def bigquery_write_query_to_table(event, context):
    query_id = base64.b64decode(event['data']).decode('utf-8')
    client = bigquery.Client()
    project_id = 'lively-option-279217'
    dataset, table, sql_query = get_query_info(project_id, query_id, client)
    table_id = f'{project_id}.{dataset}.{table}'

    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_id
    job_config.write_disposition = 'WRITE_TRUNCATE'
    # Start the query, passing in the extra configuration.
    query_job = client.query(sql_query, job_config=job_config)  # API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(table_id))
    has_son(project_id, query_id, client)
