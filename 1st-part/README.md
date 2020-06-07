
## Implementation
This data pipeline is very simple to implement and use `Google Storage`, `Cloud Functions`, `Pub/Sub`, `BigQuery & BigQuery Jobs` to process the json files and create the analytical dataset inside Bigquery, but to use the files on the folder BASE A was need to be changed, from a list of json to one json per line.

The analytical base is at the folder `/query/`.
If you want to see the whole pipeline running, I can add you to the Google Cloud Platform Project.


### 1st step
Every time a file is inserted at the data_lake bucket, the function `ingestion` is triggered and run a bigquery job to create a layer 2 table inside bigquery, according to Data Catalog Schema table.

### 2st step
When a schema is connected to a query, through Data Catalog Transformation Chain table, the `ingestion` fuction send to pubsub a message with the query id. Then the `transformation` function that is connected to the topic `chain` recieve the id and run the bigquery job.

### Data Catalog
#### schema
The table schema has following columns:
- id
- schema_json: contains the data inside the folder `/schemas/`
- bucket_name: contains bucket name, like l2_subscriptions
- dataset_destination: the dataset destination name
- table_destination: the table name {dataset_destination}.{table_destination}

#### query
- id
- sql_script: the sql script that will update/create table
- dataset_destination: the dataset destination name
- table_destination: the table name {dataset_destination}.{table_destination}

#### transformation_chain
- id
- schema_father_id: schema id that will be always connected to a query_child_id
- query_father_id: query id that will be always connected to a query_child_id
- query_child_id

### Comments
This is not the kind of implementation to deal with big data, becoming expansive. But if you dont have too much data to handle, its easier and cheaper than the 2st part solution, DataProc minimum cluster has a high starting price.
