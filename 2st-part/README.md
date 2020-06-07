## Implementation

This data pipeline implementation is similar to the 1st pipeline but, instead of using bigquery jobs to process the files and create the datamarts, we will use `Google Dataproc/pyspark` to process the files and create inside bigquery the data.
Just like the 1st part, we will use Google Functions triggers, so whenever a file is created inside the Google Storage, Functions will start a `DataProc job ` who runs a script to process this files.

We can use the Data Catalog Implementation to make this pipeline more simple and easy to implement
