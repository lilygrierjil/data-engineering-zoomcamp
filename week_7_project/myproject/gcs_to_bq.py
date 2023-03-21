from prefect_gcp.bigquery import BigQueryWarehouse, bigquery_query
from prefect_gcp import GcpCredentials
from prefect import flow, task

@task()
def create_bq_dataset():
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    client = gcp_credentials_block.get_bigquery_client(project='buoyant-song-375701')
    client.create_dataset("final_project", exists_ok=True)

@task()
def create_external_table():
    warehouse = BigQueryWarehouse.load("final-project-bq", project='buoyant-song-375701')
    create_operation = '''
    CREATE OR REPLACE EXTERNAL TABLE `final_project.external_memphis_police_data`
    OPTIONS (
    format = 'parquet',
    uris = ['gs://prefect-de-zoomcamp-lily/data/memphis_police_data.parquet']
    );
    '''
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    warehouse.execute(create_operation)

@task()
def create_partitioned_clustered_table():
    warehouse = BigQueryWarehouse.load("final-project-bq")
    create_operation = '''
    CREATE OR REPLACE TABLE final_project.memphis_police_data_partitioned_clustered
    PARTITION BY DATE(offense_date_datetime)
    CLUSTER BY category AS
    SELECT * FROM final_project.external_memphis_police_data;'''
    warehouse.execute(create_operation, credentials=GcpCredentials.load("zoom-gcp-creds"))


@flow(log_prints=True)
def etl_gcs_to_bq():
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    google_auth_credentials = gcp_credentials_block.get_credentials_from_service_account()
    print('credentials!')
    print(google_auth_credentials)
    create_bq_dataset()
    create_external_table()
    create_partitioned_clustered_table()


if __name__=='__main__':
    etl_gcs_to_bq()