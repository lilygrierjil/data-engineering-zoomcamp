from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="buoyant-song-375701",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
   

@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    write_bq(df)
    return len(df)

@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [1, 2],
    year: int = 2021,
    color: str = "yellow"
):
    total_rows_processed = 0
    for month in months:
        rows_processed = etl_gcs_to_bq(year, month, color)
        total_rows_processed += rows_processed
    print(f'total rows processed: {total_rows_processed}')

if __name__=='__main__':
    etl_parent_flow()

# ran: prefect deployment build question_3.py:etl_parent_flow -n "Parameterized GCS to BQ"
# then ran: prefect deployment apply etl_parent_flow-deployment.yaml
