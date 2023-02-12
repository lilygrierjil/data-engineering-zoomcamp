from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import os
'''Fetch trip data from web and upload to GCS.'''

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    return df



@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out as .csv.gz file"""
    path = Path(f"data/{dataset_file}.csv.gz")
    print(path)
    if not os.path.exists(f"data/"):
        os.makedirs(f"data/")
    df.to_csv(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )
    return

@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"    
    # print(dataset_url)
    df = fetch(dataset_url)
    path = write_local(df, dataset_file)
    write_gcs(path)
    return len(df)

@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [num for num in range(1, 13)],
    year: int = 2019,
):
    total_rows_processed = 0
    for month in months:
        rows_processed = etl_web_to_gcs(year, month)
        total_rows_processed += rows_processed
    print(f'total rows processed: {total_rows_processed}')

if __name__=='__main__':
    etl_parent_flow()
