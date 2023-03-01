from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import os

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    if 'lpep_pickup_datetime' in df.columns:
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    elif 'tpep_pickup_datetime' in df.columns:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    #print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as csv file"""
    path = Path(f"data/{color}/{dataset_file}.csv.gz")
    print(path)
    if not os.path.exists(f"data/{color}"):
        os.makedirs(f"data/{color}")
    df.to_csv(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local csv file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )
    return

@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"    
    # print(dataset_url)
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

def etl_parent_flow(color: str = "yellow",
    months: list[int] = [num for num in range(1, 13)],
    year: int = 2019):
    for month in months:
        etl_web_to_gcs(color, year, month)

if __name__=='__main__':
    color = "yellow"
    year = 2019
    #etl_parent_flow(color=color, year=year, months=[9, 10, 11, 12])
    #etl_parent_flow(color=color, year=2020, months=[9, 10, 11, 12])
    #etl_parent_flow(color="green", year=2019)
    etl_parent_flow(color="green", year=2020)


