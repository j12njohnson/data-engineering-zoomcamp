from pathlib import Path
import pandas as pd
import urllib.error
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=1)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0,1) > 0:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if color == 'green':
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    else:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """"Write DataFrame out locally as parquet file"""
    locdir = Path(f"data/{color}")
    filename = f"{dataset_file}.parquet"
    if not Path.exists(locdir):
        Path.mkdir(locdir)
    path = Path(f"{locdir}/{filename}")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path)
    return

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)
    
# @flow()
# def etl_parent_flow(
#     months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"):
    
#     for month in months:
#         etl_web_to_gcs(year, month, color)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], years: list[int] = [2021], colors: list[str] = ["yellow"]):
    
    for color in colors:
        for year in years:
                for month in months:
                    try:
                        etl_web_to_gcs(year, month, color)
                    except:
                        print("some error occured")
                        continue

if __name__ == '__main__':
    colors = ["yellow"]
    months = [3,2]
    years = [2019]
    etl_parent_flow(months, years, colors)