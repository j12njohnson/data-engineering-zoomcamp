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
    if color == "yellow":
        """Fix dtype issues"""
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    if color == "green":
        """Fix dtype issues"""
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
        df["trip_type"] = df["trip_type"].astype('Int64')

    if color == "yellow" or color == "green":
        df["VendorID"] = df["VendorID"].astype('Int64')
        df["RatecodeID"] = df["RatecodeID"].astype('Int64')
        df["PULocationID"] = df["PULocationID"].astype('Int64')
        df["DOLocationID"] = df["DOLocationID"].astype('Int64')
        df["passenger_count"] = df["passenger_count"].astype('Int64')
        df["payment_type"] = df["payment_type"].astype('Int64')

    if color == "fhv":
        """Rename columns"""
        df.rename({'dropoff_datetime':'dropOff_datetime'}, axis='columns', inplace=True)
        df.rename({'PULocationID':'PUlocationID'}, axis='columns', inplace=True)
        df.rename({'DOLocationID':'DOlocationID'}, axis='columns', inplace=True)

        """Fix dtype issues"""
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
        df["PUlocationID"] = df["PUlocationID"].astype('Int64')
        df["DOlocationID"] = df["DOlocationID"].astype('Int64')

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
    colors = ["green"]
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    years = [2019,2020]
    etl_parent_flow(months, years, colors)