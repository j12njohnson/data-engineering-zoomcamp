from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> pd.DataFrame: 
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.get_directory(from_path = gcs_path, local_path = f"./data/")
    df = pd.read_parquet(Path(f"./{gcs_path}"))
    return df
    
@task()
def read(path: Path) -> pd.DataFrame:
    """read the data into pandas"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    
    gcp_credentials_block = GcpCredentials.load("de-zoomcamp-creds")

    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="sincere-night-377203",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    return len(df)


@flow()
def el_gcs_to_bq(year: int, month: int, color: str) -> None:
        """Main ETL flow to load data into BigQuery"""
        path = extract_from_gcs(color, year, month)
        df = read(path)
        row_count = write_bq(df)
        return row_count


@flow()
def parent_el_gcs_to_bq(
    months: list[int] = [1, 2], years: list[int] = [2021], colors: list[str] = ["yellow"]):
        """Main EL flow to load data into BigQuery"""
        total_rows = 0
        for color in colors:
            for year in years:
                    for month in months:
                        rows = el_gcs_to_bq(year, month, color)
                        total_rows += rows

        print(total_rows)


if __name__=="__main__":
    colors = ["yellow"]
    months = [2,3]
    years = [2019]
    parent_el_gcs_to_bq(months, years, colors)