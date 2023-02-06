from pathlib import Path
from typing import List
import pandas as pd

from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket


import ssl
ssl._create_default_https_context = ssl._create_unverified_context

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Fetch data from web."""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """fix dtype issues"""
    print(df.head(2))
    print(f"Dtypes of df: {df.dtypes}")
    print(f"Length of df: {len(df)}")
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write to local disk."""
    dir = Path(f"data/{color}")
    dir.mkdir(parents=True, exist_ok=True)
    path = dir / f"{dataset_file}.parquet"
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: str) -> None:
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path,
    )
    return 
    

@flow()
def etl_web_to_gcs(color: str, year: int, months: List[int]) -> None:
    """ETL flow to extract data from web, transform and load into GCS."""
    # Extract
    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

        # Transform
        df = fetch(dataset_url)
        df = clean(df)
        path = write_local(df, color, dataset_file)
        write_gcs(path)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--color", type=str)
    parser.add_argument("--year", type=int)
    parser.add_argument("--months", type=int, nargs="+")
    args = parser.parse_args()

    etl_web_to_gcs(color=args.color, year=args.year, months=args.months)
