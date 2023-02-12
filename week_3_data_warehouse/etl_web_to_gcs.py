from pathlib import Path
from typing import List
import pandas as pd
import numpy as np

from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket


import ssl
ssl._create_default_https_context = ssl._create_unverified_context


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Fetch data from web."""
    df = pd.read_csv(dataset_url)
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write to local disk."""
    dir = Path(f"data/")
    dir.mkdir(parents=True, exist_ok=True)
    path = dir / f"{dataset_file}.parquet"
    df.to_parquet(path, compression="gzip")
    return path


@task()
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df['DOlocationID'] = df["DOlocationID"].astype("Int64")
    df['PUlocationID'] = df["PUlocationID"].astype("Int64")
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
    return df
    

@task()
def write_gcs(path: str) -> None:
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path,
    )
    return 


@flow()
def etl_web_to_gcs(year: int, months: List[int]) -> None:
    """ETL flow to extract data from web, transform and load into GCS."""
    # Extract
    for month in months:
        dataset_file = f"fhv_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{str(month).zfill(2)}.csv.gz"

        # Transform
        df = fetch(dataset_url)
        df = transform(df)
        path = write_local(df, dataset_file)
        write_gcs(path)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--months", type=int, nargs="+")
    args = parser.parse_args()

    etl_web_to_gcs(year=args.year, months=args.months)
