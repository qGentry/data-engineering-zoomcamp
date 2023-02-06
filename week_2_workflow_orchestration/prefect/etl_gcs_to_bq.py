from pathlib import Path
from typing import List

import pandas as pd
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
    

def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Extract data from GCS."""
    gcs_block = GcsBucket.load("zoom-gcs")
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    path = f"data/{color}/{dataset_file}.parquet"
    gcs_block.get_directory(from_path=path, local_path=f"data")

    return Path(f"data/{path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df


def write_to_bq(df: pd.DataFrame) -> None:
    """Writes pd.DataFrame to BigQuery."""
    creds_block = GcpCredentials.load("zoom-gcs-creds")
    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="tidy-bliss-376908",
        if_exists="append",
        credentials=creds_block.get_credentials_from_service_account(),
    )
    return
 

@flow(log_prints=True)
def etl_gcs_to_bq(color: str, year: int, months: List[int]) -> None:
    total_rows = 0
    for month in months:
        # Use concurrentcy to speed up the process
        print(f"Rows processed so far: {total_rows}")
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        total_rows += len(df)
        write_to_bq(df)
    print(f"Total rows processed: {total_rows}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--color", type=str)
    parser.add_argument("--year", type=int)
    parser.add_argument("--months", type=int, nargs="+")
    args = parser.parse_args()

    etl_gcs_to_bq(color=args.color, year=args.year, months=args.months)
