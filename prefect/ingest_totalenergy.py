import os
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket



@task(retries=3, log_prints=True, cache_key_fn=task_input_hash)
def get_total_energy_data(url, year, api_key, series_id) -> pd.DataFrame:
    session = requests.Session()
    df = pd.DataFrame()
    # loop through months
    months = ['01','02','03','04','05','06','07','08','09','10','11','12']
    for m in months:
        payload = {
            "api_key": api_key,
            "start": f"{year}-{m}",
            "end": f"{year}-{m}",
            "frequency": "monthly",
            "data[0]":"value"
        }
        response = session.get(url+series_id+'/data/', params=payload)
        print(f"Fetching data for {year}-{m}...")
        if response.status_code == 200:
            data = response.json()['response']
            df = pd.concat([df, pd.DataFrame(data['data'])])
            print("Sucessfull")
        else:
            print(f"Error {response.status_code} for {year}-{m}")
            raise
    return df

@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    nan_strings = ['Not Available','No Data Reported','Withheld','Not Applicable','Not Meaningful']
    df.value.mask(df.value.isin(nan_strings), inplace=True)
    df['value'] = df['value'].astype(float)
    df['period'] = pd.to_datetime(df['period'])
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"../data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtc-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@task(retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("dtc-gcp-creds")

    df.to_gbq(
        destination_table="eia_totalenergy.totalenergy_monthly",
        project_id="ethereal-link-375303",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
    )

@flow()
def etl_web_to_gcs(year: int) -> None:
    """The main ETL function"""
    # API URL for Total Energy data
    url = "https://api.eia.gov/v2/"
    dataset_file = f"total_energy_{year}"

    # Set API key and series id
    load_dotenv()
    api_key = os.getenv('EIA_API_KEY')
    series_id = "total-energy"

    df = get_total_energy_data(url, year, api_key, series_id)
    df_clean = clean(df)
    # path = write_local(df_clean, dataset_file)
    #write_gcs(path)
    write_bq(df)

@flow()
def etl_parent_flow(years: list[int] = list(range(2020,2024))):
    for y in years:
        print(f"Starting flow for {y}")
        etl_web_to_gcs(y)
    

if __name__ == "__main__":
    etl_parent_flow()