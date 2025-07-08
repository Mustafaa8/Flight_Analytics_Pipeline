import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.extract import *
from scripts.load import *
from prefect import task , flow


@task(log_prints=True)
def extract_data():
   flight_data_get()
   weather_data_get()

@task(log_prints=True)
def load_into_db():
   load_into_postgres()

@flow(log_prints=True,name="Skylytics_etl_flow",retries=3,retry_delay_seconds=5)
def etl_flow():
   extract_data_result = extract_data()
   load_into_db()

if __name__ == "__main__":
   etl_flow()