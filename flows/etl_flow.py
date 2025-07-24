import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.extract import *
from scripts.load import *
from prefect import task , flow
from prefect_shell import ShellOperation

@task(name="extract",log_prints=True)
def extract_data():
   flight_data_get()
   weather_data_get()

@task(name="load",log_prints=True)
def load_into_db():
   load_into_postgres()

@task(name="transform",log_prints=True)
def dbt_transformation():
   ShellOperation(
      commands=[
         "cd ./skylytics_dbt && dbt run && dbt test"
      ]
   ).run()

@flow(log_prints=True,name="Skylytics ELT workflow")
def etl_flow():
   extract = extract_data().submit()
   load = load_into_db.submit(wait_for=[extract])
   dbt_transformation.submit(wait_for=[load])


if __name__ == "__main__":
   etl_flow()