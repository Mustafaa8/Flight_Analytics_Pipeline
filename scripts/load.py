import pandas as pd
from sqlalchemy import create_engine ,text
from dotenv import load_dotenv
import os
import logging

load_dotenv()
database_url = os.environ.get("POSTGRES_LINK")
path = os.environ.get("DATA_PATH")
logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(message)s")

def load_into_postgres():
   try:
      logging.info("Creating Tables in the database...")
      engine = create_engine(url=database_url)
      with open("./sql/tables.sql") as table_sql:
         commands = table_sql.read()
      with engine.connect() as conn:
         conn.execute(text(commands))
      logging.info("Loading data into database...")

      # Reading data from files
      df_airports = pd.read_csv(f"{path}/airports.csv").drop(['type','source'],axis=1)
      df_airports = df_airports[(df_airports['iata'].notna()) & (df_airports['latitude'].notna()) & (df_airports['longitude'].notna())]
      df_routes = pd.read_csv(f"{path}/routes.csv").drop(['codeshare'],axis=1)
      df_weather = pd.read_csv(f"{path}/weather.csv")
      
      # load data into postgres
      df_airports.to_sql("airports",if_exists='replace',con=engine,chunksize=500)
      df_routes.to_sql("routes",if_exists='replace',con=engine,chunksize=500)
      df_weather.to_sql("weather",if_exists='replace',con=engine,chunksize=500)
      
      logging.info("Data has been loaded into database successfully")
   except Exception as e:
      logging.error("an error has occured while loading data")
      raise


load_into_postgres()