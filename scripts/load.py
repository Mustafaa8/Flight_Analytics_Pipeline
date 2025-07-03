import pandas as pd
from sqlalchemy import create_engine ,text
from prefect import task
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
   
      engine = create_engine(database_url)
      with open("./sql/tables.sql") as sql_file:
         sql_commands = sql_file.read()
      with engine.connect() as conn:
         conn.execute(text(sql_commands))
   
      logging.info("Loading data into database...")
   
      df_airports = pd.read_csv(f"{path}/airports.csv").drop(['type','source'],axis=1)
      df_airports = df_airports[(df_airports['iata'].notna()) & (df_airports['latitude'].notna()) & (df_airports['longitude'].notna())]
      df_routes = pd.read_csv(f"{path}/routes.csv").drop(['codeshare'],axis=1)
      #df_weather = pd.read_csv(f"{path}/weather.csv")
      df_airports.to_sql("airports",if_exists='replace',con=engine,chunksize=500)
      df_routes.to_sql("routes",if_exists='replace',con=engine,chunksize=500)
      #df_weather.to_sql("weather",if_exists='replace',chunksize=500)
   except:
      logging.error("an error has occured while loading data")
      raise
