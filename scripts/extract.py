import pandas as pd
import requests
import os 
import logging
from dotenv import load_dotenv
import datetime as dt

logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(message)s")
load_dotenv()

path = os.environ.get("DATA_PATH")

def flight_data_get():
   try:
      logging.info("Extracting Flights Data...")
      columns_name = "airport id, Name, City, Country, IATA, ICAO, Latitude, Longitude, Altitude, Timezone, DST, Tz database time zone, Type, Source".split(",")
      for i in range(0,len(columns_name),1):
         columns_name[i] = columns_name[i].strip().lower().replace(" ","_") 
      df = pd.read_table("https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",sep=",",header=None,names=columns_name,index_col="airport_id",na_values="\\N")
      df.to_csv(f"{path}/data/flight.csv",sep=",")
      logging.info("Data has been extracted and saved into data folder")
   except Exception as e:
      logging.error("An error has occured while extracting data")
      raise
    

def weather_data_get():
   pass
