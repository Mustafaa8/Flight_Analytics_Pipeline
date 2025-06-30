import pandas as pd
import requests
import os 
import logging
from dotenv import load_dotenv
import datetime as dt
import pickle

logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(message)s")
load_dotenv()

path = os.environ.get("DATA_PATH")
openweather_key = os.environ.get("OPENWEATHER_API_KEY")

def flight_data_get():
   """
   function to download and get the airport and routes data
   """
   try:
      # Download and modify the airport data
      logging.info("Extracting Flights Data...")
      columns_name = "airport id, Name, City, Country, IATA, ICAO, Latitude, Longitude, Altitude, Timezone, DST, Tz database time zone, Type, Source".split(",")
      for i in range(0,len(columns_name),1):
         columns_name[i] = columns_name[i].strip().lower().replace(" ","_") 
      airports = pd.read_table("https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",sep=",",header=None,names=columns_name,index_col="airport_id",na_values="\\N")
      airports.to_csv(f"{path}/airports.csv",sep=",")

      # for the weather api calls
        
      # Downloading the routes data
      logging.info("Extracting Routes Data...")
      routes_cols = "Airline, Airline ID, Source airport, Source airport ID, Destination airport, Destination airport ID, Codeshare, Stops, Equipment".split(",")
      for i in range(0,len(routes_cols),1):
         routes_cols[i] = routes_cols[i].strip().lower().replace(" ","_")
      routes = pd.read_table("https://raw.githubusercontent.com/jpatokal/openflights/refs/heads/master/data/routes.dat",sep = ",",header = None,names=routes_cols)
      routes.to_csv(f"{path}/routes.csv",sep=",")
      logging.info("Data has been extracted and saved into data folder")
   except Exception as e:
      logging.error("An error has occured while extracting data")
      raise
    
def weather_for_airport():
   pass

def weather_data_get():
   pass