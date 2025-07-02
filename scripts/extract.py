import pandas as pd
import requests
import os 
import logging
from dotenv import load_dotenv
import progressbar
import time
from prefect import task

logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(message)s")
load_dotenv()

path = os.environ.get("DATA_PATH")

@task
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


def map_weather_code(code):
   if code == 0:
      return "Clear Sky"
   elif code in [1,2,3]:
      return "Mainly Clear"
   elif code in [45,48]:
      return "Fog",
   elif code in [51, 53, 55]:
      return "Drizzle"
   elif code in [56,57]:
      return "Freezing Drizzle",
   elif code in [61, 63, 65]:
      return "Rain"
   elif code in [66, 67]:
      return "Freezing Rain",
   elif code in [71, 73, 75]:
      return "Snow Fall"
   elif code in [45,48]:
      return "Fog ",
   elif code == 77:
      return "Snow Grains"
   elif code in [80, 81, 82]:
      return "Rain Showers"
   elif code in [85, 86]:
      return "Snow Showers"
   elif code == 95:
      return "Thunderstorms"
   elif code in [96,99]:
      return "Heavy Thunderstorm"
   else:
      return "Unknown"

def weather_for_airport(lat,lon):
   params = {
      "latitude":lat,
      "longitude":lon,
      "current": ["temperature_2m", "wind_speed_10m", "precipitation", "weather_code"],
	   "timezone": "auto",
	   "forecast_days": 1
   }
   url = "https://api.open-meteo.com/v1/forecast"
   res = requests.get(url,params=params).json()

   weather_data = {
      "latitude":lat,
      "longitude":lon,
      "temperature":res['current']['temperature_2m'],
      "wind_speed":res['current']['wind_speed_10m'],
      "precipitation":res['current']['precipitation'],
      "timezone":res['timezone_abbreviation'],
      "weather_state":map_weather_code(res['current']['weather_code'])
   }
   time.sleep(1.1)
   return weather_data

@task
def weather_data_get():
   logging.info("Getting Weather Data...")
   df = pd.read_csv(f"{path}/airports.csv")
   df = df[(df['iata'].notna()) & (df['latitude'].notna()) & (df['longitude'].notna())]
   airports_weather_data = []
   bar = progressbar.ProgressBar(widgets=
                                 [
        'Weather Data Collection: ', progressbar.Percentage(),
        ' ', progressbar.Bar(marker=progressbar.RotatingMarker()),
        ' ', progressbar.Counter(),
        ' ', progressbar.Timer(),
    ]
    ,maxval=df.shape[0]).start()
   for index,row in df.iterrows():
      airport_weather = weather_for_airport(row['latitude'],row['longitude'])
      airport_weather['iata'] = row['iata']
      airport_weather['city'] = row['city']
      airports_weather_data.append(airport_weather)
      bar.update(index + 1)
   bar.finish()
   weather_data = pd.DataFrame(airport_weather)
   print(weather_data.head())
   weather_data.to_csv("weather.csv",sep=",")

weather_data_get()