import pandas as pd
import requests
import os 
import logging
from dotenv import load_dotenv
import progressbar
import time

logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(message)s")
load_dotenv()

path = os.environ.get("DATA_PATH")

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

def weather_for_airport(lat,lon):
   """
   get the weather for each airport for the Openmetro API 

   Args:
   - lat : Latitude of airport
   - lon : longitude of airport
   """
   params = {
      "latitude":lat,
      "longitude":lon,
      "current": ["temperature_2m", "wind_speed_10m", "precipitation", "weather_code"],
	   "timezone": "auto",
	   "forecast_days": 1
   }
   headers = {
    "User-Agent": "Mozilla/5.0 (compatible; Skylytics/1.0; +http://skylytics.example)"
    }
   url = "https://api.open-meteo.com/v1/forecast"
   res = requests.get(url,params=params,headers=headers,timeout=10).json()

   weather_data = {
      "latitude":lat,
      "longitude":lon,
      "temperature":res['current']['temperature_2m'],
      "wind_speed":res['current']['wind_speed_10m'],
      "precipitation":res['current']['precipitation'],
      "timezone":res['timezone_abbreviation'],
      "weather_code":res['current']['weather_code']
   }
   time.sleep(1)
   return weather_data

def weather_data_get():
    """
    Fetches weather data for all airports and stores result with progress caching.
    """
    try:
        logging.info("Starting weather data collection...")
        df = pd.read_csv(f"{path}/airports.csv")
        df = df[(df['iata'].notna()) & (df['latitude'].notna()) & (df['longitude'].notna())]

        cache_file = "data/weather_cache.csv"
        if os.path.exists(cache_file):
            cached_df = pd.read_csv(cache_file)
            airports_weather_data = cached_df.to_dict("records")
            processed_coords = {(row['latitude'], row['longitude']) for row in airports_weather_data}
            logging.info(f"Loaded {len(processed_coords)} cached records.")
        else:
            airports_weather_data = []
            processed_coords = set()

        # Progress bar
        bar = progressbar.ProgressBar(widgets=[
            'Weather Data Collection: ', progressbar.Percentage(),
            ' ', progressbar.Bar(marker=progressbar.RotatingMarker()),
            ' ', progressbar.Counter(), ' ', progressbar.Timer()
        ], maxval=df.shape[0]).start()

        # Loop through airports
        for index, row in df.iterrows():
            lat, lon = row['latitude'], row['longitude']
            coord_key = (lat, lon)

            if coord_key in processed_coords:
                continue

            try:
                if index % 100 == 0 and index > 0:
                    pd.DataFrame(airports_weather_data).to_csv(cache_file, index=False)
                    logging.info(f"Saved progress at {index} rows.")
                    time.sleep(5)

                weather = weather_for_airport(lat, lon)
                weather["airport_iata"] = row["iata"]
                weather["airport_city"] = row["city"]
                airports_weather_data.append(weather)
                processed_coords.add(coord_key)

                bar.update(index + 1)

            except KeyboardInterrupt:
                logging.error("Interrupted by user.")
                raise

            except Exception as e:
                logging.error(f"Failed for [{lat}, {lon}] - {e}")
                continue

        bar.finish()

        # Final save
        weather_df = pd.DataFrame(airports_weather_data)
        weather_df.to_csv("data/weather.csv", index=False)
        os.remove(cache_file)
        logging.info("Weather data collection completed and saved.")

    except Exception as e:
        logging.error(f"Unhandled error occurred: {e}")
        raise

