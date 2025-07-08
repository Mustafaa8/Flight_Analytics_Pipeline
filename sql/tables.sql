CREATE TABLE IF NOT EXISTS airports (
   airport_id Bigint PRIMARY KEY,
   name varchar(255),
   city varchar(255),
   country varchar(255),
   iata varchar(15),
   icao varchar(15),
   latitude double precision,
   longitude double precision,
   altitude INTEGER,
   timezone double precision,
   dst double precision,
   tz_database_time_zone varchar(255)
);

CREATE TABLE IF NOT EXISTS routes (
   id SERIAL,
   airline INTEGER,
   airline_id INTEGER,
   source_airport VARCHAR(255),
   source_airport_id INTEGER,
   destination_airport VARCHAR(255),
   destination_airport_id INTEGER,
   stops INTEGER,
   equipment VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS weather (
   id SERIAL PRIMARY KEY,
   airport_iata VARCHAR(255),
   airport_city VARCHAR(255),
   latitude double precision,
   longitude double precision,
   temperature double precision,
   wind_speed double precision,
   precipitation double precision,
   timezone VARCHAR(255),
   weather_code INTEGER
);