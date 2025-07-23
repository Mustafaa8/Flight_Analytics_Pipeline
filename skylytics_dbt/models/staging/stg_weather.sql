SELECT 
   airport_iata,
   temperature,
   wind_speed,
   precipitation,
   weather_code,
   CASE 
      WHEN weather_code IN (0) THEN 'Clear sky'
      WHEN weather_code IN (1, 2, 3) THEN 'partly cloudy'
      WHEN weather_code IN (45, 48) THEN 'Fog and depositing rime fog'
      WHEN weather_code IN (51, 53, 55) THEN 'Drizzle'
      WHEN weather_code IN (56, 57) THEN 'Freezing Drizzle'
      WHEN weather_code IN (61, 63, 65) THEN 'Rain'
      WHEN weather_code IN (66, 67) THEN 'Freezing Rain'
      WHEN weather_code IN (71, 73, 75) THEN 'Snow fall'
      WHEN weather_code IN (77) THEN 'Snow grains'
      WHEN weather_code IN (80, 81, 82) THEN 'Rain showers'
      WHEN weather_code IN (85, 86) THEN 'Snow showers'
      WHEN weather_code IN (95) THEN 'Thunderstorm'
      WHEN weather_code IN (96, 99) THEN 'Thunderstorm with hail'
      ELSE 'Unknown'  
      END AS weather_condition,
   timezone,
   NOW() AS observed_at
FROM
   {{source('flight_data','weather')}}