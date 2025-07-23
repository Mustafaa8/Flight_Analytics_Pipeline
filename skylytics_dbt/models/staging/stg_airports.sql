SELECT 
   airport_id,
   iata,
   city,
   country,
   latitude,
   longitude
FROM {{ source('flight_data','airports') }}
WHERE iata IS NOT NULL