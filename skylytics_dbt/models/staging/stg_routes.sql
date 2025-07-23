SELECT 
   source_airport,
   destination_airport,
   stops
FROM 
   {{ source('flight_data','routes') }}