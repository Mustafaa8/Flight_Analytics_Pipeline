SELECT
  airport_iata,
  COUNT(*) AS observations,
  ROUND(AVG(temperature)::numeric, 1) AS avg_temp,
  ROUND(AVG(wind_speed)::numeric, 1) AS avg_wind,
  MAX(observed_at) AS last_update
FROM {{ ref('stg_weather') }}
GROUP BY airport_iata