SELECT
   a.country,
   COUNT(*) AS observations,
   ROUND(AVG(w.temperature)::numeric, 1) AS avg_temp,
   ROUND(AVG(w.wind_speed)::numeric, 1) AS avg_wind,
   MAX(w.observed_at) AS last_update
FROM {{ ref('stg_weather') }} AS w
JOIN {{ ref('stg_airports') }} AS a
  ON w.airport_iata = a.iata
GROUP BY a.country
ORDER BY a.country
   