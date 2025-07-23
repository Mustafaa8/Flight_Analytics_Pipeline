SELECT
  source_airport,
  destination_airport,
  COUNT(*) AS route_count
FROM {{ ref('stg_routes') }}
GROUP BY source_airport, destination_airport
ORDER BY route_count DESC
LIMIT 50