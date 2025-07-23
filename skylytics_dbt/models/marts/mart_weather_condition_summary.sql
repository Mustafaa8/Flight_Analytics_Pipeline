SELECT
    weather_condition,
    COUNT(*) AS occurrences,
    ROUND(AVG(temperature)::numeric, 1) AS avg_temp,
    ROUND(AVG(wind_speed)::numeric, 1) AS avg_wind
FROM {{ ref('stg_weather') }}
GROUP BY weather_condition
ORDER BY occurrences DESC