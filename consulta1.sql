-- Média do AQI por cidade
SELECT city, AVG(aqi) as average_aqi 
FROM air_quality 
GROUP BY city;