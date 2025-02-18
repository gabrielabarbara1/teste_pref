-- Cidades com AQI acima de 100
SELECT city, aqi 
FROM air_quality 
WHERE aqi > 100;