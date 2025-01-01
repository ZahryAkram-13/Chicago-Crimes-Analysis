-- Crée une table temporaire avec les colonnes nécessaires après suppression
CREATE TABLE filtered_data AS
SELECT 
    `Date`, 
    `Primary Type`, 
    `Description`, 
    `Location Description`, 
    `Arrest`, 
    `Domestic`, 
    `Beat`, 
    `District`, 
    `Community Area`, 
    `Year`, 
    `Latitude`, 
    `Longitude`, 
    `Location`
FROM raw_data
WHERE `Updated On` IS NOT NULL;

-- Sépare la colonne 'Date' en trois colonnes distinctes : Date, Time, AM_PM
CREATE TABLE transformed_date_time AS
SELECT
    SPLIT(`Date`, ' ')[0] AS `Date`,
    SPLIT(`Date`, ' ')[1] AS `Time`,
    SPLIT(`Date`, ' ')[2] AS `AM_PM`
FROM filtered_data;

-- Remplit les valeurs manquantes pour 'Location Description' avec la valeur la plus fréquente
WITH freq_location AS (
    SELECT `Location Description`, COUNT(*) AS count
    FROM filtered_data
    GROUP BY `Location Description`
    ORDER BY count DESC
    LIMIT 1
)
CREATE TABLE filled_location_description AS
SELECT 
    *,
    COALESCE(`Location Description`, (SELECT `Location Description` FROM freq_location)) AS `Location Description`
FROM filtered_data;

-- Ajoute une colonne 'hour_exact' à partir de 'Time' et 'AM_PM'
CREATE TABLE hourly_data AS
SELECT 
    *,
    CASE
        WHEN `AM_PM` = 'AM' AND SPLIT(`Time`, ':')[0] = '12' THEN 0
        WHEN `AM_PM` = 'AM' THEN CAST(SPLIT(`Time`, ':')[0] AS INT)
        WHEN `AM_PM` = 'PM' AND SPLIT(`Time`, ':')[0] = '12' THEN 12
        ELSE CAST(SPLIT(`Time`, ':')[0] AS INT) + 12
    END AS `hour_exact`
FROM transformed_date_time;

-- Calcule le nombre de crimes par heure
CREATE TABLE crime_by_hour AS
SELECT 
    `hour_exact`, 
    COUNT(*) AS crime_count
FROM hourly_data
GROUP BY `hour_exact`
ORDER BY crime_count DESC;

-- Identifie les hotspots de crimes Kidnapping et Robbery
CREATE TABLE kidnapping_robbery_hotspots AS
SELECT 
    `Primary Type`, 
    `Location`, 
    `Location Description`, 
    COUNT(*) AS count
FROM filtered_data
WHERE `Primary Type` IN ('KIDNAPPING', 'ROBBERY')
GROUP BY `Primary Type`, `Location`, `Location Description`
ORDER BY count DESC
LIMIT 20;

-- Analyse le type de crime par district
CREATE TABLE crime_type_by_district AS
SELECT 
    `District`, 
    `Primary Type`, 
    COUNT(*) AS crime_count
FROM filtered_data
GROUP BY `District`, `Primary Type`
ORDER BY `District`, crime_count DESC;
