
-- Top 5 stadiums by capacity --

SELECT * 
FROM world_stadiums.stadiums
ORDER BY `Seating capacity` DESC
LIMIT 5;

-- Count of stadiums in each country --

SELECT Country, COUNT(id) as nbr_of_stads
FROM world_stadiums.stadiums
GROUP BY Country
ORDER BY nbr_of_stads DESC;

-- Count of stadiums in each region --

SELECT Region, COUNT(id) as nbr_of_stads
FROM world_stadiums.stadiums
GROUP BY Region
ORDER BY nbr_of_stads DESC;

-- Average capacity by region --

SELECT Region, AVG(`Seating capacity`) as avg_capacity
FROM world_stadiums.stadiums
GROUP BY Region
ORDER BY avg_capacity DESC;

-- Top 3 stadiums by capacity within each region --

WITH stads_ranked AS(
SELECT *,  
RANK() OVER( PARTITION BY Region ORDER BY `Seating capacity` DESC) AS stad_rank
FROM world_stadiums.stadiums
)

SELECT Region, Stadium, Country, `Seating capacity`, stad_rank
FROM stads_ranked
WHERE stad_rank <= 3

-- Stadiums with capacity above regional average --

WITH regional_avg AS (
  SELECT Region, AVG(`Seating capacity`) as avg_capacity
  FROM world_stadiums.stadiums
  GROUP BY Region
)

SELECT t1.Region, t1.Stadium, t1.Country, `Seating capacity`, avg_capacity
FROM world_stadiums.stadiums AS t1 INNER JOIN regional_avg AS t2
ON t1.Region = t2.Region
WHERE t1.`Seating capacity` > avg_capacity

-- Stadiums with the closest capacity to regional average --

WITH regional_avg AS (
  SELECT Region, AVG(`Seating capacity`) as avg_capacity
  FROM world_stadiums.stadiums
  GROUP BY Region
)

SELECT Stadium, Region, Country, `Home teams`, `Seating capacity`, avg_capacity, distance, 
FROM (
SELECT t1.Stadium, t1.Region, t1.Country, t1.`Home teams`, t1.`Seating capacity`, t2.avg_capacity, 
ABS(t1.`Seating capacity` - t2.avg_capacity) AS distance, 
RANK() OVER( PARTITION BY t1.Region ORDER BY ABS(t1.`Seating capacity` - t2.avg_capacity)) AS rankk
FROM world_stadiums.stadiums AS t1 INNER JOIN regional_avg AS t2
ON t1.Region = t2.Region
)
WHERE rankk = 1
