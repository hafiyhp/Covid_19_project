-- Database: Covid_Project_2020_2023

-- DROP DATABASE IF EXISTS "Covid_Project_2020_2023";

CREATE DATABASE "Covid_Project_2020_2023"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;


CREATE TABLE covid_deaths (
	        iso_code VARCHAR(255),
			continent VARCHAR(255),
			location VARCHAR(100),
			date DATE,
			total_cases INT,
			new_cases INT,
			total_deaths INT,
			population BIGINT);

CREATE TABLE covid_vax(
	        iso_code VARCHAR(255),
	        continent VARCHAR(100),
			location VARCHAR(100),
			date DATE,
			total_vaccinations NUMERIC,
			people_vaccinated NUMERIC,
			people_fully_vaccinated NUMERIC);
			

COPY covid_deaths FROM 'C:/Program Files/PostgreSQL/16/data/Data_copy/CovidDeaths_2020_2023_SQL.csv'
DELIMITER ',' CSV HEADER;

COPY covid_vax FROM 'C:/Program Files/PostgreSQL/16/data/Data_copy/Covid_Vacs_2020_2023_SQL.csv'
DELIMITER ',' CSV HEADER;

SELECT * from covid_vax;

SELECT * from covid_deaths;

/* Total Cases vs Total Deaths for each day in the UK */
SELECT
	location,
	date,
	total_cases,
	total_deaths,
	ROUND((total_deaths / total_cases) * 100,2) as death_percentage
	FROM covid_deaths
	WHERE location LIKE '%United Kingdom%'
	ORDER BY
	location,
	date;

COPY (
SELECT
	location,
	date,
	total_cases,
	total_deaths,
	ROUND((total_deaths / total_cases) * 100,2) as death_percentage
	FROM covid_deaths
	WHERE location LIKE '%United Kingdom%'
	ORDER BY
	location,
	date
) TO 'C:/Program Files/PostgreSQL/16/data/Data_copy/CovidDeaths_totalcases_vs_death.csv' WITH CSV HEADER;


/* Total Cases vs Population for each day in the UK */

SELECT
	location,
	date,
	total_cases,
	population,
	ROUND((total_cases / population) * 100,2) as case_percentage
FROM covid_deaths
WHERE location like '%United Kingdom%'
ORDER BY location, date;

COPY (
SELECT
	location,
	date,
	total_cases,
	population,
	ROUND((total_cases / population) * 100,2) as case_percentage
FROM covid_deaths
WHERE location like '%United Kingdom%'
ORDER BY location, date)
TO 'C:/Program Files/PostgreSQL/16/data/Data_copy/CovidDeaths_totalcases_vs_population.csv' WITH CSV HEADER;



/* 3) Total Deaths for each day in the UK in each year*/

/* This query works */

SELECT
	DATE_PART ('year', date) AS year,
	date,
	SUM(total_deaths) AS total_deaths
FROM covid_deaths
WHERE location = 'United Kingdom'
GROUP BY year, date
ORDER BY year, date;

COPY (SELECT
	DATE_PART ('year', date) AS year,
	date,
	SUM(total_deaths) AS total_deaths
FROM covid_deaths
WHERE location = 'United Kingdom'
GROUP BY year, date
ORDER BY year, date
) TO 'C:/Program Files/PostgreSQL/16/data/Data_copy/CovidDeaths_timeseries_plot.csv' WITH CSV HEADER;

/* 3) Total Deaths for each day in the UK in each year
 Use SUM and DATE_PART
 	
The DATE_PART function is used in the SQL query to extract a specific component (such as year, month, day, etc.) 
from a date or timestamp value. It returns the specified component as a numeric value.
 
 By date, visualize the graph in a time series analysis
 By year, visualise the graph by whisker and bar plot
*/

/* This query doesn't works */
 SELECT 
	year,
	location AS "United Kingdom",
	SUM(total_deaths) AS total_deaths
FROM (
SELECT 
	DATE_PART('year', date) AS year,
	date,
	SUM(total_deaths) AS total_deaths
FROM covid_deaths
WHERE location = 'United Kingdom'
GROUP BY
	year, date) AS subquery
WHERE year IN (2020, 2021, 2022, 2023)
GROUP BY year, "United Kingdom"
ORDER BY year;

/* This query works */
SELECT 
	year,
	SUM(total_deaths) as "total deaths in United Kingdom"
FROM (
SELECT 
	DATE_PART('year', date) AS year,
	date,
	SUM(total_deaths) AS total_deaths
FROM covid_deaths
WHERE location = 'United Kingdom'
GROUP BY year, date) 
AS subquery
WHERE year IN (2020, 2021, 2022, 2023)
GROUP BY year
ORDER BY year;

COPY (SELECT 
	year,
	SUM(total_deaths) as "total deaths in United Kingdom"
FROM (
SELECT 
	DATE_PART('year', date) AS year,
	date,
	SUM(total_deaths) AS total_deaths
FROM covid_deaths
WHERE location = 'United Kingdom'
GROUP BY year, date) 
AS subquery
WHERE year IN (2020, 2021, 2022, 2023)
GROUP BY year
ORDER BY year)TO 'C:/Program Files/PostgreSQL/16/data/Data_copy/CovidDeaths_barplot.csv' WITH CSV HEADER;


/*

Exploring the Covid vaccinations tables

-> use INNER JOIN table
----> then find total population vs total vaccinations -> finally use CTE

*/

SELECT * FROM covid_vax;

SELECT * FROM covid_deaths AS dea
INNER JOIN covid_vax AS vac ON
dea.location = vac.location AND
dea.date = vac.date AND
dea.continent = vac.continent;

COPY (
SELECT * FROM covid_deaths AS dea
INNER JOIN covid_vax AS vac ON
dea.location = vac.location AND
dea.date = vac.date AND
dea.continent = vac.continent)
TO 'C:/Program Files/PostgreSQL/16/data/Data_copy/CovidDea_Vac.csv' WITH CSV HEADER;

SELECT
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	SUM(CAST(vac.total_vaccinations as numeric))
	OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date)
	AS roll_vaccinations
FROM covid_deaths AS dea
INNER JOIN covid_vax AS vac ON
dea.location = vac.location AND
dea.date = vac.date
WHERE
dea.continent IS NOT NULL
AND dea.location = 'United Kingdom'
ORDER BY
dea.location,
dea.date;

/* Using CTE */
 WITH popVSvac(continent, location, date, population,
			 roll_vaccinations) AS (
			 SELECT
			 dea.continent,
			 dea.location,
			 dea.date,
			 dea.population,
			 SUM(CAST(vac.total_vaccinations AS numeric)) OVER 
				 (PARTITION BY dea.location ORDER BY dea.location, dea.date) as roll_vaccinations
			FROM covid_deaths as dea
			INNER JOIN covid_vax as vac ON
				 dea.location = vac.location AND
				 dea.date = vac.date
				WHERE
				 dea.continent IS NOT NULL
				 AND dea.location = 'United Kingdom')
			SELECT * FROM popVSvac;
			
WITH popVSvac(continent, location, date, population,
			 roll_vaccinations) AS (
			 SELECT
			 dea.continent,
			 dea.location,
			 dea.date,
			 dea.population,
			 SUM(CAST(vac.total_vaccinations AS numeric)) OVER 
				 (PARTITION BY dea.location ORDER BY dea.location, dea.date) as roll_vaccinations
			FROM covid_deaths as dea
			INNER JOIN covid_vax as vac ON
				 dea.location = vac.location AND
				 dea.date = vac.date
				WHERE
				 dea.continent IS NOT NULL
				 AND dea.location = 'United Kingdom')
			SELECT *,
			ROUND((roll_vaccinations / population) * 100,2) as vac_percent
			 FROM popVSvac;
			 

SELECT * from covid_vax;

EXPLAIN 

WITH popVSvac(continent, location, date, population,
			  roll_fully_vaccinations,
			 roll_vaccinations) AS (
			 SELECT
			 dea.continent,
			 dea.location,
			 dea.date,
			 dea.population,
			 SUM(CAST(vac.people_fully_vaccinated AS numeric)) OVER 
				 (PARTITION BY dea.location ORDER BY dea.location, dea.date) as roll_fully_vaccinations,
			SUM(CAST(vac.people_vaccinated AS numeric)) OVER
				 	(PARTITION BY dea.location ORDER BY dea.location, dea.date) as roll_vaccinations
			FROM covid_deaths as dea
			INNER JOIN covid_vax as vac ON
				 dea.location = vac.location AND
				 dea.date = vac.date
				WHERE
				 dea.continent IS NOT NULL
				 AND dea.location = 'United Kingdom')
			SELECT *,
			roll_fully_vaccinations as fully_vac,
            roll_vaccinations as part_vac
			 FROM popVSvac;
			 
-- Create View
 CREATE VIEW vaccination_central AS (
SELECT
 dea.continent, 
 dea.location, 
 dea.date, 
 dea.population, 
 SUM(CAST(vac.people_fully_vaccinated AS numeric)) OVER 
				 (PARTITION BY dea.location ORDER BY dea.location, dea.date) as roll_fully_vaccinations,
			SUM(CAST(vac.people_vaccinated AS numeric)) OVER
				 	(PARTITION BY dea.location ORDER BY dea.location, dea.date) as roll_vaccinations
			FROM covid_deaths as dea
			INNER JOIN covid_vax as vac ON
				 dea.location = vac.location AND
				 dea.date = vac.date
				WHERE
				 dea.continent IS NOT NULL
				 AND dea.location = 'United Kingdom');
 
SELECT * FROM vaccination_central;

COPY (SELECT
 dea.continent, 
 dea.location, 
 dea.date, 
 dea.population, 
 SUM(CAST(vac.people_fully_vaccinated AS numeric)) OVER 
				 (PARTITION BY dea.location ORDER BY dea.location, dea.date) as roll_fully_vaccinations,
			SUM(CAST(vac.people_vaccinated AS numeric)) OVER
				 	(PARTITION BY dea.location ORDER BY dea.location, dea.date) as roll_vaccinations
			FROM covid_deaths as dea
			INNER JOIN covid_vax as vac ON
				 dea.location = vac.location AND
				 dea.date = vac.date
				WHERE
				 dea.continent IS NOT NULL
				 AND dea.location = 'United Kingdom') 
			TO 'C:/Program Files/PostgreSQL/16/data/Data_copy/Vaccination_central.csv' WITH CSV HEADER;
 
/* Use EXPLAIN to find query execution time */ 