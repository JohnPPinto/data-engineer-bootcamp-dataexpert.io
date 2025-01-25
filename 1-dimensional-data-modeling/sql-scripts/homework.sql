-- https://github.com/DataExpert-io/data-engineer-handbook/blob/main/bootcamp/materials/1-dimensional-data-modeling/homework/homework.md

-- Assignment Tasks
-- 1. DDL for actors table: Create a DDL for an actors table with the following fields:

-- 	films: An array of struct with the following fields:

-- 	film: The name of the film.
-- 	votes: The number of votes the film received.
-- 	rating: The rating of the film.
-- 	filmid: A unique identifier for each film.

-- 	quality_class: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. 

-- 	It's categorized as follows:
-- 	star: Average rating > 8.
-- 	good: Average rating > 7 and ≤ 8.
-- 	average: Average rating > 6 and ≤ 7.
-- 	bad: Average rating ≤ 6.

-- 	is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).

CREATE TYPE films AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid	TEXT
);

CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad');

CREATE TABLE actors (
	actorid TEXT,
	actor TEXT,
	films films[],
	quality_class quality_class,
	current_year INTEGER,
	is_active BOOLEAN,
	PRIMARY KEY (actorid, current_year)
);

-- 2. Cumulative table generation query: Write a query that populates the actors table one year at a time.

INSERT INTO actors
WITH 
	previous_year AS (
		SELECT *
		FROM actors
		WHERE current_year = 1969
	),
	this_year AS (
		SELECT 
			actorid,
			actor,
			year,
			ARRAY_AGG(ARRAY[ROW(
				film,
				votes,
				rating,
				filmid
			)::films]) AS films,
			AVG(rating) AS avg_rating
		FROM actor_films
		WHERE year = 1970
		GROUP BY actorid, actor, year
	)
SELECT
	COALESCE(ty.actorid, py.actorid) AS actorid,
	COALESCE(ty.actor, py.actor) AS actor,
	CASE
		WHEN py.current_year IS NULL
			THEN ty.films
		WHEN ty.year IS NOT NULL
			THEN py.films || ty.films
		ELSE py.films
	END AS films,
	CASE
		WHEN ty.avg_rating IS NULL
			THEN py.quality_class
		ELSE
			CASE
				WHEN ty.avg_rating > 8 THEN 'star'
				WHEN ty.avg_rating > 7 THEN 'good'
				WHEN ty.avg_rating > 6 THEN 'average'
				ELSE 'bad'
			END::quality_class
	END AS quality_class,
	COALESCE(ty.year, py.current_year + 1) AS current_year,
	CASE
		WHEN ty.year IS NULL
			THEN FALSE
		ELSE TRUE
	END AS is_active
FROM this_year ty
FULL OUTER JOIN previous_year py
	ON ty.actorid = py.actorid;

-- 3. DDL for actors_history_scd table: Create a DDL for an actors_history_scd table with the following features:

-- Implements type 2 dimension modeling (i.e., includes start_date and end_date fields).
-- Tracks quality_class and is_active status for each actor in the actors table.

CREATE TABLE actors_history_scd (
	actorid TEXT,
	actor TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	current_year INTEGER,
	start_date INTEGER,
	end_date INTEGER,
	PRIMARY KEY(actorid, start_date)
);

-- 4. Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
INSERT INTO actors_history_scd
WITH
	with_previous AS (
		SELECT
			actorid,
			actor,
			quality_class,
			is_active,
			current_year,
			LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
			LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
		FROM actors
		WHERE current_year <= 2020
	),
	with_indicators AS (
		SELECT
			*,
			CASE
				WHEN quality_class <> previous_quality_class THEN 1
				WHEN is_active <> previous_is_active THEN 1
				ELSE 0
			END AS change_indicator
		FROM with_previous
	),
	with_streaks AS (
		SELECT
			*,
			SUM(change_indicator)
				OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
		FROM with_indicators
	)
SELECT
	actorid,
	actor,
	quality_class,
	is_active,
	2020 AS current_year,
	MIN(current_year) AS start_date,
	MAX(current_year) AS end_date
FROM with_streaks
GROUP BY actorid, actor, streak_identifier, quality_class, is_active
ORDER BY actorid, actor, streak_identifier;

-- 5. Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.

CREATE TYPE actors_scd_type AS (
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER
);

WITH
	last_year_scd AS (
		SELECT *
		FROM actors_history_scd
		WHERE current_year = 2020
			AND end_date = 2020
	),
	historical_scd AS (
		SELECT
			actorid,
			actor,
			quality_class,
			is_active,
			start_date,
			end_date
		FROM actors_history_scd
		WHERE current_year = 2020
			AND end_date < 2020
	),
	this_year_data AS (
		SELECT * 
		FROM actors 
		WHERE current_year = 2021
	),
	unchanged_records AS (
		SELECT
			ty.actorid,
			ty.actor,
			ty.quality_class,
			ty.is_active,
			ly.start_date,
			ty.current_year AS end_date
		FROM this_year_data ty
		JOIN last_year_scd ly
			ON ty.actorid = ly.actorid
		WHERE ty.quality_class = ly.quality_class
			AND ty.is_active = ly.is_active
	),
	changed_records AS (
		SELECT
			ty.actorid,
			ty.actor,
			UNNEST(
				ARRAY[
					ROW(
						ly.quality_class,
						ly.is_active,
						ly.start_date,
						ly.end_date
					)::actors_scd_type,
					ROW(
						ty.quality_class,
						ty.is_active,
						ty.current_year,
						ty.current_year
					)::actors_scd_type
				]
			) AS records
		FROM this_year_data ty
		LEFT JOIN last_year_scd ly
			ON ty.actorid = ly.actorid
		WHERE ty.quality_class <> ly.quality_class
			OR ty.is_active <> ly.is_active 
	),
	unnested_changed_records AS (
		SELECT
			actorid,
			actor,
			(records::actors_scd_type).quality_class,
			(records::actors_scd_type).is_active,
			(records::actors_scd_type).start_date,
			(records::actors_scd_type).end_date
		FROM changed_records
	),
	new_records AS (
		SELECT
			ty.actorid,
			ty.actor,
			ty.quality_class,
			ty.is_active,
			ty.current_year AS start_date,
			ty.current_year AS end_date
		FROM this_year_data ty
		LEFT JOIN last_year_scd ly
			ON ty.actorid = ly.actorid
		WHERE ly.actorid IS NULL
	)
SELECT 
	*,
	2021 AS current_year
FROM (
	SELECT *
	FROM historical_scd
	UNION ALL
	SELECT *
	FROM unchanged_records
	UNION ALL
	SELECT *
	FROM unnested_changed_records
	UNION ALL
	SELECT *
	FROM new_records
) AS scd_table
ORDER BY actor, start_date;