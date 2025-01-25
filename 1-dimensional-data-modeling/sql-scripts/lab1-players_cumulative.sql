SELECT * 
FROM player_seasons;

-- Creating a struct data type for data that are important and changes frequently
CREATE TYPE season_stats AS (
	season INTEGER,
	gp REAL,
	pts REAL,
	reb REAL,
	ast REAL
);

-- Creating a ENUM data type for representing the scores
CREATE TYPE scoring_class AS ENUM('star', 'good', 'average', 'bad');

-- Creating a table that stores cumulative data from the updated table
CREATE TABLE players (
	player_name TEXT,
	height TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season INTEGER,
	current_season INTEGER,
	is_active BOOLEAN,
	PRIMARY KEY(player_name, current_season)
);

SELECT * FROM players;

-- Inserting the cumulative data in the cumulative dimension table
INSERT INTO players
WITH yesterday AS (
	SELECT * 
	FROM players
	WHERE current_season = 2000
),
	today AS (
		SELECT *
		FROM player_seasons
		WHERE season = 2001
	)
SELECT
	-- Removing null values when initializing the table and even later on.
	COALESCE(t.player_name, y.player_name) AS player_name,
	COALESCE(t.height, y.height) AS height,
	COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
	COALESCE(t.draft_year, y.draft_year) AS draft_year,
	COALESCE(t.draft_round, y.draft_round) AS draft_round,
	COALESCE(t.draft_number, y.draft_number) AS draft_number,
	-- Filling the season stats data, when player is new and does not have past data
	CASE 
		WHEN y.season_stats IS NULL
			THEN ARRAY[ROW(
				t.season,
				t.gp,
				t.pts,
				t.reb,
				t.ast
				)::season_stats]
	-- Concatenating both the yesterday and todays data when player has reappeared.
		WHEN t.season IS NOT NULL 
			THEN y.season_stats || ARRAY[ROW(
				t.season,
				t.gp,
				t.pts,
				t.reb,
				t.ast
				)::season_stats]
		ELSE y.season_stats
	END AS season_stats,
	-- Adding scoring class for different points category
	CASE 
		WHEN t.season IS NOT NULL 
			THEN CASE
				WHEN t.pts > 20 THEN 'star'
				WHEN t.pts > 15 THEN 'good'
				WHEN t.pts > 10 THEN 'average'
				ELSE 'bad'
			END::scoring_class
		ELSE y.scoring_class
	END AS scoring_class,
	-- Incrementing for every year from last appeared
	CASE
		WHEN t.season IS NOT NULL THEN 0
		ELSE y.year_since_last_season + 1
	END AS year_since_last_season,
	COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name;

-- Inserting data in the players table with Additional functionality
-- Note- Similar to the above query, check the above easier understanding.
INSERT INTO players
WITH 
	years AS (
		SELECT *
		FROM GENERATE_SERIES(1996, 2022) AS season
		),
	players AS (
		SELECT 
			player_name,
			MIN(season) AS first_season
		FROM player_seasons
		GROUP BY player_name
	),
	players_and_seasons AS (
		SELECT *
		FROM players p
		JOIN years y
			ON p.first_season <= y.season
	),
	windowed AS (
		SELECT
			pas.player_name,
			pas.season,
			ARRAY_REMOVE(
				ARRAY_AGG(
					CASE
						WHEN ps.season IS NOT NULL
							THEN ROW(
								ps.season,
								ps.gp,
								ps.pts,
								ps.reb,
								ps.ast
							)::season_stats
					END
				)
				OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
				NULL
			) AS seasons
		FROM players_and_seasons pas
		LEFT JOIN player_seasons ps
			ON pas.player_name = ps.player_name
				AND pas.season = ps.season
		ORDER BY pas.player_name, pas.season
	),
	static AS (
		SELECT
			player_name,
			MAX(height) AS height,
        	MAX(college) AS college,
        	MAX(country) AS country,
        	MAX(draft_year) AS draft_year,
        	MAX(draft_round) AS draft_round,
        	MAX(draft_number) AS draft_number
    	FROM player_seasons
    	GROUP BY player_name
	)
SELECT
	w.player_name,
	s.height,
	s.college,
	s.country,
	s.draft_year,
	s.draft_round,
	s.draft_number,
	seasons AS season_stats,
	CASE
		WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
		WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
		WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
		ELSE 'bad'
	END::scoring_class AS scoring_class,
	w.season - (seasons[CARDINALITY(seasons)]::season_stats).season AS years_since_last_season,
	w.season,
	(seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
	ON w.player_name = s.player_name;

-- Unnesting the data back into separate columns
SELECT 
	player_name,
	(UNNEST(season_stats)::season_stats).*,
	scoring_class,
	year_since_last_season,
	current_season
FROM players
WHERE current_season = 2001;

SELECT
	player_name,
	(season_stats[1]::season_stats).pts AS first_season_pts,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts AS latest_season_pts,
	CASE 
		WHEN(season_stats[1]::season_stats).pts = 0
			THEN (season_stats[CARDINALITY(season_stats)]::season_stats).pts
		ELSE ((season_stats[CARDINALITY(season_stats)]::season_stats).pts - 
			(season_stats[1]::season_stats).pts) /
			(season_stats[1]::season_stats).pts
	END AS performance_score
FROM players
WHERE current_season = 2001
ORDER BY performance_score DESC;
