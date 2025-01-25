CREATE TABLE players_scd (
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, start_season)
);

CREATE TYPE scd_type AS (
	scoring_class scoring_class,
	is_active boolean,
	start_season INTEGER,
	end_season INTEGER
)

INSERT INTO players_scd
WITH
	-- Previous data of scoring class and is_active are lagged for comparison
	with_previous AS (
		SELECT 
			player_name, 
			current_season,
			scoring_class, 
			is_active,
			LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
			LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
		FROM players
		WHERE current_season <= 2021
	),
	-- Comparing the lagged and actual values and checking for which data changes
	with_indicators AS (
		SELECT 
			*,
			CASE
				WHEN scoring_class <> previous_scoring_class THEN 1
				WHEN is_active <> previous_is_active THEN 1
				ELSE 0
			END AS change_indicator
		FROM with_previous
	),
	-- Incrementing the changed values
	with_streaks AS (
		SELECT
			*,
			SUM(change_indicator) 
				OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
		FROM with_indicators
	)
SELECT 
	player_name,
	scoring_class,
	is_active,
	MIN(current_season) AS start_season,
	MAX(current_season) AS end_season,
	2021 AS current_season
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, streak_identifier;

SELECT * FROM players_scd;

-- Updating the scd table with new data
WITH 
	-- Getting the last temporal data
	last_season_scd AS (
		SELECT * FROM players_scd
		WHERE current_season = 2021
			AND end_season = 2021
	),
	-- Getting the temporal data other than the last one
	historical_scd AS (
		SELECT
			player_name,
			scoring_class,
			is_active,
			start_season,
			end_season
		FROM players_scd
		WHERE current_season = 2021
			AND end_season < 2021
	),
	-- Getting the updated data
	this_season_data AS (
		SELECT * FROM players
		WHERE current_season = 2022
	),
	-- Getting the records that have not been changed from the new data
	unchanged_records AS (
		SELECT
			ts.player_name,
			ts.scoring_class,
			ts.is_active,
			ls.start_season,
			ts.current_season as end_season
		FROM this_season_data ts
		JOIN last_season_scd ls
			ON ts.player_name = ls.player_name
		WHERE ts.scoring_class = ls.scoring_class
			AND ts.is_active = ls.is_active
	),
	-- Getting the records that have been changed from the new data
	changed_records AS (
		SELECT
			ts.player_name,
			UNNEST(
				ARRAY[
					ROW(
						ls.scoring_class,
						ls.is_active,
						ls.start_season,
						ls.end_season
					)::scd_type,
					ROW(
						ts.scoring_class,
						ts.is_active,
						ts.current_season,
						ts.current_season
					)::scd_type
				]
			) AS records
		FROM this_season_data ts
		LEFT JOIN last_season_scd ls
			ON ts.player_name = ls.player_name
		WHERE ts.scoring_class <> ls.scoring_class
			OR ts.is_active <> ls.is_active
	),
	unnested_changed_records AS (
		SELECT
			player_name,
			(records::scd_type).scoring_class,
			(records::scd_type).is_active,
			(records::scd_type).start_season,
			(records::scd_type).end_season
		FROM changed_records
	),
	-- Getting completely new records from the new data
	new_records AS (
		SELECT
			ts.player_name,
			ts.scoring_class,
			ts.is_active,
			ts.current_season AS start_season,
			ts.current_season AS end_season
		FROM this_season_data ts
		LEFT JOIN last_season_scd ls
			ON ts.player_name = ls.player_name
		WHERE ls.player_name IS NULL
	)
SELECT 
	*, 
	2022 AS current_season
FROM (
	SELECT *
	FROM historical_scd
	UNION ALL
	SELECT * 
	FROM unchanged_records
	UNION  ALl
	SELECT * 
	FROM unnested_changed_records
	UNION ALL
	SELECT * 
	FROM new_records
) AS scd_table;
