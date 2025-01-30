-- The homework this week will be using the devices and events dataset
-- Construct the following eight queries:

-- 1. A query to deduplicate game_details from Day 1 so there's no duplicates

CREATE TABLE fct_game_details (
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
	dim_player_name TEXT,
	dim_start_position TEXT, 
	dim_is_playing_at_home BOOLEAN,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_minutes REAL,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_reb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnover INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER,
	PRIMARY KEY (
		dim_game_date,
		dim_team_id,
		dim_player_id
	)
);

INSERT INTO fct_game_details
WITH
	-- Removing duplication from the table
	deduped AS (
		SELECT
			g.game_date_est,
			g.season,
			g.home_team_id,
			gd.*,
			ROW_NUMBER() OVER(
				PARTITION BY 
					player_id, 
					gd.game_id, 
					team_id
			) AS row_num
		FROM 
			game_details gd
			JOIN games g
				ON gd.game_id = g.game_id
	)
-- Selecting specific attributes and getting new information
SELECT
	game_date_est AS dim_game_date,
	season AS dim_season,
	team_id AS dim_team_id,
	player_id AS dim_player_id,
	player_name AS dim_player_name,
	start_position AS dim_start_position,
	team_id = home_team_id 
		AS dim_is_playing_at_home,
	COALESCE(POSITION('DNP' in comment), 0) > 0
		AS dim_did_not_play,
	COALESCE(POSITION('DND' in comment), 0) > 0
		AS dim_did_not_dress,
	COALESCE(POSITION('NWT' in comment), 0) > 0
		AS dim_not_with_team,
	CAST(SPLIT_PART(min, ':', 1) AS REAL) 
		+ CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60  
		AS m_minutes,
	fgm AS m_fgm,
	fga AS m_fga,
	fg3m AS m_fg3m,
	fg3a AS m_fg3a,
	ftm AS m_ftm,
	fta AS m_fta,
	oreb AS m_oreb,
	dreb AS m_dreb,
	reb AS m_reb,
	ast AS m_ast,
	stl AS m_stl,
	blk AS m_blk,
	"TO" AS m_turnovers,
	pf AS m_pf,
	pts AS m_pts,
	plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1;

-- 2. A DDL for an user_devices_cumulated table that has:
-- 	a. a device_activity_datelist which tracks a users active days by browser_type
-- 	b. data type here should look similar to MAP<STRING, ARRAY[DATE]>
-- 		or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)

CREATE TABLE user_devices_cumulated (
	user_id NUMERIC,
	device_id NUMERIC,
	browser_type TEXT,
	date DATE,
	-- The list of device activity in the past where the user was active
	device_activity_datelist DATE[],
	PRIMARY KEY (user_id, device_id, browser_type, date)
);

-- 3. A cumulative query to generate device_activity_datelist from events

-- Accumulating daily data
INSERT INTO user_devices_cumulated
WITH
	-- Using windows function to find the duplicates within the table
	today AS (
		SELECT
			e.user_id,
			e.device_id,
			d.browser_type,
			DATE(e.event_time::TIMESTAMP) AS date,
			ROW_NUMBER() OVER(
				PARTITION BY 
					e.user_id, 
					e.device_id, 
					d.browser_type
				) AS row_num
		FROM events AS e
		JOIN devices AS d
			ON e.device_id = d.device_id
		WHERE DATE(e.event_time) = DATE('2023-01-31') 
			AND e.user_id IS NOT NULL 
			AND e.device_id IS NOT NULL
	),
	-- Selecting only the first row and the rest are duplicates
	dedupped_today AS (
		SELECT *
		FROM today
		WHERE row_num = 1
	),
	yesterday AS (
		SELECT *
		FROM user_devices_cumulated
		WHERE date = DATE('2023-01-30')
	)
SELECT
	COALESCE(dt.user_id, y.user_id) AS user_id,
	COALESCE(dt.device_id, y.device_id) AS device_id,
	COALESCE(dt.browser_type, y.browser_type) AS browser_type,
	COALESCE(dt.date, y.date + INTERVAL '1 day')::DATE AS date,
	-- Cumulating all the dates by concating the yesterday data with todays data
	CASE
		WHEN y.device_activity_datelist IS NULL
			THEN ARRAY[dt.date]
		WHEN dt.date IS NULL
			THEN y.device_activity_datelist
		ELSE ARRAY[dt.date] || y.device_activity_datelist
	END AS device_activity_datelist
FROM dedupped_today AS dt
FULL OUTER JOIN yesterday AS y
	ON dt.user_id = y.user_id
		AND dt.device_id = y.device_id
		AND dt.browser_type = y.browser_type;

-- 4. A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column

WITH
	user_devices AS (
		SELECT *
		FROM user_devices_cumulated
		WHERE date = DATE('2023-01-31')
	),
	-- Generating a series to match with the active days
	series AS (
		SELECT *
		FROM GENERATE_SERIES(
			DATE('2023-01-01'),
			DATE('2023-01-31'),
			INTERVAL '1 day'
		) AS series_date
	),
	place_holder_ints AS (
		SELECT
			CASE
				-- Checking whether the generated date is present in the active days array
				WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
					-- Converts the difference from today to active date into 32 bit decimal
					-- 32 Bit range from 0 to 31, so subracting the difference with 31
					THEN POW(2, 31 - (date - DATE(series_date)))::BIGINT
				ELSE 0
			END AS placeholder_int_values,
			*
		FROM user_devices
		CROSS JOIN series
	)
SELECT
	user_id,
	device_id,
	browser_type,
	device_activity_datelist,
	-- Summing all the 32 bit decimal and converting it into binary
	-- Wherever there is 1 in the binary that was an active date, 
	-- all the values are in descending order in days.
	-- e.g. 10010...1,1,0,1,0 -> 31,30,29,28,27...4,3,2,1,0 
	SUM(placeholder_int_values)::BIGINT::BIT(32) AS datelist_int
FROM place_holder_ints
GROUP BY user_id, device_id, browser_type, device_activity_datelist;

-- 5. A DDL for hosts_cumulated table
-- 	a. A host_activity_datelist which logs to see which dates each host is experiencing any activity

CREATE TABLE hosts_cumulated (
	host TEXT,
	date DATE,
	-- The list of device activity in the past where the user was active
	host_activity_datelist DATE[],
	PRIMARY KEY (host, date)
);

-- 6. The incremental query to generate host_activity_datelist

-- Accumulating daily data
INSERT INTO hosts_cumulated
WITH
	today AS (
		SELECT
			host,
			DATE(event_time::TIMESTAMP) AS date
		FROM events
		WHERE DATE(event_time) = DATE('2023-01-10')
		GROUP BY host, date
	),
	yesterday AS (
		SELECT *
		FROM hosts_cumulated
		WHERE date = DATE('2023-01-09')
	)
SELECT
	COALESCE(t.host, y.host) AS host,
	COALESCE(t.date, y.date + INTERVAL '1 day')::DATE AS date,
	-- Cumulating all the dates by concating the yesterday data with todays data
	CASE
		WHEN y.host_activity_datelist IS NULL
			THEN ARRAY[t.date]
		WHEN t.date IS NULL
			THEN y.host_activity_datelist
		ELSE ARRAY[t.date] || y.host_activity_datelist
	END AS host_activity_datelist
FROM today AS t
FULL OUTER JOIN yesterday AS y
	ON t.host = y.host;
	
-- 7. A monthly, reduced fact table DDL host_activity_reduced
-- 	a. month
-- 	b. host
-- 	c. hit_array - think COUNT(1)
-- 	d. unique_visitors array - think COUNT(DISTINCT user_id)

CREATE TABLE host_activity_reduced (
	host TEXT,
	month_start DATE,
	metric TEXT,
	-- Counting number of hits for the website
	hit_array BIGINT[],
	-- Counting number of unique visitors for the website
	unique_visitors BIGINT[],
	PRIMARY KEY (host, month_start, metric)
);

-- 8. An incremental query that loads host_activity_reduced
-- 	a. day-by-day

-- All the values are aggregated and stored in an array month-wise
-- Values within the array increment daily up to last day of the month
INSERT INTO host_activity_reduced
WITH
	daily_aggregate AS (
		SELECT
			host,
			DATE(event_time) AS date,
			COUNT(1) AS num_site_hits,
			COUNT(DISTINCT(user_id)) AS unique_visitors
		FROM events
		WHERE DATE(event_time) = DATE('2023-01-10')
		GROUP BY host, date
	),
	yesterday_array AS (
		SELECT *
		FROM host_activity_reduced
		WHERE month_start = DATE('2023-01-01')
	)
SELECT
	COALESCE(da.host, ya.host) AS host,
	COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
	'site_hits' AS metric,
	CASE
		-- Concatenating the hits value from yesterday array 
		-- and daily aggregate table
		WHEN ya.hit_array IS NOT NULL
			THEN ya.hit_array || ARRAY[COALESCE(da.num_site_hits, 0)]
		-- When data is not present and a new user appears then all 
		-- the previous values are generated with 0 and concatenated 
		-- with the daily aggregated table value
		WHEN ya.hit_array IS NULL
			THEN ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)])
				|| ARRAY[COALESCE(da.num_site_hits, 0)]
	END AS hit_array,
	CASE
		-- Concatenating the hits value from yesterday array 
		-- and daily aggregate table
		WHEN ya.unique_visitors IS NOT NULL
			THEN ya.unique_visitors || ARRAY[COALESCE(da.unique_visitors, 0)]
		WHEN ya.unique_visitors IS NULL
		-- When data is not present and a new user appears then all 
		-- the previous values are generated with 0 and concatenated 
		-- with the daily aggregated table value
			THEN ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)])
				|| ARRAY[COALESCE(da.unique_visitors, 0)]
	END AS unique_visitors
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya
	ON da.host = ya.host
-- Using the upsert method to update the table whenever 
-- there is changes to hit array and unique visitors
ON CONFLICT (host, month_start, metric)
DO
	UPDATE SET 
		hit_array = EXCLUDED.hit_array,
		unique_visitors = EXCLUDED.unique_visitors;