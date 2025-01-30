CREATE TABLE user_cumulated(
	user_id  TEXT,
	-- The current date of the user
	date Date,
	-- The list of dates in the past where the user was active
	dates_active DATE[],
	PRIMARY KEY (user_id, date)
);

-- Accumulating daily data
INSERT INTO user_cumulated
WITH
	yesterday AS (
		SELECT *
		FROM user_cumulated
		WHERE date = DATE('2023-01-30')
	),
	today AS (
		SELECT
			user_id::TEXT,
			DATE(CAST(event_time AS TIMESTAMP)) AS date_active
		FROM events
		WHERE
			DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
			AND user_id IS NOT NULL
		GROUP BY user_id, date_active
	)
SELECT
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS DATE,
	CASE
		WHEN y.dates_active IS NULL
			THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL
			THEN y.dates_active
		ELSE ARRAY[t.date_active] || y.dates_active
	END AS dates_active
FROM today t
FULL OUTER JOIN yesterday y
	ON t.user_id = y.user_id;

WITH
	-- Getting last date (today date) data 
	users AS (
		SELECT *
		FROM user_cumulated
		WHERE date = DATE('2023-01-31')
	),
	-- Generating a series with to match with the active days 
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
			WHEN dates_active @> ARRAY[DATE(series_date)]
				-- Converts the difference from today to active date into 32 bit decimal
				-- 32 Bit decimal range from 0-31
				THEN POW(2, 31 - (date - DATE(series_date)))::BIGINT
			ELSE 0
		END AS placeholder_int_value,
		*
	FROM users
	CROSS JOIN series
	)
SELECT
	user_id,
	SUM(placeholder_int_value)::BIGINT::BIT(32),
	BIT_COUNT(SUM(placeholder_int_value)::BIGINT::BIT(32)) > 0
		AS dim_is_monthly_active,
	BIT_COUNT('11111110000000000000000000000000'::BIT(32) &
		SUM(placeholder_int_value)::BIGINT::BIT(32)) > 0 
		AS dim_is_weekly_active,
	BIT_COUNT('10000000000000000000000000000000'::BIT(32) &
		SUM(placeholder_int_value)::BIGINT::BIT(32)) > 0 
		AS dim_is_daily_active
FROM place_holder_ints
GROUP BY user_id
	
	